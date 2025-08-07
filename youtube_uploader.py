import os
import csv
import logging
import time
from googleapiclient.http import MediaFileUpload
from auth import get_authenticated_service

UPLOAD_LOG_FILE = os.path.join("resources", "uploaded_videos.log")

def load_uploaded_ids(log_file):
    """Reads the log file and returns a set of already uploaded video IDs."""
    if not os.path.exists(log_file):
        return set()
    with open(log_file, 'r', encoding='utf-8') as f:
        return {line.strip() for line in f}

def log_uploaded_id(video_id, log_file):
    """Appends a successfully uploaded video ID to the log file."""
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f"{video_id}\n")

def extract_tiktok_tags(description):
    """Extracts hashtags from a video description string."""
    if not description:
        return []
    tags = [word.strip('#,.-_') for word in description.split() if word.startswith('#')]
    logging.debug(f"Extracted {len(tags)} tags from description: {tags}")
    return tags

def upload_to_youtube(youtube, video_path, title, description, tags):
    """Uploads a single video to YouTube."""
    try:
        logging.info(f"Uploading video: {video_path}")
        body = {
            "snippet": {"title": title, "description": description, "tags": tags, "categoryId": "22"},
            "status": {"privacyStatus": "public", "selfDeclaredMadeForKids": False}
        }
        media = MediaFileUpload(video_path, chunksize=-1, resumable=True)
        request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
        response = request.execute()
        logging.info(f"Uploaded successfully: Video ID {response['id']}")
        return True
    except FileNotFoundError:
        logging.error(f"Video file not found at {video_path}. Skipping upload.")
        return False
    except Exception as e:
        logging.error(f"Error uploading video '{title}': {e}", exc_info=True)
        return False

def process_and_upload_clips(download_dir, max_uploads):
    """Processes metadata and uploads new, unique clips to YouTube."""
    # Use the passed-in value for the upload limit
    MAX_UPLOADS_PER_DAY = max_uploads
    
    # Safely calculate delay, avoiding division by zero
    if MAX_UPLOADS_PER_DAY > 0:
        DELAY_SECONDS = (24 * 60 * 60) // MAX_UPLOADS_PER_DAY
    else:
        DELAY_SECONDS = 0
    
    logging.debug("Starting YouTube upload process.")
    try:
        youtube = get_authenticated_service()
        metadata_path = os.path.join(download_dir, 'metadata.csv')
        
        if not os.path.exists(metadata_path):
            logging.error(f"Metadata file not found: {metadata_path}")
            return
        
        uploaded_ids = load_uploaded_ids(UPLOAD_LOG_FILE)
        logging.info(f"Loaded {len(uploaded_ids)} previously uploaded video IDs.")

        with open(metadata_path, mode='r', encoding='utf-8') as csv_file:
            videos_to_upload = list(csv.DictReader(csv_file))
        
        upload_count = 0
        new_videos_found = 0
        for row in videos_to_upload:
            video_id = row.get('video_id')
            if not video_id:
                logging.warning(f"Skipping row with missing video_id: {row}")
                continue

            if video_id in uploaded_ids:
                logging.debug(f"Skipping already uploaded video ID: {video_id}")
                continue

            new_videos_found += 1
            if upload_count >= MAX_UPLOADS_PER_DAY:
                logging.info(f"Daily upload limit of {MAX_UPLOADS_PER_DAY} reached. More new videos are available for the next run.")
                break

            username = row['author_username']
            video_description = row.get("video_description", "")
            video_path = os.path.join(download_dir, f"@{username}_video_{video_id}.mp4")
            
            if os.path.exists(video_path):
                title = video_description.split('#')[0].strip()[:90] or f"Check out this clip from {username}"
                tiktok_tags = extract_tiktok_tags(video_description)
                default_tags = ["shorts", "tiktok", "viral", "trending"]
                combined_tags = list(dict.fromkeys(default_tags + tiktok_tags))
                hashtag_string = " ".join([f"#{tag}" for tag in combined_tags])
                full_description = f"{video_description}\n\nCredit to @{username} on TikTok.\n\n{hashtag_string}"
                
                logging.info(f"Attempting to upload new video {upload_count + 1} of {min(new_videos_found, MAX_UPLOADS_PER_DAY)} (ID: {video_id})")
                
                if upload_to_youtube(youtube, video_path, title, full_description, combined_tags):
                    log_uploaded_id(video_id, UPLOAD_LOG_FILE)
                    upload_count += 1
                    
                    if upload_count < MAX_UPLOADS_PER_DAY and DELAY_SECONDS > 0:
                        logging.info(f"Waiting for {DELAY_SECONDS / 3600:.2f} hours before next upload.")
                        time.sleep(DELAY_SECONDS)
                else:
                    logging.warning(f"Failed to upload {video_path}. It will be retried on the next run.")
            else:
                logging.warning(f"Video file not found for ID {video_id}: {video_path}")
        
        if new_videos_found == 0:
            logging.info("No new videos found to upload.")

    except Exception as e:
        logging.critical("A critical error occurred in the upload process.", exc_info=True)
