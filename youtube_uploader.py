import os
import csv
import logging
import time
from googleapiclient.http import MediaFileUpload
from auth import get_authenticated_service

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_tiktok_tags(description):
    """
    Extracts hashtags from a video description string and cleans them for YouTube.
    """
    if not description:
        return []
    
    tags = [word.strip('#,.-_') for word in description.split() if word.startswith('#')]
    logging.info(f"Extracted {len(tags)} tags from description: {tags}")
    return tags

def upload_to_youtube(youtube, video_path, title, description, tags):
    """
    Uploads a single video to YouTube and returns True on success, False on failure.
    """
    try:
        logging.info(f"Uploading video: {video_path} with title: {title}")

        body = {
            "snippet": {
                "title": title,
                "description": description, 
                "tags": tags,             # The tags are also in the metadata field
                "categoryId": "22"        # Category ID 22 is "People & Blogs"
            },
            "status": {
                "privacyStatus": "public",
                "selfDeclaredMadeForKids": False
            }
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
        logging.error(f"Error uploading video '{title}': {e}")
        return False

def process_and_upload_clips(download_dir):
    """
    Processes and uploads a maximum of 5 clips per day, spaced out evenly.
    """
    MAX_UPLOADS_PER_DAY = 5
    # Delay calculated to spread uploads over 24 hours
    DELAY_SECONDS = (24 * 60 * 60) // MAX_UPLOADS_PER_DAY

    try:
        youtube = get_authenticated_service()
        metadata_path = os.path.join(download_dir, 'metadata.csv')
        
        if not os.path.exists(metadata_path):
            logging.error(f"Metadata file not found: {metadata_path}")
            return

        upload_count = 0
        with open(metadata_path, mode='r', encoding='utf-8') as csv_file:
            videos_to_upload = list(csv.DictReader(csv_file))

            for row in videos_to_upload:
                if upload_count >= MAX_UPLOADS_PER_DAY:
                    logging.info(f"Daily upload limit of {MAX_UPLOADS_PER_DAY} reached.")
                    break

                video_id = row['video_id']
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
                    

                    logging.info(f"Attempting to upload video {upload_count + 1} of {MAX_UPLOADS_PER_DAY}...")
                    
                    if upload_to_youtube(youtube, video_path, title, full_description, combined_tags):
                        upload_count += 1
                        if upload_count < MAX_UPLOADS_PER_DAY and upload_count < len(videos_to_upload):
                            logging.info(f"Waiting for {DELAY_SECONDS / 3600:.2f} hours before next upload.")
                            time.sleep(DELAY_SECONDS)
                    else:
                        logging.warning(f"Failed to upload {video_path}. Skipping.")
                else:
                    logging.warning(f"Video file not found for ID {video_id}: {video_path}")

    except Exception as e:
        logging.error(f"A critical error occurred in the upload process: {e}")

if __name__ == '__main__':
    DOWNLOAD_DIRECTORY = 'path/to/your/downloads' 
    process_and_upload_clips(DOWNLOAD_DIRECTORY)
