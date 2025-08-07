import os
import shutil
import logging
import pyktok as pyk
import pandas as pd

def _load_processed_creators(progress_log_path):
    """Reads the progress log and returns a set of processed creator usernames."""
    if not os.path.exists(progress_log_path):
        return set()
    with open(progress_log_path, 'r', encoding='utf-8') as f:
        return {line.strip() for line in f}

def _log_processed_creator(creator, progress_log_path):
    """Adds a creator's username to the progress log."""
    with open(progress_log_path, 'a', encoding='utf-8') as f:
        f.write(f"{creator}\n")

def _download_single_creator(username, download_dir, videos_per_creator):
    """Downloads clips for a single creator and returns the path to their metadata."""
    temp_metadata_path = os.path.join(download_dir, f"temp_{username}_metadata.csv")
    logging.info(f"Downloading up to {videos_per_creator} videos for user: {username}")
    
    try:
        pyk.specify_browser("edge")
        pyk.save_tiktok_multi_page(
            username,
            ent_type='user',
            save_video=True,
            metadata_fn=temp_metadata_path,
            video_ct=videos_per_creator
        )
        
        for filename in os.listdir(os.getcwd()):
            if filename.startswith(f"@{username}") and filename.endswith(".mp4"):
                shutil.move(os.path.join(os.getcwd(), filename), os.path.join(download_dir, filename))
        
        return temp_metadata_path
    except Exception as e:
        logging.error(f"Failed to download videos for {username}: {e}", exc_info=True)
        return None

def download_and_combine_clips(creators, download_dir, progress_log_path, metadata_path, videos_per_creator):
    """
    Downloads videos from a list of creators, skipping those already processed
    in the current run, and appends their metadata to the main CSV file.
    """
    os.makedirs(download_dir, exist_ok=True)
    processed_creators = _load_processed_creators(progress_log_path)
    logging.info(f"Resuming run. Found {len(processed_creators)} already processed creators.")

    for creator in creators:
        if creator in processed_creators:
            logging.info(f"Skipping already processed creator: {creator}")
            continue

        temp_csv_path = _download_single_creator(creator, download_dir, videos_per_creator)
        
        if temp_csv_path and os.path.exists(temp_csv_path):
            try:
                df = pd.read_csv(temp_csv_path)
                if not df.empty:
                    # Append to the main CSV, writing header only if file is new
                    header = not os.path.exists(metadata_path)
                    df.to_csv(metadata_path, mode='a', header=header, index=False, encoding='utf-8')
                    logging.info(f"Appended metadata for {creator} to the main CSV.")
                    
                    # Mark this creator as done for this run
                    _log_processed_creator(creator, progress_log_path)
            except pd.errors.EmptyDataError:
                logging.warning(f"Metadata file for {creator} was empty. Skipping.")
            finally:
                os.remove(temp_csv_path) # Clean up temp file
        else:
            logging.warning(f"Could not retrieve or find metadata for {creator}.")

    if os.path.exists(metadata_path):
        # Final check: de-duplicate the master CSV just in case
        final_df = pd.read_csv(metadata_path)
        final_df.drop_duplicates(subset='video_id', inplace=True)
        final_df.to_csv(metadata_path, index=False, encoding='utf-8')
        logging.info(f"Download phase complete. Final metadata contains {len(final_df)} unique videos.")
    else:
        logging.warning("Download phase complete, but no metadata was collected.")
