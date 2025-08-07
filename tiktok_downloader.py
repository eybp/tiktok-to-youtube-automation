import os
import shutil
import logging
import pyktok as pyk
import pandas as pd

def _download_single_creator(username, download_dir, videos_per_creator):
    """Downloads clips and metadata for a single creator to a temporary CSV."""
    temp_metadata_path = os.path.join(download_dir, f"temp_{username}_metadata.csv")
    logging.info(f"Downloading up to {videos_per_creator} videos for user: {username}")
    
    try:
        pyk.specify_browser("edge")
        pyk.save_tiktok_multi_page(
            username,
            ent_type='user',
            save_video=True,
            metadata_fn=temp_metadata_path,
            video_ct=videos_per_creator  # Use the passed-in value here
        )
        
        for filename in os.listdir(os.getcwd()):
            if filename.startswith(f"@{username}") and filename.endswith(".mp4"):
                shutil.move(os.path.join(os.getcwd(), filename), os.path.join(download_dir, filename))
        
        return temp_metadata_path
    except Exception as e:
        logging.error(f"Failed to download videos for {username}: {e}", exc_info=True)
        return None

def download_and_combine_clips(creators, download_dir, videos_per_creator):
    """
    Downloads videos from a list of creators and combines their metadata.
    """
    os.makedirs(download_dir, exist_ok=True)
    all_metadata_dfs = []
    
    for creator in creators:
        # Pass the download limit down to the single creator function
        temp_csv_path = _download_single_creator(creator, download_dir, videos_per_creator)
        if temp_csv_path and os.path.exists(temp_csv_path):
            try:
                df = pd.read_csv(temp_csv_path)
                all_metadata_dfs.append(df)
                logging.info(f"Successfully processed metadata for {creator}.")
            except pd.errors.EmptyDataError:
                logging.warning(f"Metadata file for {creator} was empty. Skipping.")
            finally:
                os.remove(temp_csv_path)
        else:
            logging.warning(f"Could not retrieve or find metadata for {creator}.")

    if not all_metadata_dfs:
        logging.error("No metadata was collected from any creator. Halting process.")
        return

    combined_df = pd.concat(all_metadata_dfs, ignore_index=True)
    combined_df.drop_duplicates(subset='video_id', inplace=True)

    main_metadata_path = os.path.join(download_dir, 'metadata.csv')
    combined_df.to_csv(main_metadata_path, index=False, encoding='utf-8')
    logging.info(f"All metadata combined into '{main_metadata_path}' with {len(combined_df)} unique videos.")
