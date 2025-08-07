import logging
from dotenv import load_dotenv

# Load environment variables from .env file FIRST
load_dotenv()

from tiktok_downloader import download_and_combine_clips
from youtube_uploader import process_and_upload_clips
from logger import setup_logger

if __name__ == "__main__":
    # Set up all logging handlers, including Discord
    setup_logger()

    # --- Main Configuration ---
    # List all your TikTok creators here
    TIKTOK_CREATORS = ["tipsykun", "srtarixz", "poud1e"]
    
    # Set how many recent videos to check for each creator
    VIDEOS_TO_CHECK_PER_CREATOR = 1
    
    # Set how many new videos to upload in a 24-hour period
    MAX_UPLOADS_PER_DAY = 5
    
    DOWNLOAD_DIR = "./tiktok_downloads"

    try:
        logging.info(f"Starting process for {len(TIKTOK_CREATORS)} creators.")
        logging.info(f"Checking the {VIDEOS_TO_CHECK_PER_CREATOR} most recent videos per creator.")
        logging.info(f"Daily upload limit set to {MAX_UPLOADS_PER_DAY} videos.")
        
        # Pass the download limit to the downloader function
        download_and_combine_clips(
            creators=TIKTOK_CREATORS,
            download_dir=DOWNLOAD_DIR,
            videos_per_creator=VIDEOS_TO_CHECK_PER_CREATOR
        )
        
        # Pass the upload limit to the uploader function
        process_and_upload_clips(
            download_dir=DOWNLOAD_DIR,
            max_uploads=MAX_UPLOADS_PER_DAY
        )
        
        logging.info("Process completed successfully.")

    except Exception as e:
        logging.critical(f"A critical error occurred in the main process.", exc_info=True)
