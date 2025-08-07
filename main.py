import logging
from dotenv import load_dotenv

load_dotenv()

from tiktok_downloader import download_tiktok_clips
from youtube_uploader import process_and_upload_clips
from logger import setup_logger

if __name__ == "__main__":
    setup_logger()

    try:
        TIKTOK_USERNAME = "camm4x"  # Replace with your TikTok username
        DOWNLOAD_DIR = "./tiktok_downloads"

        logging.info(f"Starting process for user: {TIKTOK_USERNAME}")
        download_tiktok_clips(TIKTOK_USERNAME, DOWNLOAD_DIR)
        process_and_upload_clips(DOWNLOAD_DIR)
        logging.info("Process completed successfully.")

    except Exception as e:
        # Log the full exception traceback to all handlers (file, console, Discord)
        logging.critical(f"A critical error occurred in the main process.", exc_info=True)
