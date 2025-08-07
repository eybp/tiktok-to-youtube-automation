import logging
import os
import sys
from dotenv import load_dotenv

load_dotenv()

from tiktok_downloader import download_and_combine_clips
from youtube_uploader import process_and_upload_clips
from logger import setup_logger

DOWNLOAD_DIR = "./tiktok_downloads"
METADATA_CSV_PATH = os.path.join(DOWNLOAD_DIR, 'metadata.csv')
DOWNLOAD_PROGRESS_LOG = os.path.join(DOWNLOAD_DIR, 'download_progress.log')
RUN_COMPLETE_MARKER = os.path.join(DOWNLOAD_DIR, 'run_complete.marker')


def manage_run_state():
    """
    Checks for a completion marker to automatically decide if this is a fresh or resumed run.
    """
    if os.path.exists(RUN_COMPLETE_MARKER):
        logging.info("Completion marker found. Previous run finished successfully.")
        logging.info("--- Starting a fresh run. Clearing previous state. ---")
        
        if os.path.exists(METADATA_CSV_PATH):
            os.remove(METADATA_CSV_PATH)
        if os.path.exists(DOWNLOAD_PROGRESS_LOG):
            os.remove(DOWNLOAD_PROGRESS_LOG)
        
        os.remove(RUN_COMPLETE_MARKER)
    else:
        logging.warning("No completion marker found. Attempting to resume previous run.")


if __name__ == "__main__":
    setup_logger()

    TIKTOK_CREATORS = ["tipsykun", "srtarixz", "poud1e"]
    VIDEOS_TO_CHECK_PER_CREATOR = 3
    MAX_UPLOADS_PER_DAY = 9999999999
    
    try:
        manage_run_state()

        try:
            logging.info(f"Starting process for {len(TIKTOK_CREATORS)} creators.")
            logging.info(f"Checking the {VIDEOS_TO_CHECK_PER_CREATOR} most recent videos per creator.")
            logging.info(f"Daily upload limit set to {MAX_UPLOADS_PER_DAY} videos.")
            
            download_and_combine_clips(
                creators=TIKTOK_CREATORS,
                download_dir=DOWNLOAD_DIR,
                progress_log_path=DOWNLOAD_PROGRESS_LOG,
                metadata_path=METADATA_CSV_PATH,
                videos_per_creator=VIDEOS_TO_CHECK_PER_CREATOR
            )
            
            process_and_upload_clips(
                download_dir=DOWNLOAD_DIR,
                max_uploads=MAX_UPLOADS_PER_DAY
            )
            
            logging.info("--- Process completed successfully. ---")
            with open(RUN_COMPLETE_MARKER, 'w') as f:
                pass
            logging.info(f"Run completion marker created.")

        except Exception as e:
            logging.critical(f"A critical error occurred in the main process.", exc_info=True)

    except KeyboardInterrupt:
        logging.warning("\nCtrl+C detected. Shutting down gracefully.")
        sys.exit(130)
