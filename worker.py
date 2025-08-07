import logging
import os
import time
import json
from tiktok_downloader import download_and_combine_clips
from youtube_uploader import process_and_upload_clips

CONFIG_PATH = "config.json"
DOWNLOAD_DIR = "./tiktok_downloads"
METADATA_CSV_PATH = os.path.join(DOWNLOAD_DIR, 'metadata.csv')
DOWNLOAD_PROGRESS_LOG = os.path.join(DOWNLOAD_DIR, 'download_progress.log')
RUN_COMPLETE_MARKER = os.path.join(DOWNLOAD_DIR, 'run_complete.marker')

def manage_run_state():
    """Checks for a completion marker to decide if this is a fresh or resumed run."""
    if os.path.exists(RUN_COMPLETE_MARKER):
        logging.info("Completion marker found. Previous run finished successfully. Starting a fresh run.")
        if os.path.exists(METADATA_CSV_PATH): os.remove(METADATA_CSV_PATH)
        if os.path.exists(DOWNLOAD_PROGRESS_LOG): os.remove(DOWNLOAD_PROGRESS_LOG)
        os.remove(RUN_COMPLETE_MARKER)
    else:
        logging.warning("No completion marker found. Attempting to resume previous run.")

def run_bot_cycle(stop_event):
    """The main automation logic loop, designed to be run in a separate thread."""
    logging.info("✅ Worker thread started.")
    
    try:
        # Load configuration from the JSON file
        with open(CONFIG_PATH, 'r') as f:
            config = json.load(f)
        
        tiktok_creators = config.get("tiktok_creators", [])
        videos_per_creator = config.get("videos_to_check_per_creator", 10)
        max_uploads = config.get("max_uploads_per_day", 5)

        # Check for the stop signal before starting heavy work
        if stop_event.is_set():
            logging.warning("🛑 Worker thread received stop signal before starting.")
            return

        manage_run_state()
        
        logging.info(f"Starting process for {len(tiktok_creators)} creators.")
        download_and_combine_clips(
            creators=tiktok_creators,
            download_dir=DOWNLOAD_DIR,
            progress_log_path=DOWNLOAD_PROGRESS_LOG,
            metadata_path=METADATA_CSV_PATH,
            videos_per_creator=videos_per_creator
        )
        
        process_and_upload_clips(
            download_dir=DOWNLOAD_DIR,
            max_uploads=max_uploads,
            stop_event=stop_event  # Pass the stop event to the uploader
        )
        
        logging.info("--- Process completed successfully. ---")
        with open(RUN_COMPLETE_MARKER, 'w') as f:
            pass
        logging.info("Run completion marker created.")

    except Exception as e:
        logging.critical("A critical error occurred in the worker thread.", exc_info=True)
    finally:
        logging.info("✅ Worker thread finished.")