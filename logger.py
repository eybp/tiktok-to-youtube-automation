import logging
import requests
import os
import sys

class DiscordHandler(logging.Handler):
    """A custom logging handler that sends logs to a Discord webhook."""
    def __init__(self, webhook_url):
        super().__init__()
        self.webhook_url = webhook_url
        self.colors = {
            'DEBUG': 0x808080,
            'INFO': 0x0000FF,
            'WARNING': 0xFFFF00,
            'ERROR': 0xFF0000,
            'CRITICAL': 0x8B0000
        }

    def emit(self, record):
        """Format and send the log record to Discord."""
        try:
            log_entry = self.format(record)
            if len(log_entry) > 1900:
                log_entry = log_entry[:1900] + "..."

            embed = {
                "title": f"Log: {record.levelname}",
                "description": f"```\n{log_entry}\n```",
                "color": self.colors.get(record.levelname, 0x000000)
            }
            
            payload = {"embeds": [embed]}
            # Disable internal logging from requests itself during the post
            logging.getLogger("urllib3").propagate = False
            requests.post(self.webhook_url, json=payload, timeout=5)
            logging.getLogger("urllib3").propagate = True

        except Exception as e:
            sys.stderr.write(f"--- FATAL: DiscordHandler failed to send log: {e}\n")
            pass

def setup_logger():
    """Configure logging settings, including the Discord handler."""
    
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    if logger.hasHandlers():
        logger.handlers.clear()

    log_format = logging.Formatter("%(asctime)s - %(levelname)s - %(module)s - %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(log_format)
    logger.addHandler(stream_handler)
    
    file_handler = logging.FileHandler("tiktok_to_youtube.log", encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)

    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    if webhook_url and webhook_url.startswith("https://discord.com/api/webhooks/"):
        discord_handler = DiscordHandler(webhook_url)
        discord_handler.setLevel(logging.DEBUG)
        discord_handler.setFormatter(logging.Formatter('%(message)s'))
        logger.addHandler(discord_handler)
        logger.info("Discord logging is enabled.")
    else:
        logger.warning("DISCORD_WEBHOOK_URL is missing or invalid. Discord logging is disabled.")
