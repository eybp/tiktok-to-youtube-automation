import discord
from discord.ext import commands
import os
import json
import threading
import logging
from dotenv import load_dotenv
import worker
from logger import setup_logger
import asyncio
import signal

# --- Setup ---
load_dotenv()
setup_logger()
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

# --- Global state management for the worker thread ---
worker_thread = None
stop_event = None
CONFIG_PATH = "config.json"
CONFIG_LOCK = threading.Lock()

def load_config():
    with CONFIG_LOCK:
        with open(CONFIG_PATH, 'r') as f:
            return json.load(f)

def save_config(data):
    with CONFIG_LOCK:
        with open(CONFIG_PATH, 'w') as f:
            json.dump(data, f, indent=2)

# --- Control Panel UI View ---
class ControlPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.update_button_states()

    def update_button_states(self):
        global worker_thread
        is_running = worker_thread is not None and worker_thread.is_alive()
        
        self.start_button.disabled = is_running
        self.stop_button.disabled = not is_running
        self.restart_button.disabled = is_running # Disable restart while running to simplify logic

    @discord.ui.button(label="Start", style=discord.ButtonStyle.green, custom_id="persistent_start")
    async def start_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        global worker_thread, stop_event
        if worker_thread is None or not worker_thread.is_alive():
            stop_event = threading.Event()
            worker_thread = threading.Thread(target=worker.run_bot_cycle, args=(stop_event,), daemon=True)
            worker_thread.start()
            await interaction.response.send_message("✅ Bot process started in the background.", ephemeral=True)
            logging.info("Bot process started via Discord.")
        else:
            await interaction.response.send_message("❌ Bot is already running.", ephemeral=True)
        self.update_button_states()
        await interaction.message.edit(view=self)

    @discord.ui.button(label="Stop", style=discord.ButtonStyle.red, custom_id="persistent_stop")
    async def stop_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        global worker_thread, stop_event
        if worker_thread and worker_thread.is_alive() and stop_event:
            stop_event.set()
            await interaction.response.send_message("⏳ Sending stop signal... The worker will stop shortly.", ephemeral=True)
            logging.info("Stop signal sent to worker thread.")
        else:
            await interaction.response.send_message("❌ Bot is not currently running.", ephemeral=True)
        self.update_button_states()
        await interaction.message.edit(view=self)
        
    @discord.ui.button(label="Restart", style=discord.ButtonStyle.blurple, custom_id="persistent_restart")
    async def restart_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        global worker_thread, stop_event
        # This button is now only usable when the bot is stopped.
        if worker_thread and worker_thread.is_alive():
             await interaction.response.send_message("❌ Please stop the bot before restarting.", ephemeral=True)
             return
        
        await self.start_button.callback(interaction) # Just call the start logic

# --- Bot Events and Commands (No Changes Here) ---
@bot.event
async def on_ready():
    bot.add_view(ControlPanelView())
    await bot.tree.sync()
    logging.info(f'Logged in as {bot.user}. Bot is ready.')

@bot.tree.command(name="panel", description="Displays the bot control panel.")
async def panel(interaction: discord.Interaction):
    embed = discord.Embed(
        title="Bot Control Panel",
        description="Use the buttons below to manage the TikTok downloader and uploader.",
        color=discord.Color.blue()
    )
    is_running = worker_thread is not None and worker_thread.is_alive()
    status = "Running" if is_running else "Stopped"
    embed.add_field(name="Current Status", value=f"**{status}**", inline=False)
    
    view = ControlPanelView()
    await interaction.response.send_message(embed=embed, view=view)

@bot.tree.command(name="status", description="Checks the current status of the bot.")
async def status(interaction: discord.Interaction):
    is_running = worker_thread is not None and worker_thread.is_alive()
    status = "✅ Running" if is_running else "❌ Stopped"
    await interaction.response.send_message(f"The bot process is currently: **{status}**", ephemeral=True)

# --- Config Management Commands ---
creators_group = discord.app_commands.Group(name="creators", description="Manage the list of TikTok creators.")

@creators_group.command(name="list", description="Lists all current TikTok creators.")
async def list_creators(interaction: discord.Interaction):
    config = load_config()
    creator_list = "\n".join([f"- `{creator}`" for creator in config["tiktok_creators"]])
    await interaction.response.send_message(f"**Current Creators:**\n{creator_list}", ephemeral=True)

@creators_group.command(name="add", description="Adds a new TikTok creator to the list.")
async def add_creator(interaction: discord.Interaction, username: str):
    config = load_config()
    if username.lower() not in config["tiktok_creators"]:
        config["tiktok_creators"].append(username.lower())
        save_config(config)
        await interaction.response.send_message(f"✅ Added `{username}` to the creator list.", ephemeral=True)
    else:
        await interaction.response.send_message(f"❌ `{username}` is already in the list.", ephemeral=True)

@creators_group.command(name="remove", description="Removes a TikTok creator from the list.")
async def remove_creator(interaction: discord.Interaction, username: str):
    config = load_config()
    if username.lower() in config["tiktok_creators"]:
        config["tiktok_creators"].remove(username.lower())
        save_config(config)
        await interaction.response.send_message(f"✅ Removed `{username}` from the creator list.", ephemeral=True)
    else:
        await interaction.response.send_message(f"❌ `{username}` was not found in the list.", ephemeral=True)

bot.tree.add_command(creators_group)

@bot.tree.command(name="config", description="Set bot configuration values.")
async def config(interaction: discord.Interaction, uploads_per_day: int, downloads_per_creator: int):
    config = load_config()
    config["max_uploads_per_day"] = uploads_per_day
    config["videos_to_check_per_creator"] = downloads_per_creator
    save_config(config)
    await interaction.response.send_message(
        f"✅ Config updated:\n"
        f"- Uploads per day: `{uploads_per_day}`\n"
        f"- Downloads per creator: `{downloads_per_creator}`",
        ephemeral=True
    )

async def shutdown(signal, loop):
    """Handles the graceful shutdown of the bot and worker thread."""
    logging.warning(f"Received exit signal {signal.name}... shutting down.")
    global worker_thread, stop_event
    if worker_thread and worker_thread.is_alive() and stop_event:
        logging.info("Signaling worker thread to stop...")
        stop_event.set()
        # Give the thread a moment to stop
        worker_thread.join(timeout=10)
        if worker_thread.is_alive():
            logging.warning("Worker thread did not stop in time.")

    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    logging.debug(f"Cancelling {len(tasks)} outstanding tasks.")
    await asyncio.gather(*tasks, return_exceptions=True)
    
    logging.info("Closing Discord connection.")
    await bot.close()
    loop.stop()

# --- Run the Bot ---
if __name__ == "__main__":
    bot_token = os.getenv("DISCORD_BOT_TOKEN")
    if not bot_token:
        logging.critical("DISCORD_BOT_TOKEN not found in .env file. Bot cannot start.")
    else:
        bot.run(bot_token)
