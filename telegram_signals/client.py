import os
from telethon import TelegramClient


def get_client():
    api_id = int(os.getenv("TELEGRAM_API_ID"))
    api_hash = os.getenv("TELEGRAM_API_HASH")
    session_name = os.getenv("TELEGRAM_SESSION_NAME", "tg_signal_session")

    client = TelegramClient(session_name, api_id, api_hash)

    return client