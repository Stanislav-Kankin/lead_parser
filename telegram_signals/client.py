import os
from dotenv import load_dotenv
from telethon import TelegramClient

load_dotenv()


def get_client():
    api_id = int(os.getenv("TELEGRAM_API_ID"))
    api_hash = os.getenv("TELEGRAM_API_HASH")
    session_name = os.getenv("TELEGRAM_SESSION_NAME", "tg_signal_session")

    if not api_id or not api_hash:
        raise RuntimeError("Не заданы TELEGRAM_API_ID / TELEGRAM_API_HASH в .env")

    return TelegramClient(session_name, api_id, api_hash)