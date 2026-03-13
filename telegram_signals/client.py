import os
import logging

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.functions.contacts import SearchRequest
from telethon.tl.types import InputPeerEmpty

load_dotenv()

logger = logging.getLogger(__name__)


def get_client() -> TelegramClient:
    api_id_raw = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH")
    session_name = os.getenv("TELEGRAM_SESSION_NAME", "lead_signal_session")

    if not api_id_raw or not api_hash:
        raise RuntimeError("Не заданы TELEGRAM_API_ID / TELEGRAM_API_HASH в .env")

    api_id = int(api_id_raw)
    return TelegramClient(session_name, api_id, api_hash)


async def search_public_chats(client: TelegramClient, query: str, limit: int = 20) -> list:
    """
    Ищем публичные чаты/каналы по короткому запросу.
    """
    logger.info("[telegram_signals] discover_chats query=%s", query)

    result = await client(SearchRequest(
        q=query,
        limit=limit,
    ))

    chats = []
    seen_ids = set()

    for chat in result.chats:
        chat_id = getattr(chat, "id", None)
        username = getattr(chat, "username", None)

        if not chat_id or chat_id in seen_ids:
            continue

        # Берем только сущности, у которых есть title.
        title = getattr(chat, "title", None)
        if not title:
            continue

        seen_ids.add(chat_id)
        chats.append(chat)

    logger.info("[telegram_signals] discover_chats_done query=%s found=%s", query, len(chats))
    return chats