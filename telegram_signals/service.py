from __future__ import annotations

import logging

from telethon.errors import FloodWaitError

from .client import get_client
from .keywords import SEGMENT_QUERIES
from .signal_classifier import classify_signal
from .repository import save_signals

logger = logging.getLogger(__name__)


def _peer_to_url(entity) -> str | None:
    username = getattr(entity, "username", None)
    if username:
        return f"https://t.me/{username}"
    return None


def _author_name(sender) -> str | None:
    if not sender:
        return None
    first = getattr(sender, "first_name", None) or ""
    last = getattr(sender, "last_name", None) or ""
    full = (first + " " + last).strip()
    return full or getattr(sender, "title", None)


async def collect_signals(segment: str, per_query_limit: int = 25) -> dict:
    queries = SEGMENT_QUERIES.get(segment)
    if not queries:
        raise RuntimeError(f"Неизвестный сегмент: {segment}")

    client = get_client()
    await client.start()

    collected: list[dict] = []

    try:
        for query in queries:
            logger.info("[telegram_signals] search query=%s segment=%s", query, segment)
            try:
                async for msg in client.iter_messages(None, search=query, limit=per_query_limit):
                    text = getattr(msg, "message", None)
                    if not text:
                        continue

                    signal = classify_signal(text)
                    if signal["level"] == "low":
                        continue

                    chat = await msg.get_chat() if msg.chat_id else None
                    sender = None
                    try:
                        sender = await msg.get_sender()
                    except Exception:
                        sender = None

                    chat_title = getattr(chat, "title", None) or getattr(chat, "username", None) or str(getattr(msg, "chat_id", "-"))
                    chat_username = getattr(chat, "username", None)

                    item = {
                        "source_query": query,
                        "segment": signal["segment"],
                        "chat_id": str(getattr(msg, "chat_id", None) or ""),
                        "chat_title": chat_title,
                        "chat_username": chat_username,
                        "chat_url": _peer_to_url(chat),
                        "message_id": msg.id,
                        "message_date": getattr(msg, "date", None),
                        "author_id": str(getattr(msg, "sender_id", None) or ""),
                        "author_name": _author_name(sender),
                        "author_username": getattr(sender, "username", None),
                        "message_text": text,
                        "text_excerpt": text[:280],
                        "matched_keywords": ", ".join(signal["matches"][:12]),
                        "signal_score": signal["score"],
                        "signal_level": signal["level"],
                        "recommended_opener": signal["opener"],
                        "status": "new",
                    }
                    collected.append(item)
            except FloodWaitError as e:
                logger.warning("[telegram_signals] flood wait query=%s wait=%s", query, e.seconds)
                break
    finally:
        await client.disconnect()

    stats = save_signals(collected) if collected else {"created": 0, "updated": 0}
    logger.info("[telegram_signals] done segment=%s created=%s updated=%s total=%s", segment, stats["created"], stats["updated"], len(collected))
    return {"items": collected, **stats}
