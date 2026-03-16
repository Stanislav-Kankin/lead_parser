import logging
from typing import Any

from telethon.tl.types import Message

from telegram_signals.client import get_client, search_public_chats
from telegram_signals.keywords import CHAT_DISCOVERY_KEYWORDS
from telegram_signals.signal_classifier import classify_signal
from telegram_signals.repository import save_signals


logger = logging.getLogger(__name__)


def _chat_url(chat: Any) -> str | None:
    username = getattr(chat, "username", None)
    if username:
        return f"https://t.me/{username}"
    return None


def _chat_title(chat: Any) -> str:
    return getattr(chat, "title", None) or getattr(chat, "username", None) or str(getattr(chat, "id", "unknown"))


CHAT_TITLE_POSITIVE_HINTS = (
    "wb",
    "wildberries",
    "ozon",
    "маркетплей",
    "селлер",
    "поставщик",
    "ecom",
    "e-commerce",
    "ecommerce",
    "интернет-магаз",
    "товарк",
    "товарн",
)

CHAT_TITLE_NEGATIVE_HINTS = (
    "travel",
    "банк",
    "bank",
    "design",
    "дизайн",
    "скидк",
    "купон",
    "coupon",
    "veterin",
    "ветерин",
    "работа",
    "ваканс",
    "резюме",
    "course",
    "курс",
    "crypto",
    "крипт",
)

def _is_relevant_chat(chat: Any, segment: str) -> bool:
    title = (_chat_title(chat) or "").lower()
    username = (getattr(chat, "username", None) or "").lower()
    haystack = f"{title} {username}".strip()

    if not haystack:
        return False

    if any(token in haystack for token in CHAT_TITLE_NEGATIVE_HINTS):
        return False

    if segment == "manufacturer_secondary":
        return any(token in haystack for token in ("бренд", "производ", "опт", "market", "маркетплей", "ecom"))

    return any(token in haystack for token in CHAT_TITLE_POSITIVE_HINTS)


def _author_username(message: Message) -> str | None:
    sender = getattr(message, "sender", None)
    if sender:
        return getattr(sender, "username", None)
    return None


async def _collect_messages_from_chat(client, chat, limit_per_chat: int) -> list[Message]:
    messages: list[Message] = []

    async for msg in client.iter_messages(chat, limit=limit_per_chat):
        if not msg or not getattr(msg, "message", None):
            continue
        messages.append(msg)

    return messages


async def collect_signals(segment: str, limit_chats: int = 12, limit_messages_per_chat: int = 80) -> dict:
    if segment not in CHAT_DISCOVERY_KEYWORDS:
        raise ValueError(f"Неизвестный сегмент: {segment}")

    client = get_client()
    await client.connect()

    scanned_chats = 0
    scanned_messages = 0
    kept_signals = 0

    seen_chat_ids = set()
    discovered_chats = []
    collected_items = []

    try:
        for query in CHAT_DISCOVERY_KEYWORDS[segment]:
            chats = await search_public_chats(client, query, limit=limit_chats)

            for chat in chats:
                chat_id = getattr(chat, "id", None)
                if not chat_id or chat_id in seen_chat_ids:
                    continue
                if not _is_relevant_chat(chat, segment):
                    logger.info(
                        "[telegram_signals] skip_chat_irrelevant segment=%s chat=%s",
                        segment,
                        _chat_title(chat),
                    )
                    continue

                seen_chat_ids.add(chat_id)
                discovered_chats.append(chat)

        logger.info(
            "[telegram_signals] discovered_chats segment=%s total=%s",
            segment,
            len(discovered_chats),
        )

        for idx, chat in enumerate(discovered_chats, start=1):
            scanned_chats += 1
            title = _chat_title(chat)

            logger.info(
                "[telegram_signals] scan_chat segment=%s chat=%s (%s/%s)",
                segment,
                title,
                idx,
                len(discovered_chats),
            )

            try:
                messages = await _collect_messages_from_chat(client, chat, limit_messages_per_chat)
            except Exception as exc:
                logger.warning(
                    "[telegram_signals] scan_chat_failed chat=%s error=%s",
                    title,
                    exc,
                )
                continue

            scanned_messages += len(messages)

            for msg in messages:
                text = (msg.message or "").strip()
                if len(text) < 40:
                    continue

                signal = classify_signal(text, segment)
                if signal["level"] == "low":
                    continue

                kept_signals += 1

                collected_items.append({
                    "source_query": segment,
                    "segment": segment,
                    "chat_id": str(getattr(chat, "id", "")),
                    "chat_title": title,
                    "chat_username": getattr(chat, "username", None),
                    "chat_url": _chat_url(chat),
                    "message_id": msg.id,
                    "message_date": msg.date,
                    "author_id": str(msg.sender_id) if getattr(msg, "sender_id", None) is not None else None,
                    "author_name": None,
                    "author_username": _author_username(msg),
                    "message_text": text,
                    "text_excerpt": text[:280],
                    "matched_keywords": ",".join(signal["matches"]),
                    "signal_score": signal["score"],
                    "signal_level": signal["level"],
                    "recommended_opener": signal["recommended_opener"],
                    "status": "new",
                })

        save_stats = save_signals(collected_items) if collected_items else {"created": 0, "updated": 0}

        logger.info(
            "[telegram_signals] done segment=%s chats=%s messages=%s kept=%s created=%s updated=%s",
            segment,
            scanned_chats,
            scanned_messages,
            kept_signals,
            save_stats["created"],
            save_stats["updated"],
        )

        return {
            "created": save_stats["created"],
            "updated": save_stats["updated"],
            "scanned_chats": scanned_chats,
            "scanned_messages": scanned_messages,
            "kept_signals": kept_signals,
        }

    finally:
        await client.disconnect()