import logging
from typing import Any

from telethon.tl.types import Message

from telegram_signals.client import get_client, search_public_chats
from telegram_signals.keywords import CHAT_DISCOVERY_KEYWORDS
from telegram_signals.signal_classifier import classify_signal
from telegram_signals.repository import save_signals


logger = logging.getLogger(__name__)

IGNORED_CHAT_MARKERS = [
    "travel",
    "банк",
    "bank",
    "design",
    "дизайн",
    "скид",
    "купон",
    "вакан",
    "резюме",
    "работа",
    "курс",
    "обуч",
    "crypto",
    "крипт",
    "game",
    "игр",
]

PREFERRED_CHAT_MARKERS = [
    "wb",
    "wildberries",
    "ozon",
    "маркетплейс",
    "селлер",
    "поставщик",
    "бренд",
    "ecom",
    "ecommerce",
    "интернет-магазин",
    "производ",
]

ALLOWED_MESSAGE_TYPES = {"pain", "need_contractor", "direct_growth", "brand_signal"}


def _chat_url(chat: Any) -> str | None:
    username = getattr(chat, "username", None)
    if username:
        return f"https://t.me/{username}"
    return None


def _chat_title(chat: Any) -> str:
    return getattr(chat, "title", None) or getattr(chat, "username", None) or str(getattr(chat, "id", "unknown"))


def _author_username(message: Message) -> str | None:
    sender = getattr(message, "sender", None)
    if sender:
        return getattr(sender, "username", None)
    return None


def _is_relevant_chat(chat: Any) -> bool:
    title = (_chat_title(chat) or "").lower()
    username = (getattr(chat, "username", None) or "").lower()
    haystack = f"{title} {username}".strip()
    if not haystack:
        return False
    if any(marker in haystack for marker in IGNORED_CHAT_MARKERS):
        return False
    return any(marker in haystack for marker in PREFERRED_CHAT_MARKERS)


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
                if not _is_relevant_chat(chat):
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
                if len(text) < 50:
                    continue

                author_username = _author_username(msg)
                signal = classify_signal(text, segment, author_username=author_username)
                if signal["level"] == "low":
                    continue
                if signal["message_type"] not in ALLOWED_MESSAGE_TYPES:
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
                    "author_id": str(getattr(msg, "sender_id", "")) if getattr(msg, "sender_id", None) else None,
                    "author_name": None,
                    "author_username": author_username,
                    "message_text": text,
                    "text_excerpt": text[:280],
                    "matched_keywords": ",".join(signal["matches"]),
                    "message_type": signal["message_type"],
                    "icp_score": signal["icp_score"],
                    "pain_score": signal["pain_score"],
                    "intent_score": signal["intent_score"],
                    "contactability_score": signal["contactability_score"],
                    "is_actionable": signal["is_actionable"],
                    "contact_hint": signal["contact_hint"],
                    "company_hint": signal["company_hint"],
                    "website_hint": signal["website_hint"],
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
