import logging
from typing import Any

from telethon.tl.types import Message

from telegram_signals.client import get_client, search_public_chats
from telegram_signals.keywords import CHAT_BAD_HINTS, CHAT_DISCOVERY_KEYWORDS, CHAT_GOOD_HINTS
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


def _author_username(message: Message) -> str | None:
    sender = getattr(message, "sender", None)
    if sender:
        return getattr(sender, "username", None)
    return None


def _is_relevant_chat(chat: Any) -> bool:
    title = (getattr(chat, "title", None) or "").lower()
    username = (getattr(chat, "username", None) or "").lower()
    haystack = f"{title} {username}".strip()

    if any(bad in haystack for bad in CHAT_BAD_HINTS):
        return False
    if any(good in haystack for good in CHAT_GOOD_HINTS):
        return True
    return False


def _excerpt(text: str, limit: int = 320) -> str:
    text = " ".join((text or "").split())
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


async def _collect_messages_from_chat(client, chat, limit_per_chat: int) -> list[Message]:
    messages: list[Message] = []
    async for msg in client.iter_messages(chat, limit=limit_per_chat):
        if not msg or not getattr(msg, "message", None):
            continue
        messages.append(msg)
    messages.reverse()
    return messages


def _context_window(messages: list[Message], idx: int, radius: int = 2) -> str:
    left = max(0, idx - radius)
    right = min(len(messages), idx + radius + 1)
    parts: list[str] = []
    for i in range(left, right):
        if i == idx:
            continue
        message_text = getattr(messages[i], "message", None)
        if message_text:
            parts.append(str(message_text))
    return "\n".join(parts)


async def collect_signals(
    segment: str,
    limit_chats: int = 12,
    limit_messages_per_chat: int = 80,
    context_radius: int = 2,
) -> dict:
    if segment not in CHAT_DISCOVERY_KEYWORDS:
        raise ValueError(f"Неизвестный сегмент: {segment}")

    client = get_client()
    await client.connect()

    scanned_chats = 0
    scanned_messages = 0
    kept_signals = 0

    seen_chat_ids: set[int] = set()
    discovered_chats: list[Any] = []
    collected_items: list[dict] = []

    try:
        for query in CHAT_DISCOVERY_KEYWORDS[segment]:
            chats = await search_public_chats(client, query, limit=limit_chats)

            for chat in chats:
                chat_id = getattr(chat, "id", None)
                if not chat_id or chat_id in seen_chat_ids:
                    continue
                seen_chat_ids.add(chat_id)

                if not _is_relevant_chat(chat):
                    continue

                discovered_chats.append(chat)

        logger.info("[telegram_signals] discovered_chats segment=%s total=%s", segment, len(discovered_chats))

        for idx, chat in enumerate(discovered_chats, start=1):
            scanned_chats += 1
            logger.info(
                "[telegram_signals] scan_chat segment=%s chat=%s (%s/%s)",
                segment,
                _chat_title(chat),
                idx,
                len(discovered_chats),
            )

            messages = await _collect_messages_from_chat(client, chat, limit_messages_per_chat)
            scanned_messages += len(messages)

            for msg_idx, msg in enumerate(messages):
                text = (getattr(msg, "message", None) or "").strip()
                if not text:
                    continue

                context_text = _context_window(messages, msg_idx, radius=context_radius)
                author_username = _author_username(msg)

                classification = classify_signal(
                    text,
                    segment,
                    context_text=context_text,
                    author_username=author_username,
                    chat_title=_chat_title(chat),
                    chat_username=getattr(chat, "username", None),
                )

                if classification["message_type"] in {"noise", "vacancy"}:
                    continue
                if classification["final_lead_score"] < 6:
                    continue

                item = {
                    "source_query": None,
                    "segment": segment,
                    "chat_id": str(getattr(chat, "id", "")),
                    "chat_title": _chat_title(chat),
                    "chat_username": getattr(chat, "username", None),
                    "chat_url": _chat_url(chat),
                    "message_id": getattr(msg, "id", 0),
                    "message_date": getattr(msg, "date", None),
                    "author_id": str(getattr(msg, "sender_id", "") or ""),
                    "author_name": None,
                    "author_username": author_username,
                    "message_text": text,
                    "text_excerpt": _excerpt(text),
                    "signal_score": classification["signal_score"],
                    "signal_level": classification["signal_level"],
                    "recommended_opener": classification["recommended_opener"],
                    "source_type": "chat_message",
                    "is_comment": False,
                    "parent_message_id": getattr(msg, "reply_to_msg_id", None),
                    "root_message_id": getattr(msg, "reply_to_msg_id", None) or getattr(msg, "id", 0),
                    **classification,
                }
                collected_items.append(item)
                kept_signals += 1

        result = save_signals(collected_items)
        logger.info(
            "[telegram_signals] done segment=%s chats=%s messages=%s kept=%s created=%s updated=%s",
            segment,
            scanned_chats,
            scanned_messages,
            kept_signals,
            result["created"],
            result["updated"],
        )
        return result
    finally:
        await client.disconnect()
