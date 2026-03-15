import logging
from typing import Any

from telethon.tl.types import Message

from telegram_signals.client import get_client, search_public_chats
from telegram_signals.conversation.thread_builder import build_thread_views
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
    message_ids: set[int] = set()

    async for msg in client.iter_messages(chat, limit=limit_per_chat):
        if not msg or not getattr(msg, "message", None):
            continue
        message_id = getattr(msg, "id", None)
        if message_id:
            message_ids.add(int(message_id))
        messages.append(msg)

    missing_parent_ids = sorted(
        {
            int(getattr(msg, "reply_to_msg_id", 0) or 0)
            for msg in messages
            if getattr(msg, "reply_to_msg_id", None) and int(getattr(msg, "reply_to_msg_id", 0) or 0) not in message_ids
        }
    )
    if missing_parent_ids:
        fetched_parents = await client.get_messages(chat, ids=missing_parent_ids)
        for parent in fetched_parents or []:
            if not parent or not getattr(parent, "message", None):
                continue
            parent_id = getattr(parent, "id", None)
            if not parent_id or int(parent_id) in message_ids:
                continue
            messages.append(parent)
            message_ids.add(int(parent_id))

    messages.sort(key=lambda item: (getattr(item, "date", None) or 0, getattr(item, "id", 0) or 0))
    return messages


async def collect_signals(
    segment: str,
    limit_chats: int = 12,
    limit_messages_per_chat: int = 80,
    context_radius: int = 2,
) -> dict:
    del context_radius  # v7: контекст теперь берём из reply chain, а не из радиуса соседних сообщений

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

            thread_views = build_thread_views(messages)
            for view in thread_views:
                msg = view.message
                text = (getattr(msg, "message", None) or "").strip()
                if not text:
                    continue

                author_username = _author_username(msg)
                classification = classify_signal(
                    text,
                    segment,
                    context_text=view.context_text,
                    conversation_text=view.conversation_text,
                    author_username=author_username,
                    chat_title=_chat_title(chat),
                    chat_username=getattr(chat, "username", None),
                    reply_depth=view.reply_depth,
                )

                if classification["message_type"] in {"noise", "vacancy"}:
                    continue
                if classification["final_lead_score"] < 7:
                    continue
                if classification["lead_fit"] == "contractor":
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
                    "source_type": "chat_message",
                    "is_comment": bool(view.parent_message_id),
                    "parent_message_id": view.parent_message_id,
                    "root_message_id": view.root_message_id,
                    "reply_depth": view.reply_depth,
                    "conversation_key": view.conversation_key,
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
