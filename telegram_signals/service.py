import logging
from typing import Any
from datetime import datetime, timedelta, timezone

from telethon.tl.types import Message

from telegram_signals.client import get_client, search_public_chats
from telegram_signals.conversation.thread_builder import build_thread_views
from telegram_signals.keywords import CHAT_DISCOVERY_KEYWORDS
from telegram_signals.repository import save_signals
from telegram_signals.signal_classifier import classify_signal


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


def _author_name(message: Message) -> str | None:
    sender = getattr(message, "sender", None)
    if not sender:
        return None

    first_name = (getattr(sender, "first_name", None) or "").strip()
    last_name = (getattr(sender, "last_name", None) or "").strip()
    full_name = " ".join(part for part in [first_name, last_name] if part).strip()
    if full_name:
        return full_name

    title = (getattr(sender, "title", None) or "").strip()
    return title or None


def _is_recent_message(message: Message, max_age_hours: int) -> bool:
    message_date = getattr(message, "date", None)
    if not message_date:
        return False
    if getattr(message_date, "tzinfo", None) is None:
        message_date = message_date.replace(tzinfo=timezone.utc)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    return message_date >= cutoff


async def _collect_messages_from_chat(client, chat, limit_per_chat: int, max_age_hours: int) -> list[Message]:
    messages: list[Message] = []

    async for msg in client.iter_messages(chat, limit=limit_per_chat):
        if not msg or not getattr(msg, "message", None):
            continue
        if not _is_recent_message(msg, max_age_hours):
            break
        messages.append(msg)

    return messages


def _signal_level(signal: dict) -> str:
    return str(signal.get("signal_level") or signal.get("level") or "low")


def _signal_score(signal: dict) -> int:
    try:
        return int(signal.get("signal_score") or signal.get("score") or 0)
    except Exception:
        return 0


def _matched_keywords(signal: dict) -> str:
    value = signal.get("matched_keywords")
    if isinstance(value, list):
        return ",".join(str(item) for item in value if item)
    if value is None:
        matches = signal.get("matches") or []
        if isinstance(matches, list):
            return ",".join(str(item) for item in matches if item)
        return str(matches or "")
    return str(value)


async def collect_signals(
    segment: str,
    limit_chats: int = 12,
    limit_messages_per_chat: int = 80,
    max_age_hours: int = 48,
) -> dict:
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
            chat_username = getattr(chat, "username", None)

            logger.info(
                "[telegram_signals] scan_chat segment=%s chat=%s (%s/%s)",
                segment,
                title,
                idx,
                len(discovered_chats),
            )

            try:
                messages = await _collect_messages_from_chat(client, chat, limit_messages_per_chat, max_age_hours)
            except Exception as exc:
                logger.warning(
                    "[telegram_signals] scan_chat_failed chat=%s error=%s",
                    title,
                    exc,
                )
                continue

            scanned_messages += len(messages)
            thread_views = build_thread_views(messages)

            for thread_view in thread_views:
                msg = thread_view.message
                text = (getattr(msg, "message", None) or "").strip()
                if len(text) < 30:
                    continue

                signal = classify_signal(
                    text,
                    segment,
                    context_text=thread_view.context_text,
                    conversation_text=thread_view.conversation_text,
                    author_username=_author_username(msg),
                    author_name=_author_name(msg),
                    chat_title=title,
                    chat_username=chat_username,
                    reply_depth=thread_view.reply_depth,
                )

                lead_fit = str(signal.get("lead_fit") or "noise")
                signal_level = _signal_level(signal)
                if lead_fit == "noise" and signal_level == "low":
                    continue

                kept_signals += 1

                collected_items.append({
                    "source_query": segment,
                    "segment": segment,
                    "chat_id": str(getattr(chat, "id", "")),
                    "chat_title": title,
                    "chat_username": chat_username,
                    "chat_url": _chat_url(chat),
                    "message_id": msg.id,
                    "message_date": msg.date,
                    "author_id": str(msg.sender_id) if getattr(msg, "sender_id", None) is not None else None,
                    "author_name": _author_name(msg),
                    "author_username": _author_username(msg),
                    "message_text": text,
                    "text_excerpt": text[:280],
                    "matched_keywords": _matched_keywords(signal),
                    "signal_score": _signal_score(signal),
                    "signal_level": signal_level,
                    "recommended_opener": signal.get("recommended_opener"),
                    "source_type": "comment" if thread_view.reply_depth >= 1 else "chat_message",
                    "is_comment": thread_view.reply_depth >= 1,
                    "parent_message_id": thread_view.parent_message_id,
                    "root_message_id": thread_view.root_message_id,
                    "reply_depth": thread_view.reply_depth,
                    "conversation_key": thread_view.conversation_key,
                    "conversation_score": signal.get("conversation_score", 0),
                    "pain_detected": signal.get("pain_detected"),
                    "icp_detected": signal.get("icp_detected"),
                    "message_type": signal.get("message_type"),
                    "conversation_type": signal.get("conversation_type"),
                    "author_type_guess": signal.get("author_type_guess"),
                    "icp_score": signal.get("icp_score", 0),
                    "pain_score": signal.get("pain_score", 0),
                    "intent_score": signal.get("intent_score", 0),
                    "context_score": signal.get("context_score", 0),
                    "owner_likelihood_score": signal.get("owner_likelihood_score", 0),
                    "promo_penalty": signal.get("promo_penalty", 0),
                    "contractor_penalty": signal.get("contractor_penalty", 0),
                    "final_lead_score": signal.get("final_lead_score", _signal_score(signal)),
                    "contactability_score": signal.get("contactability_score", 0),
                    "contact_entity_type": signal.get("contact_entity_type"),
                    "contact_entity_score": signal.get("contact_entity_score", 0),
                    "is_person_reachable": signal.get("is_person_reachable", 0),
                    "lead_fit": lead_fit,
                    "next_step": signal.get("next_step"),
                    "why_actionable": signal.get("why_actionable"),
                    "company_hint": signal.get("company_hint"),
                    "website_hint": signal.get("website_hint"),
                    "contact_hint": signal.get("contact_hint"),
                    "review_status": "unchecked",
                    "reviewed_at": None,
                    "is_actionable": signal.get("is_actionable", 0),
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
            "max_age_hours": max_age_hours,
        }

    finally:
        await client.disconnect()
