import logging
from typing import Any
from datetime import datetime, timedelta, timezone

from telethon.tl.types import Message

from storage.lead_repository import get_seen_author, upsert_seen_author
from telegram_signals.client import get_client, search_public_chats
from telegram_signals.conversation.thread_builder import build_thread_views
from telegram_signals.keywords import CHAT_DISCOVERY_KEYWORDS, PAIN_SEARCH_QUERIES, SOURCE_DISCOVERY_QUERIES
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


def _normalize_chat_ref(value: str) -> str:
    ref = (value or "").strip()
    if not ref:
        return ""
    ref = ref.replace("https://t.me/", "").replace("http://t.me/", "").strip()
    ref = ref.split("?", 1)[0].strip().strip("/")
    if ref.startswith("joinchat/") or ref.startswith("+"):
        return ref
    return ref.lstrip("@")


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
    "кэш",
    "кэшбэк",
    "малина",
    "купон",
    "coupon",
    "veterin",
    "ветерин",
    "работа",
    "ваканс",
    "резюме",
    "course",
    "курс",
    "реклама wb",
    "реклама wb и ozon",
    "crypto",
    "крипт",
    "яндекс для интернет-магазинов",
    "форум",
    "поставщики",
    "поставщик",
    "доставка",
    "китай",
    "ozon marketplace",
    "т-бизнес секреты",
    "бот модератор",
)


def _is_relevant_chat(chat: Any, segment: str, good_hints: list[str] | None = None, bad_hints: list[str] | None = None) -> bool:
    title = (_chat_title(chat) or "").lower()
    username = (getattr(chat, "username", None) or "").lower()
    haystack = f"{title} {username}".strip()

    if not haystack:
        return False

    negative = [item.lower() for item in (bad_hints or CHAT_TITLE_NEGATIVE_HINTS) if item]
    positive = [item.lower() for item in (good_hints or []) if item]

    if any(token in haystack for token in negative):
        return False

    if positive:
        return any(token in haystack for token in positive)

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


async def _search_messages_global(client, query: str, limit: int, max_age_hours: int) -> list[Message]:
    messages: list[Message] = []
    if not query.strip():
        return messages

    logger.info("[telegram_signals] global_message_search query=%s limit=%s", query, limit)
    async for msg in client.iter_messages(None, search=query, limit=limit):
        if not msg or not getattr(msg, "message", None):
            continue
        if not _is_recent_message(msg, max_age_hours):
            continue
        messages.append(msg)

    logger.info("[telegram_signals] global_message_search_done query=%s found=%s", query, len(messages))
    return messages


async def _search_messages_in_chat(client, chat, query: str, limit: int, max_age_hours: int) -> list[Message]:
    messages: list[Message] = []
    if not query.strip():
        return messages

    async for msg in client.iter_messages(chat, search=query, limit=limit):
        if not msg or not getattr(msg, "message", None):
            continue
        if not _is_recent_message(msg, max_age_hours):
            continue
        messages.append(msg)

    return messages


async def _load_configured_chats(client, source_chats: list[str]) -> list[Any]:
    chats = []
    for raw_ref in source_chats:
        ref = _normalize_chat_ref(raw_ref)
        if not ref:
            continue
        try:
            chat = await client.get_entity(ref)
        except Exception as exc:
            logger.warning("[telegram_signals] configured_chat_failed ref=%s error=%s", raw_ref, exc)
            continue
        title = getattr(chat, "title", None) or getattr(chat, "username", None)
        if title:
            chats.append(chat)
    return chats


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


async def _message_chat(client, message: Message) -> Any | None:
    chat = getattr(message, "chat", None)
    if chat is not None:
        return chat
    try:
        return await message.get_chat()
    except Exception:
        return None


async def collect_signals(
    segment: str,
    limit_chats: int = 12,
    limit_messages_per_chat: int = 80,
    max_age_hours: int = 96,
    profile: dict | None = None,
) -> dict:
    if segment not in CHAT_DISCOVERY_KEYWORDS:
        raise ValueError(f"Неизвестный сегмент: {segment}")

    client = get_client()
    await client.connect()

    scanned_chats = 0
    scanned_messages = 0
    kept_signals = 0

    seen_chat_ids = set()
    seen_message_keys = set()
    discovered_chats = []
    collected_items = []

    try:
        profile = profile or {}
        queries = profile.get("queries") or CHAT_DISCOVERY_KEYWORDS[segment]
        source_chats = profile.get("source_chats") or []
        stop_words = [item.lower() for item in profile.get("stop_words", []) if item]
        good_hints = profile.get("good_chat_hints") or None
        bad_hints = profile.get("bad_chat_hints") or None
        min_score = int(profile.get("min_score") or 0)
        chat_queries = list(dict.fromkeys([
            item
            for item in (queries + SOURCE_DISCOVERY_QUERIES.get(segment, []))
            if str(item).strip()
        ]))[:12]
        message_queries = list(dict.fromkeys([
            item
            for item in (PAIN_SEARCH_QUERIES.get(segment, []) + queries)
            if str(item).strip()
        ]))[:10]
        max_discovered_chats = max(1, int(limit_chats or 1))
        per_query_chat_limit = max(3, min(8, max_discovered_chats))

        async def process_message(
            msg: Message,
            *,
            chat: Any | None,
            title: str,
            chat_username: str | None,
            source_query: str,
            source_type: str,
            context_text: str = "",
            conversation_text: str = "",
            reply_depth: int = 0,
            parent_message_id: int | None = None,
            root_message_id: int | None = None,
            conversation_key: str | None = None,
        ) -> bool:
            text = (getattr(msg, "message", None) or "").strip()
            if len(text) < 30:
                return False
            if stop_words and any(word in text.lower() for word in stop_words):
                return False

            chat_id = str(getattr(chat, "id", "") or getattr(msg, "chat_id", "") or "")
            dedupe_key = (chat_id, int(getattr(msg, "id", 0) or 0))
            if dedupe_key in seen_message_keys:
                return False
            seen_message_keys.add(dedupe_key)

            signal = classify_signal(
                text,
                segment,
                context_text=context_text,
                conversation_text=conversation_text,
                author_username=_author_username(msg),
                author_name=_author_name(msg),
                chat_title=title,
                chat_username=chat_username,
                reply_depth=reply_depth,
            )

            lead_fit = str(signal.get("lead_fit") or "noise")
            signal_level = _signal_level(signal)
            if lead_fit in {"noise", "not_icp"} and signal_level == "low":
                return False
            if lead_fit in {"not_icp", "market_insight"} and int(signal.get("lead_score_100", 0) or 0) < 40:
                return False
            if min_score and int(signal.get("lead_score_100", 0) or 0) < min_score:
                return False

            author_id = str(msg.sender_id) if getattr(msg, "sender_id", None) is not None else None
            seen_author = get_seen_author(author_id)
            is_duplicate = False
            if seen_author and seen_author.last_signal_at:
                last_signal_at = seen_author.last_signal_at
                if last_signal_at.tzinfo is not None:
                    last_signal_at = last_signal_at.replace(tzinfo=None)
                is_duplicate = (
                    last_signal_at >= datetime.utcnow() - timedelta(days=7)
                    and int(seen_author.signal_count_7d or 0) > 2
                )

            collected_items.append({
                "source_query": source_query,
                "segment": segment,
                "chat_id": chat_id,
                "chat_title": title,
                "chat_username": chat_username,
                "chat_url": _chat_url(chat) if chat is not None else None,
                "message_id": msg.id,
                "message_date": msg.date,
                "author_id": author_id,
                "author_name": _author_name(msg),
                "author_username": _author_username(msg),
                "message_text": text,
                "text_excerpt": text[:280],
                "matched_keywords": _matched_keywords(signal),
                "signal_score": _signal_score(signal),
                "signal_level": signal_level,
                "recommended_opener": signal.get("recommended_opener"),
                "source_type": source_type,
                "is_comment": reply_depth >= 1,
                "parent_message_id": parent_message_id,
                "root_message_id": root_message_id,
                "reply_depth": reply_depth,
                "conversation_key": conversation_key,
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
                "outreach_segment": signal.get("outreach_segment"),
                "outreach_stage": signal.get("outreach_stage"),
                "cjm_stage": signal.get("cjm_stage"),
                "outreach_angle": signal.get("outreach_angle"),
                "bridge_to_offer": signal.get("bridge_to_offer"),
                "lead_category": signal.get("lead_category"),
                "lead_score_100": signal.get("lead_score_100", 0),
                "likely_icp": signal.get("likely_icp"),
                "marketplace": signal.get("marketplace"),
                "niche": signal.get("niche"),
                "budget_hint": signal.get("budget_hint"),
                "urgency": signal.get("urgency"),
                "best_reply_draft": signal.get("best_reply_draft"),
                "next_question": signal.get("next_question"),
                "reply_tone": signal.get("reply_tone"),
                "opener_soft": signal.get("opener_soft"),
                "opener_expert": signal.get("opener_expert"),
                "opener_sales": signal.get("opener_sales"),
                "review_status": "unchecked",
                "reviewed_at": None,
                "is_actionable": signal.get("is_actionable", 0),
                "status": "new",
                "is_duplicate": is_duplicate,
            })
            return True

        for chat in await _load_configured_chats(client, source_chats):
            chat_id = getattr(chat, "id", None)
            if not chat_id or chat_id in seen_chat_ids:
                continue
            seen_chat_ids.add(chat_id)
            discovered_chats.append(chat)

        for query in chat_queries:
            if len(discovered_chats) >= max_discovered_chats:
                break
            chats = await search_public_chats(client, query, limit=per_query_chat_limit)

            for chat in chats:
                if len(discovered_chats) >= max_discovered_chats:
                    break
                chat_id = getattr(chat, "id", None)
                if not chat_id or chat_id in seen_chat_ids:
                    continue
                if not _is_relevant_chat(chat, segment, good_hints=good_hints, bad_hints=bad_hints):
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

        global_message_limit = max(6, min(12, limit_chats))
        for query in message_queries[:3]:
            try:
                global_messages = await _search_messages_global(client, query, global_message_limit, max_age_hours)
            except Exception as exc:
                logger.warning(
                    "[telegram_signals] global_message_search_failed query=%s error=%s",
                    query,
                    exc,
                )
                continue

            scanned_messages += len(global_messages)
            for msg in global_messages:
                chat = await _message_chat(client, msg)
                title = _chat_title(chat) if chat is not None else str(getattr(msg, "chat_id", "unknown"))
                chat_username = getattr(chat, "username", None) if chat is not None else None
                if await process_message(
                    msg,
                    chat=chat,
                    title=title,
                    chat_username=chat_username,
                    source_query=query,
                    source_type="keyword_search",
                ):
                    kept_signals += 1

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
                source_type = "comment" if thread_view.reply_depth >= 1 else "chat_message"
                if await process_message(
                    msg,
                    chat=chat,
                    title=title,
                    chat_username=chat_username,
                    source_query=segment,
                    source_type=source_type,
                    context_text=thread_view.context_text,
                    conversation_text=thread_view.conversation_text,
                    reply_depth=thread_view.reply_depth,
                    parent_message_id=thread_view.parent_message_id,
                    root_message_id=thread_view.root_message_id,
                    conversation_key=thread_view.conversation_key,
                ):
                    kept_signals += 1

            per_chat_search_limit = max(6, min(16, limit_messages_per_chat // 4))
            for query in message_queries:
                try:
                    search_messages = await _search_messages_in_chat(client, chat, query, per_chat_search_limit, max_age_hours)
                except Exception as exc:
                    logger.warning(
                        "[telegram_signals] chat_message_search_failed chat=%s query=%s error=%s",
                        title,
                        query,
                        exc,
                    )
                    continue
                scanned_messages += len(search_messages)
                for msg in search_messages:
                    if await process_message(
                        msg,
                        chat=chat,
                        title=title,
                        chat_username=chat_username,
                        source_query=query,
                        source_type="keyword_search_in_chat",
                    ):
                        kept_signals += 1

        save_stats = save_signals(collected_items) if collected_items else {"created": 0, "updated": 0}
        for item in collected_items:
            upsert_seen_author(item.get("author_id"))

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
