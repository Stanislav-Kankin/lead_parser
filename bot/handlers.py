import asyncio
import math
from datetime import datetime, timezone

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, FSInputFile, InlineKeyboardButton, InlineKeyboardMarkup, Message

from bot.keyboards import PAGE_SIZE, main_menu, pagination_keyboard, telegram_signals_debug_menu, telegram_signals_menu
from enrichment.domain_analyzer import analyze_domain
from enrichment.inn_client import get_company_by_domain
from scoring.hypothesis_classifier import build_hypothesis
from scoring.icp_classifier import classify_icp
from sources.domain_search import search_domains_multi
from sources.query_builder import build_queries
from storage.lead_repository import get_last_leads, save_leads
from telegram_signals.exporter import export_signals_to_xlsx
from telegram_signals.repository import (
    WORKING_LEAD_FITS,
    get_contacted_leads,
    get_signal_by_id,
    get_business_like_messages,
    get_discussion_leads,
    get_hot_leads,
    get_market_intelligence,
    get_review_leads,
    get_reviewed_leads,
    get_signals,
    get_target_leads,
    set_signal_review_status,
    set_signal_status,
)
from telegram_signals.client import get_client
from telegram_signals.service import collect_signals
from utils.domain_normalizer import normalize_domain

router = Router()
SEARCH_RESULTS_CACHE: dict[int, list[dict]] = {}
PAGE_JUMP_STATE: dict[int, dict] = {}

TG_MESSAGE_LIMIT = 3900
OUTREACH_DRAFT_CACHE: dict[int, dict] = {}


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return int(value or 0)
    except Exception:
        return default


def _build_contact_link(signal) -> str | None:
    author_username = getattr(signal, "author_username", None)
    if author_username:
        username = str(author_username).lstrip("@")
        return f"https://t.me/{username}"
    return None


def _build_message_link(signal) -> str | None:
    message_id = getattr(signal, "message_id", None)
    if not message_id:
        return None

    chat_username = getattr(signal, "chat_username", None)
    if chat_username:
        return f"https://t.me/{str(chat_username).lstrip('@')}/{message_id}"

    chat_url = getattr(signal, "chat_url", None)
    if chat_url:
        return f"{str(chat_url).rstrip('/')}/{message_id}"

    chat_id = str(getattr(signal, "chat_id", None) or "").strip()
    if not chat_id:
        return None

    internal_id = chat_id.removeprefix("-100").lstrip("-")
    if internal_id.isdigit():
        return f"https://t.me/c/{internal_id}/{message_id}"

    return None


def _build_outreach_text(signal) -> str:
    best_reply = (getattr(signal, "best_reply_draft", None) or "").strip()
    if best_reply:
        return best_reply

    name = (getattr(signal, "author_name", None) or "").strip()
    first_name = name.split()[0] if name and name.lower() not in {"-", "unknown"} else ""
    hello = f"{first_name}, добрый день!" if first_name else "Добрый день!"
    chat_title = (getattr(signal, "chat_title", None) or "чате селлеров").strip()

    raw_message = " ".join((getattr(signal, "message_text", None) or "").split())
    trigger = raw_message[:120].rsplit(" ", 1)[0] if len(raw_message) > 120 else raw_message
    trigger_line = f"увидел ваше сообщение в чате «{chat_title}»: «{trigger}»." if trigger else f"увидел ваше сообщение в чате «{chat_title}»."
    message_text = raw_message.lower()
    pain = _build_lead_summary(signal).lower()
    haystack = f"{message_text}\n{pain}"

    if any(token in haystack for token in ["возврат", "пвз", "не выкуп", "невыкуп", "срок хранения"]):
        hook = trigger_line
        bridge = (
            "По описанию это может быть не классический возврат по браку, а невыкуп/истечение срока хранения: "
            "товар доехал до ПВЗ, покупатель не забрал, и дальше маркетплейс оформляет обратную логистику продавцу."
        )
        value = (
            "Я бы сначала проверил, по каким SKU это чаще всего происходит, нет ли всплеска по отдельным регионам/ПВЗ "
            "и не вырос ли срок доставки. Из-за длинной доставки невыкупы обычно растут первыми. Если проблема в нескольких SKU, "
            "то уже имеет смысл смотреть карточку, цену и ожидания покупателя."
        )
    elif any(token in haystack for token in ["упд", "усн", "расход", "отчет реализации", "фин отчет", "документ"]):
        hook = trigger_line
        bridge = (
            "Там часто путаница из-за того, что УПД и еженедельные отчёты отражают разные срезы операций, "
            "поэтому суммы могут не совпадать один в один."
        )
        value = (
            "Я бы сверял не общую сумму сразу, а отдельно комиссии, логистику, хранение, удержания и возвраты по периодам. "
            "Так быстрее видно, где именно расход расходится и это ошибка документа, периода или логики отчета."
        )
    elif any(token in haystack for token in ["карточк", "первые продажи", "не берут", "себестоимости", "нет продаж"]):
        hook = trigger_line
        bridge = (
            "Когда товар начинает ехать только через сильную скидку, причина не всегда в цене. Часто карточка просто не набрала первые поведенческие сигналы."
        )
        value = (
            "Я бы смотрел три вещи: по каким запросам карточка реально показывается, есть ли клики без добавлений в корзину "
            "и не проваливается ли конверсия на фото/первом экране. Скидка часто маскирует не цену, а слабый первый экран."
        )
    elif any(token in haystack for token in ["возврат", "пвз", "комисси", "марж", "штраф"]):
        hook = trigger_line
        bridge = (
            "Понимаю боль: когда комиссии, возвраты и внутренняя реклама растут одновременно, сложно понять, где именно ломается экономика. "
            "По нашей практике, когда ДРР на маркетплейсе уходит за 25-30%, оптимизация внутри площадки часто уже почти не меняет итоговую маржу."
        )
        value = (
            "Я бы сначала разложил это по статьям: комиссия, логистика, хранение, возвраты, реклама и скидки. "
            "Обычно после этого видно, это проблема товара, категории или условий площадки, а не одна общая «просадка». "
            "Если бренд уже уперся в эту экономику, дальше можно смотреть direct-канал рядом с WB/Ozon, но не как замену площадки."
        )
    elif any(token in haystack for token in ["клиентск", "повторн", "прямой канал", "яндекс.кит", "яндекс кит", "свой магазин", "свой сайт", "direct"]):
        hook = trigger_line
        bridge = (
            "В прямом канале обычно три сценария: сайт есть, но трафик дорогой; непонятно, откуда брать спрос без маркетплейса; "
            "или уже пробовали, но экономика не сошлась."
        )
        value = (
            "Сначала я бы проверил повторный спрос, маржу по SKU и источник трафика. Если это сходится, сайт/Кит/Директ можно тестировать как небольшой допканал рядом с WB/Ozon, а не как большой переезд."
        )
    else:
        hook = trigger_line
        bridge = (
            "Похоже, вопрос не только технический: часто такие вещи упираются в конкретный товар, категорию или условия площадки."
        )
        value = (
            "Я бы сначала разобрал, где именно возникает проблема: в карточке, логистике, возвратах, рекламе или документах."
        )

    return (
        f"{hello}\n\n"
        f"{hook}\n\n"
        f"{bridge}\n\n"
        f"{value}\n\n"
        "Если хотите, могу набросать 2-3 гипотезы, где бы я проверял в первую очередь. Без продажи, просто по делу."
    )


def _ru_outreach_segment(value: str | None) -> str:
    mapping = {
        "mp_operations": "Операционка MP",
        "mp_accounting": "Документы/учёт MP",
        "mp_demand": "Спрос/карточка",
        "mp_unit_economics": "Экономика/маржа",
        "direct_channel_interest": "Интерес к direct/Кит",
        "vendor_search": "Ищет подрядчика",
        "mp_general_pain": "Общая боль MP",
        "ignore": "Игнор",
    }
    return mapping.get(value or "", value or "-")


def _ru_outreach_stage(value: str | None) -> str:
    mapping = {
        "awareness": "Осознание",
        "problem_aware": "Понимает проблему",
        "solution_search": "Ищет решение",
        "ignore": "Игнор",
    }
    return mapping.get(value or "", value or "-")


def _outreach_target(signal) -> str | int | None:
    username = getattr(signal, "author_username", None)
    if username:
        return f"@{str(username).lstrip('@')}"
    author_id = str(getattr(signal, "author_id", None) or "").strip()
    if author_id and author_id.lstrip("-").isdigit():
        return int(author_id)
    return None


def _outreach_target_label(signal) -> str:
    username = getattr(signal, "author_username", None)
    if username:
        return f"@{str(username).lstrip('@')}"
    author_name = (getattr(signal, "author_name", None) or "").strip()
    if author_name:
        return f"{author_name} (нет username, открыть личку нельзя)"
    return "Автор без username"


def _lead_identity(signal) -> str:
    if getattr(signal, "author_username", None):
        return f"@{str(signal.author_username).lstrip('@')}"
    if getattr(signal, "author_name", None):
        return str(signal.author_name)
    return getattr(signal, "chat_title", None) or "Без имени"


def _build_lead_summary(signal) -> str:
    pain = (getattr(signal, "pain_detected", None) or "").strip()
    icp = (getattr(signal, "icp_detected", None) or "").strip()
    bits = []
    if pain:
        pain_items = [item.strip() for item in pain.split(",") if item.strip()]
        bits.append(", ".join(pain_items[:4]))
    if icp:
        icp_items = [item.strip() for item in icp.split(",") if item.strip()]
        bits.append(", ".join(icp_items[:3]))
    if bits:
        return "; ".join(bits)
    why = (getattr(signal, "why_actionable", None) or "").strip()
    if why:
        why = why.split(';')[0].strip()
        return why[:160]
    return "Нужна ручная проверка контекста"


def _build_hot_lead_alert(signal) -> str:
    message_url = _build_message_link(signal)
    link = f'\n<a href="{escape_html(message_url)}">Открыть сообщение</a>' if message_url else ""
    return (
        "🔥 <b>Новый горячий лид</b>\n"
        f"<b>Score:</b> {getattr(signal, 'lead_score_100', 0) or 0}\n"
        f"<b>Чат:</b> {escape_html(getattr(signal, 'chat_title', None) or '-')}\n"
        f"<b>Тип:</b> {escape_html(getattr(signal, 'lead_category', None) or '-')}\n"
        f"<b>ICP:</b> {escape_html(getattr(signal, 'likely_icp', None) or '-')}\n"
        f"<b>Сообщение:</b> {escape_html(_trim_text(getattr(signal, 'text_excerpt', None) or getattr(signal, 'message_text', None) or '-', 220))}"
        f"{link}"
    )


def _recency_score(signal) -> int:
    message_date = getattr(signal, "message_date", None)
    if not message_date:
        return 0
    if getattr(message_date, "tzinfo", None) is None:
        message_date = message_date.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    age_days = max((now - message_date).days, 0)
    if age_days <= 1:
        return 5
    if age_days <= 3:
        return 4
    if age_days <= 7:
        return 3
    if age_days <= 14:
        return 2
    if age_days <= 30:
        return 1
    return 0


def _sales_priority(signal) -> tuple:
    message_date = getattr(signal, "message_date", None) or datetime.min
    return (
        _recency_score(signal),
        _safe_int(getattr(signal, "final_lead_score", 0)),
        _safe_int(getattr(signal, "pain_score", 0)),
        _safe_int(getattr(signal, "icp_score", 0)),
        _safe_int(getattr(signal, "contactability_score", 0)),
        -_safe_int(getattr(signal, "contractor_penalty", 0)),
        message_date,
    )


def _dedupe_signals_to_leads(items) -> list:
    leads = {}
    for item in items:
        key = (
            (getattr(item, "author_username", None) or "").strip().lower(),
            getattr(item, "chat_id", None),
        )
        if not key[0]:
            key = (
                (getattr(item, "author_name", None) or "").strip().lower(),
                getattr(item, "chat_id", None),
            )
        if not key[0]:
            key = (f"msg:{getattr(item, 'message_id', 0)}", getattr(item, "chat_id", None))

        current = leads.get(key)
        if current is None or _sales_priority(item) > _sales_priority(current):
            leads[key] = item

    return sorted(leads.values(), key=_sales_priority, reverse=True)


def _lead_list_header(title: str, page: int, total_pages: int, segment_label: str, total_items: int) -> str:
    return (
        f"<b>{title}</b>\n"
        f"<b>Сегмент:</b> {escape_html(segment_label)}\n"
        f"<b>Лид:</b> {page + 1}/{total_pages}\n"
        f"<b>Уникальных лидов:</b> {total_items}"
    )


def _build_lead_page(title: str, items, page: int, total_pages: int, segment_label: str) -> str:
    signal = items[page]
    return "\n\n".join([_lead_list_header(title, page, total_pages, segment_label, len(items)), format_sales_lead_card(page + 1, signal)])


def _get_sales_view_payload(view: str, segment_filter: str | None):
    segment_label = _ru_segment(segment_filter or "all")
    if view == "tg_targets":
        return "🎯 Auto-target", _dedupe_signals_to_leads(get_target_leads(segment=segment_filter, limit=None)), segment_label
    if view == "tg_review":
        return "🟡 Проверить компанию", _dedupe_signals_to_leads(get_review_leads(segment=segment_filter, limit=None)), segment_label
    if view == "tg_ok":
        return "✅ ОК лиды", _dedupe_signals_to_leads(get_reviewed_leads("ok", segment=segment_filter, limit=None)), segment_label
    if view == "tg_contacted":
        return "📬 Связались", _dedupe_signals_to_leads(get_contacted_leads(segment=segment_filter, limit=None)), segment_label
    if view == "tg_not_ok":
        return "❌ Не ОК лиды", _dedupe_signals_to_leads(get_reviewed_leads("not_ok", segment=segment_filter, limit=None)), segment_label
    return "Лиды", [], segment_label


def _view_empty_text(view: str) -> str:
    mapping = {
        "tg_targets": "Пока авто-target пустой. Рабочий путь: открыть Проверить, отметить ОК и писать из ОК.",
        "tg_review": "Пока нет свежих лидов за 96 часов. Нажми «Собрать боли WB/Ozon → сайт/Кит».",
        "tg_ok": "Пока нет ОК-лидов. Сначала отметь подходящих людей в «Проверить».",
        "tg_contacted": "Пока нет лидов, с которыми уже связались.",
        "tg_not_ok": "Пока нет лидов, которые ты отметил как не ОК.",
    }
    return mapping.get(view, "Пока данных нет.")


async def _render_sales_view(callback: CallbackQuery, view: str, page: int, segment_filter: str | None) -> None:
    title, items, segment_label = _get_sales_view_payload(view, segment_filter)
    if not items:
        await _send_or_edit(callback, _view_empty_text(view), reply_markup=telegram_signals_menu())
        return

    total_pages = len(items)
    page = max(0, min(page, total_pages - 1))
    text = _build_lead_page(title, items, page, total_pages, segment_label)
    await _send_or_edit(
        callback,
        text,
        reply_markup=sales_lead_keyboard(view, page, total_pages, items[page], extra=(segment_filter or "all")),
    )


def sales_lead_keyboard(prefix: str, page: int, total_pages: int, signal, *, extra: str | None = None):
    rows = []
    nav = []
    segment = extra or "all"
    suffix = f":{segment}"
    signal_id = getattr(signal, "id", None)

    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"{prefix}:{page - 1}{suffix}"))
    nav.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="page_info"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"{prefix}:{page + 1}{suffix}"))
    if nav:
        rows.append(nav)

    rows.append([
        InlineKeyboardButton(text="🔢 Перейти", callback_data=f"jump:{prefix}:{segment}"),
    ])

    if signal_id is not None and prefix in {"tg_targets", "tg_review", "tg_ok", "tg_not_ok"}:
        if _outreach_target(signal):
            if getattr(signal, "is_person_reachable", False) and getattr(signal, "author_username", None):
                rows.append([
                    InlineKeyboardButton(
                        text="✉️ Написать",
                        callback_data=f"draft_msg:{signal_id}:{prefix}:{page}:{segment}",
                    )
                ])
            else:
                fallback_url = getattr(signal, "chat_url", None) or _build_message_link(signal)
                if fallback_url:
                    rows.append([InlineKeyboardButton(text="Смотреть чат →", url=fallback_url)])
        if prefix == "tg_ok":
            rows.append([
                InlineKeyboardButton(
                    text="❌ Убрать из ОК",
                    callback_data=f"mark:not_ok:{signal_id}:{prefix}:{page}:{segment}",
                )
            ])
        elif prefix == "tg_not_ok":
            rows.append([
                InlineKeyboardButton(
                    text="✅ Вернуть в ОК",
                    callback_data=f"mark:ok:{signal_id}:{prefix}:{page}:{segment}",
                )
            ])
        else:
            rows.append([
                InlineKeyboardButton(
                    text="✅ ОК лид",
                    callback_data=f"mark:ok:{signal_id}:{prefix}:{page}:{segment}",
                ),
                InlineKeyboardButton(
                    text="❌ не ОК лид",
                    callback_data=f"mark:not_ok:{signal_id}:{prefix}:{page}:{segment}",
                ),
            ])

    open_message = _build_message_link(signal)
    open_contact = _build_contact_link(signal)
    action_row = []
    if open_message:
        action_row.append(InlineKeyboardButton(text="Открыть сообщение", url=open_message))
    if open_contact:
        action_row.append(InlineKeyboardButton(text="Профиль", url=open_contact))
    if action_row:
        rows.append(action_row)

    rows.append([InlineKeyboardButton(text="⬅️ В меню", callback_data="tg_signals_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _trim_text(value: str | None, limit: int = 220) -> str:
    value = (value or "").strip()
    if len(value) <= limit:
        return value or "-"
    return value[: limit - 1].rstrip() + "…"


def _excerpt_by_words(value: str | None, limit: int = 120) -> str:
    value = " ".join((value or "").split())
    if len(value) <= limit:
        return value or "-"
    cut = value[:limit].rsplit(" ", 1)[0].strip()
    return (cut or value[:limit]).rstrip(".,;:") + "…"


def _time_ago(value) -> str:
    if not value:
        return "-"
    now = datetime.now(timezone.utc)
    if getattr(value, "tzinfo", None) is None:
        value = value.replace(tzinfo=timezone.utc)
    delta = now - value
    minutes = max(0, int(delta.total_seconds() // 60))
    if minutes < 60:
        return f"{minutes} мин назад"
    hours = minutes // 60
    if hours < 24:
        return f"{hours} ч назад"
    days = hours // 24
    return f"{days} д назад"


def _signal_level_icon(score: int) -> str:
    if score >= 75:
        return "🟣"
    if score >= 50:
        return "🟡"
    return "⚪"


def _format_signal_card(signal) -> str:
    score = int(getattr(signal, "lead_score_100", 0) or 0)
    author_name = getattr(signal, "author_name", None) or "Без имени"
    author_username = getattr(signal, "author_username", None)
    username = f"@{str(author_username).lstrip('@')}" if author_username else "-"
    chat_title = getattr(signal, "chat_title", None) or "-"
    segment_label = _ru_segment(getattr(signal, "segment", None) or "all")
    outreach_stage = _ru_outreach_stage(getattr(signal, "outreach_stage", None))
    urgency = getattr(signal, "urgency", None)
    tags = [segment_label, outreach_stage]
    if urgency:
        tags.append(str(urgency))
    text = getattr(signal, "message_text", None) or getattr(signal, "text_excerpt", None) or ""
    pain = getattr(signal, "pain_detected", None) or "-"
    lines = [
        f"━━ {_signal_level_icon(score)} {score} балла ━━━━━━━━━━━━",
        "",
        f"👤 {escape_html(author_name)}  {escape_html(username)}",
        f"💬 Чат: {escape_html(chat_title)}",
        "",
        " ".join(f"[{escape_html(tag)}]" for tag in tags if tag and tag != "-"),
        "",
        f"   «{escape_html(_excerpt_by_words(text, 120))}»",
        "",
        f"📞 Боли: {escape_html(pain)}",
        f"📅 {escape_html(_time_ago(getattr(signal, 'message_date', None)))}",
    ]
    if getattr(signal, "review_status", None) == "checked":
        lines.append("<i>✓ просмотрено</i>")
    if getattr(signal, "is_duplicate", False):
        lines.append("⚠ Повторный контакт")
    return "\n".join(lines)


def _format_sales_signal_card(idx: int, signal) -> str:
    opener = _trim_text(
        getattr(signal, "best_reply_draft", None)
        or getattr(signal, "opener_expert", None)
        or getattr(signal, "opener_soft", None)
        or getattr(signal, "recommended_opener", None)
        or "-",
        260,
    )
    icp = getattr(signal, "likely_icp", None) or "-"
    marketplace = getattr(signal, "marketplace", None) or "-"
    return (
        f"<b>{idx}.</b>\n"
        f"{_format_signal_card(signal)}\n\n"
        f"ICP: <b>{escape_html(icp)}</b> · MP: <b>{escape_html(marketplace)}</b>\n"
        f"Заход: {escape_html(opener)}"
    )


def _format_dashboard(stats: dict) -> str:
    return (
        "📊 <b>Сигналы · сейчас</b>\n\n"
        f"🟣 Горячих: <b>{stats.get('hot', 0)}</b>\n"
        f"🟡 Тёплых: <b>{stats.get('warm', 0)}</b>\n"
        f"⚪ Слабых: <b>{stats.get('weak', 0)}</b>\n"
        f"✅ Обработано сегодня: <b>{stats.get('reviewed_today', 0)}</b>"
    )


async def _send_or_edit(callback: CallbackQuery, text: str, reply_markup=None) -> None:
    try:
        await callback.message.edit_text(
            text,
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=reply_markup,
        )
    except Exception:
        await callback.message.answer(
            text,
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=reply_markup,
        )


def _signal_header(title: str, page: int, total_pages: int, segment_label: str, total_items: int) -> str:
    return (
        f"<b>{title}</b>\n"
        f"<b>Сегмент:</b> {escape_html(segment_label)}\n"
        f"<b>Страница:</b> {page + 1}/{total_pages}\n"
        f"<b>Всего:</b> {total_items}"
    )


def _build_signal_page(title: str, items, page: int, total_pages: int, segment_label: str) -> str:
    lines = [_signal_header(title, page, total_pages, segment_label, len(items))]
    chunk = items[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]
    for idx, item in enumerate(chunk, start=page * PAGE_SIZE + 1):
        card = format_signal_card(idx, item)
        candidate = "\n\n".join(lines + [card])
        if len(candidate) > TG_MESSAGE_LIMIT and len(lines) > 1:
            break
        lines.append(card)
    return "\n\n".join(lines)


def _parse_segment_page(callback_data: str) -> tuple[str | None, int]:
    parts = (callback_data or "").split(":")
    if len(parts) != 3:
        return None, 0

    _, second, third = parts

    # Supported formats:
    # 1) prefix:{page}:{segment}
    # 2) prefix:{segment}:{page}
    if second.isdigit():
        page_raw, segment = second, third
    elif third.isdigit():
        segment, page_raw = second, third
    elif second == "all":
        page_raw, segment = "0", third
    else:
        page_raw, segment = second, third

    try:
        page = max(0, int(page_raw))
    except Exception:
        page = 0

    segment_filter = None if segment == "all" else segment
    return segment_filter, page


@router.message(Command("start"))
async def start(message: Message):
    await message.answer(
        "<b>Lead Parser</b>\n\n"
        "Нажми на inline-кнопку ниже или просто пришли поисковый запрос.\n"
        "Пример: производитель косметики",
        reply_markup=main_menu(),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "find_companies")
async def find_companies(callback: CallbackQuery):
    await callback.message.answer(
        "Пришли поисковый запрос.\n"
        "Лучше писать так: <b>производитель косметики</b> или <b>бренд мебели</b>.",
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "tg_signals_menu")
async def tg_signals_menu_handler(callback: CallbackQuery):
    await _send_or_edit(
        callback,
        "<b>Telegram Signal Miner</b>\n\nСобираем боли селлеров и производителей, где можно вести в прямой канал: свой сайт или Яндекс.Кит + Директ.",
        reply_markup=telegram_signals_menu(),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("tg_collect:"))
async def tg_collect(callback: CallbackQuery):
    segment = (callback.data or "").split(":", 1)[1]
    segments = (
        ["ecom_marketplace_pain", "ecom_direct_growth", "manufacturer_secondary"]
        if segment == "all"
        else [segment]
    )
    segment_ru = "WB/Ozon + сайт/Кит + производители" if segment == "all" else _ru_segment(segment)
    await callback.answer("Запускаю поиск…")
    await callback.message.answer(
        f"Запускаю поиск Telegram-сигналов: <b>{escape_html(segment_ru)}</b>\n"
        "Ищу живые вопросы, операционные боли и признаки seller/brand: комиссии, возвраты, внешний трафик, прямой канал.",
        parse_mode="HTML",
    )
    totals = {"created": 0, "updated": 0, "scanned_chats": 0, "scanned_messages": 0, "kept_signals": 0}
    try:
        for current_segment in segments:
            result = await collect_signals(current_segment)
            for key in totals:
                totals[key] += int(result.get(key, 0) or 0)
    except Exception as e:
        await callback.message.answer(
            f"Ошибка Telegram-поиска: {escape_html(str(e))}",
            parse_mode="HTML",
        )
        return

    await callback.message.answer(
        "Поиск завершён.\n"
        f"Окно свежести: <b>96 часов</b>.\n"
        f"Сырых сигналов: <b>{totals['created']}</b> новых, <b>{totals['updated']}</b> обновлено.\n"
        f"Просмотрено чатов: <b>{totals['scanned_chats']}</b>, свежих сообщений: <b>{totals['scanned_messages']}</b>.\n"
        f"В очередях сейчас: 🎯 <b>{len(_dedupe_signals_to_leads(get_target_leads(limit=None)))}</b>, "
        f"🟡 <b>{len(_dedupe_signals_to_leads(get_review_leads(limit=None)))}</b>.\n"
        f"Сырья после первичного фильтра: <b>{totals['kept_signals']}</b>.",
        parse_mode="HTML",
        reply_markup=telegram_signals_menu(),
    )
    for hot_lead in get_hot_leads(limit=3):
        await callback.message.answer(
            _build_hot_lead_alert(hot_lead),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )


@router.callback_query(F.data.startswith("tg_list:"))
async def tg_list(callback: CallbackQuery):
    segment_filter, page = _parse_segment_page(callback.data or "")
    items = get_signals(segment=segment_filter, limit=None)
    if not items:
        await _send_or_edit(callback, "Пока Telegram-сигналов нет. Сначала запусти поиск по одному из сегментов.", reply_markup=telegram_signals_menu())
        await callback.answer()
        return

    total_pages = max(1, math.ceil(len(items) / PAGE_SIZE))
    page = min(page, total_pages - 1)
    text = _build_signal_page("Все Telegram-сигналы", items, page, total_pages, _ru_segment(segment_filter or "all"))

    await _send_or_edit(
        callback,
        text,
        reply_markup=pagination_keyboard("tg_list", page, total_pages, extra=(segment_filter or "all")),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("tg_targets:"))
async def tg_targets(callback: CallbackQuery):
    segment_filter, page = _parse_segment_page(callback.data or "")
    await _render_sales_view(callback, "tg_targets", page, segment_filter)
    await callback.answer()


@router.callback_query(F.data.startswith("tg_review:"))
async def tg_review(callback: CallbackQuery):
    segment_filter, page = _parse_segment_page(callback.data or "")
    await _render_sales_view(callback, "tg_review", page, segment_filter)
    await callback.answer()


@router.callback_query(F.data.startswith("tg_ok:"))
async def tg_ok(callback: CallbackQuery):
    segment_filter, page = _parse_segment_page(callback.data or "")
    await _render_sales_view(callback, "tg_ok", page, segment_filter)
    await callback.answer()


@router.callback_query(F.data.startswith("tg_not_ok:"))
async def tg_not_ok(callback: CallbackQuery):
    segment_filter, page = _parse_segment_page(callback.data or "")
    await _render_sales_view(callback, "tg_not_ok", page, segment_filter)
    await callback.answer()


@router.callback_query(F.data.startswith("tg_contacted:"))
async def tg_contacted(callback: CallbackQuery):
    segment_filter, page = _parse_segment_page(callback.data or "")
    await _render_sales_view(callback, "tg_contacted", page, segment_filter)
    await callback.answer()


@router.callback_query(F.data.startswith("jump:"))
async def jump_to_page_prompt(callback: CallbackQuery):
    parts = (callback.data or "").split(":", 2)
    if len(parts) != 3:
        await callback.answer()
        return
    _, view, segment = parts
    PAGE_JUMP_STATE[callback.from_user.id] = {"view": view, "segment": None if segment == "all" else segment}
    await callback.answer()
    await callback.message.answer("Введи номер страницы сообщением. Например: 15. Для отмены: отмена")


@router.callback_query(F.data.startswith("mark:"))
async def mark_lead(callback: CallbackQuery):
    parts = (callback.data or "").split(":")
    if len(parts) != 6:
        await callback.answer("Не удалось обработать отметку", show_alert=True)
        return
    _, review_status, signal_id_raw, view, page_raw, segment = parts
    try:
        signal_id = int(signal_id_raw)
        page = int(page_raw)
    except Exception:
        await callback.answer("Некорректные данные", show_alert=True)
        return

    if not set_signal_review_status(signal_id, review_status):
        await callback.answer("Лид не найден", show_alert=True)
        return

    segment_filter = None if segment == "all" else segment
    await callback.answer("Статус обновлён")
    await _render_sales_view(callback, view, page, segment_filter)


@router.callback_query(F.data.startswith("draft_msg:"))
async def draft_outreach_message(callback: CallbackQuery):
    parts = (callback.data or "").split(":")
    if len(parts) != 5:
        await callback.answer("Не удалось подготовить сообщение", show_alert=True)
        return
    _, signal_id_raw, view, page_raw, segment = parts
    try:
        signal_id = int(signal_id_raw)
        page = int(page_raw)
    except Exception:
        await callback.answer("Некорректные данные", show_alert=True)
        return

    signal = get_signal_by_id(signal_id)
    if signal is None:
        await callback.answer("Лид не найден", show_alert=True)
        return

    target = _outreach_target(signal)
    if not target:
        await callback.answer("Не удалось определить автора", show_alert=True)
        return

    draft = _build_outreach_text(signal)
    OUTREACH_DRAFT_CACHE[signal_id] = {
        "text": draft,
        "target": target,
        "view": view,
        "page": page,
        "segment": segment,
    }
    profile_url = _build_contact_link(signal)
    message_url = _build_message_link(signal)
    text = (
        "<b>Черновик для ручной отправки</b>\n"
        f"<b>Кому:</b> {escape_html(_outreach_target_label(signal))}\n\n"
        f"{escape_html(draft)}"
    )
    rows = []
    if profile_url:
        rows.append([InlineKeyboardButton(text="✉️ Открыть личку", url=profile_url)])
    elif message_url:
        rows.append([InlineKeyboardButton(text="Открыть сообщение", url=message_url)])
    rows.append([
        InlineKeyboardButton(text="✅ Отправил", callback_data=f"outreach_done:{signal_id}:{view}:{page}:{segment}"),
        InlineKeyboardButton(text="⏭️ Пропустить", callback_data=f"outreach_skip:{signal_id}:{view}:{page}:{segment}"),
    ])
    rows.append([InlineKeyboardButton(text="⬅️ Назад к лиду", callback_data=f"{view}:{page}:{segment}")])
    keyboard = InlineKeyboardMarkup(inline_keyboard=rows)
    await _send_or_edit(callback, text, reply_markup=keyboard)
    await callback.answer()


@router.callback_query(F.data.startswith("outreach_done:"))
async def outreach_done(callback: CallbackQuery):
    parts = (callback.data or "").split(":")
    if len(parts) != 5:
        await callback.answer("Не удалось обработать действие", show_alert=True)
        return
    _, signal_id_raw, view, page_raw, segment = parts
    try:
        signal_id = int(signal_id_raw)
        page = int(page_raw)
    except Exception:
        await callback.answer("Некорректные данные", show_alert=True)
        return

    if not set_signal_status(signal_id, "contacted"):
        await callback.answer("Лид не найден", show_alert=True)
        return

    segment_filter = None if segment == "all" else segment
    await callback.answer("Пометил: связались")
    await _render_sales_view(callback, view, page, segment_filter)


@router.callback_query(F.data.startswith("outreach_skip:"))
async def outreach_skip(callback: CallbackQuery):
    parts = (callback.data or "").split(":")
    if len(parts) != 5:
        await callback.answer()
        return
    _, _signal_id_raw, view, page_raw, segment = parts
    try:
        page = int(page_raw)
    except Exception:
        page = 0
    segment_filter = None if segment == "all" else segment
    await callback.answer("Пропустил")
    await _render_sales_view(callback, view, page, segment_filter)


@router.callback_query(F.data.startswith("send_msg:"))
async def send_outreach_message(callback: CallbackQuery):
    signal_id_raw = (callback.data or "").split(":", 1)[1]
    try:
        signal_id = int(signal_id_raw)
    except Exception:
        await callback.answer("Некорректные данные", show_alert=True)
        return

    draft = OUTREACH_DRAFT_CACHE.get(signal_id)
    if not draft:
        await callback.answer("Черновик устарел, открой лид заново", show_alert=True)
        return

    target = draft["target"]
    text = draft["text"]
    await callback.answer("Отправляю…")

    client = get_client()
    try:
        await client.connect()
        if not await client.is_user_authorized():
            await callback.message.answer("User-сессия Telegram не авторизована. Нужно заново создать lead_signal_session.")
            return
        await client.send_message(target, text)
    except Exception as exc:
        await callback.message.answer(
            f"Не удалось отправить сообщение {escape_html(str(target))}: {escape_html(str(exc))}",
            parse_mode="HTML",
        )
        return
    finally:
        await client.disconnect()

    view = draft.get("view") or "tg_review"
    page = int(draft.get("page") or 0)
    segment = draft.get("segment") or "all"
    segment_filter = None if segment == "all" else segment
    await callback.message.answer(
        f"Сообщение отправлено: <b>{escape_html(str(target))}</b>",
        parse_mode="HTML",
    )
    await _render_sales_view(callback, view, page, segment_filter)


@router.callback_query(F.data.startswith("tg_actionable:"))
async def tg_actionable(callback: CallbackQuery):
    segment_filter, page = _parse_segment_page(callback.data or "")
    items = get_signals(segment=segment_filter, limit=None, lead_fit_in=WORKING_LEAD_FITS)
    if not items:
        await _send_or_edit(callback, "Пока актуальных лидов нет. Сначала запусти поиск по сегменту.", reply_markup=telegram_signals_menu())
        await callback.answer()
        return

    total_pages = max(1, math.ceil(len(items) / PAGE_SIZE))
    page = min(page, total_pages - 1)
    text = _build_signal_page("Актуальные лиды", items, page, total_pages, _ru_segment(segment_filter or "all"))

    await _send_or_edit(
        callback,
        text,
        reply_markup=pagination_keyboard("tg_actionable", page, total_pages, extra=(segment_filter or "all")),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("tg_discussions:"))
async def tg_discussions(callback: CallbackQuery):
    segment_filter, page = _parse_segment_page(callback.data or "")
    items = get_discussion_leads(segment=segment_filter, limit=None)
    if not items:
        await _send_or_edit(callback, "Пока обсуждений с болью нет. Сначала запусти поиск по сегменту.", reply_markup=telegram_signals_menu())
        await callback.answer()
        return

    total_pages = max(1, math.ceil(len(items) / PAGE_SIZE))
    page = min(page, total_pages - 1)
    text = _build_signal_page("Обсуждения с болью", items, page, total_pages, _ru_segment(segment_filter or "all"))

    await _send_or_edit(
        callback,
        text,
        reply_markup=pagination_keyboard("tg_discussions", page, total_pages, extra=(segment_filter or "all")),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("tg_business:"))
async def tg_business(callback: CallbackQuery):
    segment_filter, page = _parse_segment_page(callback.data or "")
    items = get_business_like_messages(segment=segment_filter, limit=None)
    if not items:
        await _send_or_edit(callback, "Пока сообщений от business-like авторов нет. Сначала запусти поиск по сегменту.", reply_markup=telegram_signals_menu())
        await callback.answer()
        return

    total_pages = max(1, math.ceil(len(items) / PAGE_SIZE))
    page = min(page, total_pages - 1)
    text = _build_signal_page("Похожи на бизнес", items, page, total_pages, _ru_segment(segment_filter or "all"))

    await _send_or_edit(
        callback,
        text,
        reply_markup=pagination_keyboard("tg_business", page, total_pages, extra=(segment_filter or "all")),
    )
    await callback.answer()



@router.callback_query(F.data.startswith("tg_market:"))
async def tg_market(callback: CallbackQuery):
    segment_filter, page = _parse_segment_page(callback.data or "")
    items = get_market_intelligence(segment=segment_filter, limit=None)
    if not items:
        await _send_or_edit(callback, "Пока рыночных гипотез нет. Сначала запусти поиск по сегменту.", reply_markup=telegram_signals_menu())
        await callback.answer()
        return

    total_pages = max(1, math.ceil(len(items) / PAGE_SIZE))
    page = min(page, total_pages - 1)
    text = _build_signal_page("Рынок / гипотезы", items, page, total_pages, _ru_segment(segment_filter or "all"))

    await _send_or_edit(
        callback,
        text,
        reply_markup=pagination_keyboard("tg_market", page, total_pages, extra=(segment_filter or "all")),
    )
    await callback.answer()

@router.callback_query(F.data.startswith("tg_export:"))
async def tg_export(callback: CallbackQuery):
    kind = (callback.data or "").split(":", 1)[1]
    await callback.answer("Готовлю файл…")
    file_path = export_signals_to_xlsx(kind)
    await callback.message.answer_document(
        FSInputFile(str(file_path)),
        caption=f"Экспорт Telegram signals: {escape_html(kind)}",
        parse_mode="HTML",
    )


@router.callback_query(F.data == "page_info")
async def page_info(callback: CallbackQuery):
    await callback.answer()


@router.callback_query(F.data.startswith("last_leads:"))
async def last_leads(callback: CallbackQuery):
    page = _parse_page(callback.data)
    leads = get_last_leads(limit=30, only_with_contacts=True)
    if not leads:
        await callback.message.answer("Пока нет лидов с контактами.")
        await callback.answer()
        return

    total_pages = max(1, math.ceil(len(leads) / PAGE_SIZE))
    page = min(page, total_pages - 1)
    chunk = leads[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]

    text = [f"<b>Последние лиды</b>\n<b>Страница:</b> {page + 1}/{total_pages}\n"]
    for idx, lead in enumerate(chunk, start=page * PAGE_SIZE + 1):
        text.append(
            format_lead_card(
                idx,
                {
                    "domain": lead.domain,
                    "company_name": lead.company_name,
                    "title": lead.title,
                    "is_icp": lead.is_icp,
                    "lead_type_ru": _ru_lead_type(lead.lead_type),
                    "priority_ru": _ru_priority(lead.priority),
                    "icp_reason": lead.icp_reason,
                    "hypothesis": lead.hypothesis,
                    "opener": lead.opener,
                    "company_inn": lead.company_inn,
                    "company_ogrn": lead.company_ogrn,
                    "company_legal_name": lead.company_legal_name,
                    "legal_form": lead.legal_form,
                    "inn_source": lead.inn_source,
                    "company_email": lead.company_email,
                    "company_phone": lead.company_phone,
                    "contacts_source": lead.contacts_source,
                    "contact_confidence": lead.contact_confidence,
                },
            )
        )

    await callback.message.answer(
        "\n\n".join(text),
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=pagination_keyboard("last_leads", page, total_pages),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("search_page:"))
async def search_page(callback: CallbackQuery):
    page = _parse_page(callback.data)
    results = SEARCH_RESULTS_CACHE.get(callback.message.message_id if callback.message else 0)
    if not results:
        await callback.answer("Кэш поиска уже устарел.", show_alert=True)
        return

    total_pages = max(1, math.ceil(len(results) / PAGE_SIZE))
    page = min(page, total_pages - 1)
    chunk = results[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]

    text = [f"<b>Найденные компании</b>\n<b>Страница:</b> {page + 1}/{total_pages}\n"]
    for idx, lead in enumerate(chunk, start=page * PAGE_SIZE + 1):
        text.append(format_lead_card(idx, lead))

    await callback.message.answer(
        "\n\n".join(text),
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=pagination_keyboard("search_page", page, total_pages),
    )
    await callback.answer()


@router.message()
async def handle_page_jump(message: Message):
    user_id = message.from_user.id if message.from_user else 0
    state = PAGE_JUMP_STATE.get(user_id)
    if not state:
        return

    text_value = (message.text or "").strip()
    if text_value.lower() in {"отмена", "cancel", "/cancel"}:
        PAGE_JUMP_STATE.pop(user_id, None)
        await message.answer("Переход по странице отменён.")
        return

    if not text_value.isdigit():
        await message.answer("Нужен номер страницы цифрами. Например: 12. Для отмены: отмена")
        return

    page = max(0, int(text_value) - 1)
    callback_stub = type("CallbackStub", (), {"message": message})()
    await _render_sales_view(callback_stub, state["view"], page, state.get("segment"))
    PAGE_JUMP_STATE.pop(user_id, None)


@router.message()
async def handle_query(message: Message):
    query = (message.text or "").strip()
    if not query:
        await message.answer("Пришли текстовый поисковый запрос.")
        return

    await message.answer("Ищу компании...")
    queries = build_queries(query)

    try:
        raw_results = await search_domains_multi(queries=queries, per_query_limit=10, total_limit=25)
    except asyncio.TimeoutError:
        await message.answer("Поиск завис по таймауту. Попробуй ещё раз или упрости запрос.")
        return
    except Exception as e:
        await message.answer(f"Ошибка поиска: {e}")
        return

    if not raw_results:
        await message.answer("Ничего не найдено. Попробуй другой запрос.")
        return

    leads_to_save = []
    display_items = []

    for item in raw_results:
        domain = normalize_domain(item.get("domain"))
        if not domain:
            continue

        company_name = item.get("company_name")
        analysis = await analyze_domain(domain)
        icp = classify_icp(
            title=analysis.get("title"),
            domain=domain,
            company_name=company_name,
            description=analysis.get("description"),
            h1=analysis.get("h1"),
            text=analysis.get("text"),
        )
        hypothesis, opener = build_hypothesis(
            title=analysis.get("title"),
            is_icp=icp["is_icp"],
            company_name=company_name,
            text=analysis.get("text"),
        )

        helper_data = None
        if icp["is_icp"] or icp["priority"] in {"high", "medium"}:
            helper_data = await get_company_by_domain(domain)

        company_email = analysis.get("email") or _pick_value(helper_data, "email")
        company_phone = analysis.get("phone") or _pick_value(helper_data, "phone")
        company_inn = analysis.get("company_inn")
        company_ogrn = analysis.get("company_ogrn")
        company_legal_name = analysis.get("company_legal_name")
        legal_form = analysis.get("legal_form")
        inn_source = analysis.get("inn_source")
        has_contacts = bool(company_email or company_phone)

        if not has_contacts:
            continue

        contacts_source = _build_contacts_source(helper_data, analysis)
        contact_confidence = _get_contact_confidence(company_inn, company_legal_name, company_email, company_phone)

        lead = {
            "query": query,
            "company_name": company_name or analysis.get("title") or domain,
            "domain": domain,
            "source": item.get("source", "ddgs"),
            "title": analysis.get("title"),
            "is_icp": icp["is_icp"],
            "icp_reason": icp["icp_reason"],
            "lead_type": icp["lead_type"],
            "lead_type_ru": icp["lead_type_ru"],
            "priority": icp["priority"],
            "priority_ru": icp["priority_ru"],
            "hypothesis": hypothesis,
            "opener": opener,
            "company_inn": company_inn,
            "company_ogrn": company_ogrn,
            "company_legal_name": company_legal_name,
            "legal_form": legal_form,
            "inn_source": inn_source,
            "company_email": company_email,
            "company_phone": company_phone,
            "employees": _pick_value(helper_data, "employees"),
            "contacts_source": contacts_source,
            "contact_confidence": contact_confidence,
            "has_contacts": has_contacts,
            "sales_ready": bool(icp["is_icp"] and has_contacts),
            "last_enriched_at": datetime.utcnow() if helper_data else None,
        }

        leads_to_save.append(lead)
        display_items.append(lead)

    display_items.sort(
        key=lambda x: (
            x.get("sales_ready", False),
            x.get("is_icp", False),
            x.get("contact_confidence") == "high",
            x.get("priority") == "high",
        ),
        reverse=True,
    )

    save_stats = save_leads(leads_to_save) if leads_to_save else {"created": 0, "updated": 0}

    total_pages = max(1, math.ceil(len(display_items) / PAGE_SIZE)) if display_items else 1
    first_page_items = display_items[:PAGE_SIZE]

    if not first_page_items:
        await message.answer(
            "Ничего рабочего не найдено.\n"
            "Сейчас в выдаче остаются только компании, у которых нашлись контакты."
        )
        return

    response_lines = [f"<b>Найденные компании</b>\n<b>Страница:</b> 1/{total_pages}\n"]
    for idx, lead in enumerate(first_page_items, start=1):
        response_lines.append(format_lead_card(idx, lead))

    icp_count = sum(1 for item in display_items if item.get("is_icp"))
    with_inn = sum(1 for item in display_items if item.get("company_inn"))
    response_lines.append(
        "\n<b>Итог:</b>\n"
        f"<b>Всего найдено доменов:</b> {len(raw_results)}\n"
        f"<b>Показано с контактами:</b> {len(display_items)}\n"
        f"<b>ICP среди показанных:</b> {icp_count}\n"
        f"<b>С найденным ИНН:</b> {with_inn}\n"
        f"<b>Создано новых лидов:</b> {save_stats['created']}\n"
        f"<b>Обновлено существующих:</b> {save_stats['updated']}"
    )

    sent = await message.answer(
        "\n\n".join(response_lines),
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=pagination_keyboard("search_page", 0, total_pages),
    )
    SEARCH_RESULTS_CACHE[sent.message_id] = display_items


def format_lead_card(idx: int, lead: dict) -> str:
    return (
        f"<b>{idx}. {escape_html(lead.get('domain') or '-')}</b>\n"
        f"<b>Название:</b> {escape_html(_short(lead.get('company_name') or lead.get('title') or '-'))}\n"
        f"<b>ICP:</b> {'Да' if lead.get('is_icp') else 'Нет'}\n"
        f"<b>Тип:</b> {escape_html(lead.get('lead_type_ru') or _ru_lead_type(lead.get('lead_type')))}\n"
        f"<b>Приоритет:</b> {escape_html(lead.get('priority_ru') or _ru_priority(lead.get('priority')))}\n\n"
        f"<b>Контакты</b>\n"
        f"<b>Email:</b> {escape_html(lead.get('company_email') or '-')}\n"
        f"<b>Телефон:</b> {escape_html(lead.get('company_phone') or '-')}\n"
        f"<b>Источник контакта:</b> {escape_html(lead.get('contacts_source') or '-')}\n"
        f"<b>Надёжность контакта:</b> {escape_html(_ru_confidence(lead.get('contact_confidence')))}\n\n"
        f"<b>Реквизиты</b>\n"
        f"<b>ИНН:</b> {escape_html(lead.get('company_inn') or '-')}\n"
        f"<b>ОГРН:</b> {escape_html(lead.get('company_ogrn') or '-')}\n"
        f"<b>Юр. лицо:</b> {escape_html(_short(lead.get('company_legal_name') or '-', 120))}\n"
        f"<b>Форма:</b> {escape_html(lead.get('legal_form') or '-')}\n"
        f"<b>Источник ИНН:</b> {escape_html(_ru_inn_source(lead.get('inn_source')))}\n\n"
        f"<b>Гипотеза</b>\n"
        f"<b>Гипотеза:</b> {escape_html(lead.get('hypothesis') or '-')}\n"
        f"<b>Заход:</b> {escape_html(_short(lead.get('opener') or '-', 220))}\n\n"
        f"<b>Сигналы</b>\n"
        f"{escape_html(_human_reason(lead.get('icp_reason') or '-'))}"
    )


def format_sales_lead_card(idx: int, signal) -> str:
    return _format_sales_signal_card(idx, signal)


def _format_sales_lead_card_legacy(idx: int, signal) -> str:
    username = _lead_identity(signal)
    pain_hypothesis = _build_lead_summary(signal)
    context = _trim_text(getattr(signal, "text_excerpt", None) or getattr(signal, "message_text", None) or "-", 280)
    opener = _trim_text(getattr(signal, "recommended_opener", None) or "-", 220)
    chat_name = getattr(signal, "chat_title", None) or "-"
    username_raw = getattr(signal, "author_username", None)
    username_to_write = f"@{str(username_raw).lstrip('@')}" if username_raw else (getattr(signal, "author_name", None) or "-")

    profile_url = _build_contact_link(signal)
    chat_url = getattr(signal, "chat_url", None)
    message_url = _build_message_link(signal)

    message_date = getattr(signal, "message_date", None)
    actual_label = "-"
    if message_date:
        actual_label = message_date.strftime("%d.%m.%Y %H:%M")

    who_line = escape_html(username_to_write)
    if profile_url:
        who_line = f'<a href="{escape_html(profile_url)}">{escape_html(username_to_write)}</a>'

    chat_line = escape_html(chat_name)
    if message_url:
        chat_line = f'<a href="{escape_html(message_url)}">{escape_html(chat_name)}</a>'
    elif chat_url:
        chat_line = f'<a href="{escape_html(chat_url)}">{escape_html(chat_name)}</a>'

    profile_line = who_line if profile_url else escape_html(username_to_write)
    duplicate_line = "\n⚠ Повторный контакт" if getattr(signal, "is_duplicate", False) else ""

    return (
        f"<b>{idx}. {escape_html(username)}</b>\n"
        f"<b>Score:</b> {getattr(signal, 'lead_score_100', 0) or 0} | <b>Боль:</b> {escape_html(getattr(signal, 'lead_category', None) or '-')}\n"
        f"<b>ICP:</b> {escape_html(getattr(signal, 'likely_icp', None) or '-')} | <b>MP:</b> {escape_html(getattr(signal, 'marketplace', None) or '-')}\n"
        f"<b>Кому писать:</b> {who_line}\n"
        f"<b>Профиль/username:</b> {profile_line}\n"
        f"<b>Чат:</b> {chat_line}\n"
        f"<b>Актуальность:</b> {escape_html(actual_label)}\n"
        f"<b>Сценарий:</b> {escape_html(_ru_outreach_segment(getattr(signal, 'outreach_segment', None)))} / {escape_html(_ru_outreach_stage(getattr(signal, 'outreach_stage', None)))}\n"
        f"<b>Сообщение:</b> {escape_html(context)}\n"
        f"<b>Контекст:</b> {escape_html(_trim_text(getattr(signal, 'why_actionable', None) or getattr(signal, 'conversation_type', None) or '-', 160))}\n"
        f"<b>Гипотеза боли:</b> {escape_html(pain_hypothesis)}\n"
        f"<b>Opener:</b> {escape_html(opener)}"
        f"{duplicate_line}"
    )


def format_signal_card(idx: int, signal) -> str:
    return f"<b>{idx}.</b>\n{_format_signal_card(signal)}"


def _format_signal_card_legacy(idx: int, signal) -> str:
    title = escape_html(signal.chat_title or "-")
    primary_score = getattr(signal, "final_lead_score", None) or signal.signal_score or 0
    message_type = getattr(signal, "message_type", None) or "-"
    conversation_type = getattr(signal, "conversation_type", None) or "-"
    author_type = getattr(signal, "author_type_guess", None) or "-"
    lead_fit = getattr(signal, "lead_fit", None) or "-"
    next_step = getattr(signal, "next_step", None) or "-"
    why = _trim_text(getattr(signal, "why_actionable", None) or "-", 160)
    company = getattr(signal, "company_hint", None) or "-"
    website = getattr(signal, "website_hint", None)
    if website:
        company = f"{company} | {website}"

    return (
        f"<b>{idx}. {title}</b>\n"
        f"<b>Сегмент:</b> {escape_html(_ru_segment(signal.segment))}\n"
        f"<b>Lead fit:</b> {escape_html(_ru_lead_fit(lead_fit))} | <b>Действие:</b> {escape_html(_ru_next_step(next_step))}\n"
        f"<b>Score 100:</b> {getattr(signal, 'lead_score_100', 0) or 0} | <b>Боль:</b> {escape_html(getattr(signal, 'lead_category', None) or '-')}\n"
        f"<b>ICP:</b> {escape_html(getattr(signal, 'likely_icp', None) or '-')} | <b>MP:</b> {escape_html(getattr(signal, 'marketplace', None) or '-')}\n"
        f"<b>Тип:</b> {escape_html(message_type)} / {escape_html(conversation_type)}\n"
        f"<b>Автор-профиль:</b> {escape_html(author_type)}\n"
        f"<b>Уровень:</b> {escape_html(_ru_signal_level(signal.signal_level))}\n"
        f"<b>Lead score:</b> {primary_score} | <b>Signal score:</b> {signal.signal_score}\n"
        f"<b>Контакт:</b> {escape_html(getattr(signal, 'contact_hint', None) or signal.author_username or '-') }\n"
        f"<b>Компания/сайт:</b> {escape_html(company)}\n"
        f"<b>Сценарий:</b> {escape_html(_ru_outreach_segment(getattr(signal, 'outreach_segment', None)))} / {escape_html(_ru_outreach_stage(getattr(signal, 'outreach_stage', None)))}\n"
        f"<b>Почему в выдаче:</b> {escape_html(why)}\n"
        f"<b>Фрагмент:</b> {escape_html(_trim_text(signal.text_excerpt or '-', 240))}\n"
        f"<b>Заход:</b> {escape_html(_trim_text(signal.recommended_opener or '-', 180))}\n"
        f"<b>Username:</b> {escape_html(str(getattr(signal, 'author_username', None) or '-'))}\n"
        f"<b>Ссылка на чат:</b> {escape_html(signal.chat_url or '-')}"
    )


def _pick_value(data: dict | None, key: str) -> str | None:
    if not data:
        return None
    value = data.get(key)
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def _build_contacts_source(helper_data: dict | None, analysis: dict) -> str:
    has_site = bool(analysis.get("email") or analysis.get("phone"))
    has_helper = bool(helper_data and (helper_data.get("email") or helper_data.get("phone")))

    if has_site and has_helper:
        return "site + helper_api"
    if has_site:
        return "site"
    if has_helper:
        strategy = helper_data.get("lookup_strategy")
        if strategy == "root_domain":
            return "helper_api (root domain)"
        return "helper_api"
    return "-"


def _get_contact_confidence(inn: str | None, legal_name: str | None, email: str | None, phone: str | None) -> str:
    if inn or legal_name:
        return "high"
    if email and phone:
        return "medium"
    if email or phone:
        return "low"
    return "low"


def _parse_page(payload: str | None) -> int:
    try:
        return max(0, int((payload or "").split(":", 1)[1]))
    except Exception:
        return 0


def _short(value: str, limit: int = 140) -> str:
    return value if len(value) <= limit else value[: limit - 1] + "…"


def _ru_lead_type(value: str | None) -> str:
    mapping = {
        "manufacturer_or_b2b": "Производитель / B2B",
        "possible_icp": "Потенциальный ICP",
        "low_relevance": "Низкая релевантность",
    }
    return mapping.get(value or "", value or "-")


def _ru_priority(value: str | None) -> str:
    mapping = {"high": "Высокий", "medium": "Средний", "low": "Низкий"}
    return mapping.get(value or "", value or "-")


def _ru_confidence(value: str | None) -> str:
    mapping = {"high": "Высокая", "medium": "Средняя", "low": "Низкая"}
    return mapping.get(value or "", value or "-")


def _ru_inn_source(value: str | None) -> str:
    mapping = {"site_requisites": "Сайт / реквизиты"}
    return mapping.get(value or "", value or "-")


def _ru_segment(value: str | None) -> str:
    mapping = {
        "all": "Все",
        "ecom_marketplace_pain": "WB / Ozon боль",
        "ecom_direct_growth": "Свой сайт / Кит / Direct",
        "manufacturer_secondary": "Производители",
    }
    return mapping.get(value or "", value or "-")


def _ru_signal_level(value: str | None) -> str:
    mapping = {"high": "Высокий", "medium": "Средний", "low": "Низкий"}
    return mapping.get(value or "", value or "-")


def _ru_lead_fit(value: str | None) -> str:
    mapping = {
        "target": "Живая боль",
        "review": "На проверку",
        "hot_outreach": "Hot: писать",
        "warm_reply": "Warm: ответ через пользу",
        "nurture": "Nurture",
        "market_insight": "Инсайт рынка",
        "not_icp": "Не ICP",
        "contractor": "Подрядчик",
        "noise": "Шум/рынок",
    }
    return mapping.get(value or "", value or "-")


def _ru_next_step(value: str | None) -> str:
    mapping = {
        "outreach_now": "Писать сейчас",
        "research_company": "Доресерчить компанию",
        "manual_review": "Ручная проверка",
        "ignore": "Игнорировать",
    }
    return mapping.get(value or "", value or "-")


def _human_reason(reason: str) -> str:
    if "positive:" not in reason and "negative:" not in reason:
        return reason
    positive = "-"
    negative = "-"
    try:
        if "positive:" in reason:
            positive = reason.split("positive:", 1)[1].split("|", 1)[0].replace(";", ",").replace("negative:", "").strip()
        if "negative:" in reason:
            negative = reason.split("negative:", 1)[1].strip()
    except Exception:
        return reason
    return f"+ {positive or '-'}\\n- {negative or '-'}"


def escape_html(value: str) -> str:
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
