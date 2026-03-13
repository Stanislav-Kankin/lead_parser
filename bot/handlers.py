import asyncio
import html
import math
from datetime import datetime

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, FSInputFile, Message

from bot.keyboards import PAGE_SIZE, main_menu, pagination_keyboard, telegram_signals_menu
from enrichment.domain_analyzer import analyze_domain
from enrichment.inn_client import get_company_by_domain
from scoring.hypothesis_classifier import build_hypothesis
from scoring.icp_classifier import classify_icp
from sources.domain_search import search_domains_multi
from sources.query_builder import build_queries
from storage.lead_repository import get_last_leads, save_leads
from telegram_signals.exporter import build_signals_export
from telegram_signals.repository import get_signals
from telegram_signals.service import collect_signals
from utils.domain_normalizer import normalize_domain

router = Router()
SEARCH_RESULTS_CACHE: dict[int, list[dict]] = {}


TG_MESSAGE_LIMIT = 3900
TEXT_EXCERPT_LIMIT = 280
OPENER_LIMIT = 220


def _trim_text(value: str | None, limit: int) -> str:
    value = (value or "").strip()
    if len(value) <= limit:
        return value or "-"
    return value[: limit - 1].rstrip() + "…"


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


def _build_signal_page(items, page: int, total_pages: int, segment_label: str, actionable: bool = False) -> str:
    title = "Актуальные лиды из Telegram" if actionable else "Telegram-сигналы"
    counter_label = "Всего лидов" if actionable else "Всего сигналов"
    lines = [
        f"<b>{title}</b>\n"
        f"<b>Сегмент:</b> {escape_html(segment_label)}\n"
        f"<b>Страница:</b> {page + 1}/{total_pages}\n"
        f"<b>{counter_label}:</b> {len(items)}"
    ]
    chunk = items[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]
    for idx, item in enumerate(chunk, start=page * PAGE_SIZE + 1):
        card = format_signal_card(idx, item)
        candidate = "\n\n".join(lines + [card])
        if len(candidate) > TG_MESSAGE_LIMIT and len(lines) > 1:
            break
        lines.append(card)
    return "\n\n".join(lines)


RU_SEGMENTS = {
    "ecom_marketplace_pain": "WB / Ozon боль",
    "ecom_direct_growth": "Свой сайт / Direct",
    "manufacturer_secondary": "Производители",
    "all": "Все",
}


def escape_html(value: str | None) -> str:
    return html.escape(value or "-")


def _ru_segment(segment: str) -> str:
    return RU_SEGMENTS.get(segment, segment)


def _parse_page(value: str | None) -> int:
    try:
        return max(0, int((value or "").split(":")[-1]))
    except ValueError:
        return 0


def _ru_lead_type(value: str | None) -> str:
    mapping = {
        "manufacturer": "Производитель",
        "brand": "Бренд",
        "ecommerce": "E-commerce",
        "unknown": "Не определён",
    }
    return mapping.get(value or "", value or "-")


def _ru_priority(value: str | None) -> str:
    mapping = {"high": "Высокий", "medium": "Средний", "low": "Низкий"}
    return mapping.get(value or "", value or "-")


def _ru_signal_level(value: str | None) -> str:
    mapping = {"high": "Высокий", "medium": "Средний", "low": "Низкий"}
    return mapping.get(value or "", value or "-")


def _ru_signal_type(value: str | None) -> str:
    mapping = {
        "pain": "Боль / экономика",
        "need_contractor": "Запрос подрядчика",
        "direct_growth": "Рост direct / сайта",
        "brand_signal": "Бренд / производитель",
        "service_ad": "Самореклама услуг",
        "vacancy": "Вакансия",
        "noise": "Шум",
    }
    return mapping.get(value or "", value or "-")


def _pick_value(data: dict | None, key: str):
    if not data:
        return None
    value = data.get(key)
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
    return value or None


def _build_contacts_source(helper_data: dict | None, analysis: dict | None) -> str | None:
    parts: list[str] = []
    if analysis:
        if analysis.get("email"):
            parts.append("site_email")
        if analysis.get("phone"):
            parts.append("site_phone")
    if helper_data:
        if helper_data.get("email"):
            parts.append("helper_email")
        if helper_data.get("phone"):
            parts.append("helper_phone")
    return ", ".join(parts) if parts else None


def _get_contact_confidence(company_inn: str | None, company_legal_name: str | None, company_email: str | None, company_phone: str | None) -> str:
    if company_inn or company_legal_name:
        return "high"
    if company_email and company_phone:
        return "medium"
    if company_email or company_phone:
        return "low"
    return "low"


def format_signal_card(idx: int, item) -> str:
    dt = item.message_date.strftime("%Y-%m-%d %H:%M") if item.message_date else "-"
    contact_block = []
    if item.contact_hint:
        contact_block.append(f"<b>Контакт:</b> {escape_html(item.contact_hint)}")
    if item.company_hint:
        contact_block.append(f"<b>Компания:</b> {escape_html(item.company_hint)}")
    if item.website_hint:
        contact_block.append(f"<b>Сайт:</b> {escape_html(item.website_hint)}")
    contact_text = "\n".join(contact_block)
    if contact_text:
        contact_text += "\n"

    return (
        f"<b>{idx}. {escape_html(item.chat_title)}</b>\n"
        f"<b>Сегмент:</b> {escape_html(_ru_segment(item.segment or '-'))}\n"
        f"<b>Тип:</b> {escape_html(_ru_signal_type(getattr(item, 'message_type', None)))}\n"
        f"<b>Actionable:</b> {'Да' if getattr(item, 'is_actionable', False) else 'Нет'}\n"
        f"<b>Уровень:</b> {escape_html(_ru_signal_level(item.signal_level))}\n"
        f"<b>Счёт:</b> {item.signal_score}\n"
        f"<b>ICP/Pain/Intent/Contact:</b> {getattr(item, 'icp_score', 0)}/{getattr(item, 'pain_score', 0)}/{getattr(item, 'intent_score', 0)}/{getattr(item, 'contactability_score', 0)}\n"
        f"<b>Чат:</b> {escape_html(item.chat_username or '-')}\n"
        f"<b>Автор:</b> {escape_html(item.author_username or '-')}\n"
        f"<b>Дата:</b> {dt}\n"
        f"<b>Совпадения:</b> {escape_html(item.matched_keywords or '-')}\n"
        f"{contact_text}"
        f"<b>Фрагмент:</b> {escape_html(_trim_text(item.text_excerpt, TEXT_EXCERPT_LIMIT))}\n"
        f"<b>Заход:</b> {escape_html(_trim_text(item.recommended_opener, OPENER_LIMIT))}\n"
        f"<b>Ссылка на чат:</b> {escape_html(item.chat_url or '-')}"
    )


def format_lead_card(idx: int, item: dict) -> str:
    contact_parts = []
    if item.get("company_email"):
        contact_parts.append(f"email: {item['company_email']}")
    if item.get("company_phone"):
        contact_parts.append(f"phone: {item['company_phone']}")
    contacts = ", ".join(contact_parts) if contact_parts else "-"

    legal_parts = []
    if item.get("company_legal_name"):
        legal_parts.append(item["company_legal_name"])
    if item.get("company_inn"):
        legal_parts.append(f"ИНН {item['company_inn']}")
    if item.get("company_ogrn"):
        legal_parts.append(f"ОГРН {item['company_ogrn']}")
    legal_info = " | ".join(legal_parts) if legal_parts else "-"

    return (
        f"<b>{idx}. {escape_html(item.get('company_name') or item.get('domain') or '-')}</b>\n"
        f"<b>Домен:</b> {escape_html(item.get('domain') or '-')}\n"
        f"<b>Title:</b> {escape_html(item.get('title') or '-')}\n"
        f"<b>ICP:</b> {'Да' if item.get('is_icp') else 'Нет'} ({escape_html(item.get('priority_ru') or '-')})\n"
        f"<b>Тип:</b> {escape_html(item.get('lead_type_ru') or '-')}\n"
        f"<b>Причина:</b> {escape_html(item.get('icp_reason') or '-')}\n"
        f"<b>Гипотеза:</b> {escape_html(item.get('hypothesis') or '-')}\n"
        f"<b>Заход:</b> {escape_html(item.get('opener') or '-')}\n"
        f"<b>Компания:</b> {escape_html(legal_info)}\n"
        f"<b>Контакты:</b> {escape_html(contacts)}\n"
        f"<b>Источник контактов:</b> {escape_html(item.get('contacts_source') or '-')}\n"
        f"<b>Уверенность:</b> {escape_html(item.get('contact_confidence') or '-')}"
    )


@router.message(Command("start"))
async def start(message: Message):
    await message.answer(
        "<b>Lead Parser</b>\n\n"
        "1. Нажми <b>Найти компании</b> и пришли поисковый запрос\n"
        "2. Или открой <b>Telegram сигналы</b> для Telegram Signal Miner\n"
        "3. Бот проанализирует компании и покажет лиды с контактами",
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
    await callback.message.answer(
        "<b>Telegram Signal Miner</b>\n\nВыбери сегмент для поиска сигналов.",
        parse_mode="HTML",
        reply_markup=telegram_signals_menu(),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("tg_collect:"))
async def tg_collect(callback: CallbackQuery):
    segment = (callback.data or "").split(":", 1)[1]
    segment_ru = _ru_segment(segment)
    await callback.answer("Запускаю поиск...", show_alert=False)
    await callback.message.answer(
        f"Запускаю поиск Telegram-сигналов: <b>{escape_html(segment_ru)}</b>",
        parse_mode="HTML",
    )
    try:
        result = await collect_signals(segment)
    except Exception as e:
        await callback.message.answer(
            f"Ошибка Telegram-поиска: {escape_html(str(e))}",
            parse_mode="HTML",
        )
        return

    await callback.message.answer(
        f"Поиск завершён. Создано: <b>{result['created']}</b>, обновлено: <b>{result['updated']}</b>.\n"
        f"Просканировано чатов: <b>{result['scanned_chats']}</b>, сообщений: <b>{result['scanned_messages']}</b>.",
        parse_mode="HTML",
        reply_markup=telegram_signals_menu(),
    )


@router.callback_query(F.data.startswith("tg_export:"))
async def tg_export(callback: CallbackQuery):
    mode = (callback.data or "").split(":", 1)[1]
    await callback.answer("Собираю Excel...", show_alert=False)
    path = build_signals_export(mode=mode)
    caption = "Готово: export по актуальным лидам." if mode == "actionable" else "Готово: raw export по сигналам."
    await callback.message.answer_document(FSInputFile(path), caption=caption)


@router.callback_query(F.data.startswith("tg_list:"))
async def tg_list(callback: CallbackQuery):
    parts = (callback.data or "").split(":")
    if len(parts) != 3:
        await callback.answer("Некорректный callback", show_alert=True)
        return

    _, second, third = parts
    if second.isdigit() or second == "all":
        page_raw, segment = second, third
    else:
        segment, page_raw = second, third

    page = 0 if page_raw == "all" else max(0, int(page_raw))
    segment_filter = None if segment == "all" else segment
    items = get_signals(segment=segment_filter, limit=None)
    if not items:
        await callback.message.answer("Пока Telegram-сигналов нет. Сначала запусти поиск по одному из сегментов.")
        await callback.answer()
        return

    total_pages = max(1, math.ceil(len(items) / PAGE_SIZE))
    page = min(page, total_pages - 1)
    chunk = items[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]

    lines = [
        f"<b>Telegram-сигналы</b>\n"
        f"<b>Сегмент:</b> {escape_html(_ru_segment(segment_filter or 'all'))}\n"
        f"<b>Страница:</b> {page + 1}/{total_pages}\n"
        f"<b>Всего сигналов:</b> {len(items)}"
    ]
    for idx, item in enumerate(chunk, start=page * PAGE_SIZE + 1):
        lines.append(format_signal_card(idx, item))

    await callback.message.answer(
        "\n\n".join(lines),
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=pagination_keyboard("tg_list", page, total_pages, extra=(segment_filter or "all")),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("tg_actionable:"))
async def tg_actionable(callback: CallbackQuery):
    _, page_raw, segment = (callback.data or "").split(":", 2)
    page = max(0, int(page_raw))
    segment_filter = None if segment == "all" else segment
    items = get_signals(segment=segment_filter, limit=None, only_actionable=True)
    if not items:
        await callback.message.answer("Пока актуальных лидов нет. Сначала запусти поиск по сегменту.")
        await callback.answer()
        return

    total_pages = max(1, math.ceil(len(items) / PAGE_SIZE))
    page = min(page, total_pages - 1)
    chunk = items[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]

    lines = [
        f"<b>Актуальные лиды из Telegram</b>\n"
        f"<b>Сегмент:</b> {escape_html(_ru_segment(segment_filter or 'all'))}\n"
        f"<b>Страница:</b> {page + 1}/{total_pages}\n"
        f"<b>Всего лидов:</b> {len(items)}"
    ]
    for idx, item in enumerate(chunk, start=page * PAGE_SIZE + 1):
        lines.append(format_signal_card(idx, item))

    await callback.message.answer(
        "\n\n".join(lines),
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=pagination_keyboard("tg_actionable", page, total_pages, extra=(segment_filter or "all")),
    )
    await callback.answer()


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

        lead_row = {
            "query": query,
            "company_name": company_name,
            "domain": item.get("domain"),
            "domain_normalized": domain,
            "root_domain": domain,
            "source": item.get("source", "ddgs"),
            "title": analysis.get("title"),
            "is_icp": icp["is_icp"],
            "icp_reason": icp["reason"],
            "hypothesis": hypothesis,
            "opener": opener,
            "lead_type": icp["lead_type"],
            "priority": icp["priority"],
            "company_inn": company_inn,
            "company_ogrn": company_ogrn,
            "company_legal_name": company_legal_name,
            "legal_form": legal_form,
            "inn_source": inn_source,
            "company_email": company_email,
            "company_phone": company_phone,
            "contacts_source": contacts_source,
            "contact_confidence": contact_confidence,
            "has_contacts": has_contacts,
            "sales_ready": bool(icp["is_icp"] and has_contacts),
            "updated_at": datetime.utcnow(),
            "last_enriched_at": datetime.utcnow(),
        }
        leads_to_save.append(lead_row)
        display_items.append(
            {
                "domain": domain,
                "company_name": company_name,
                "title": analysis.get("title"),
                "is_icp": icp["is_icp"],
                "lead_type_ru": _ru_lead_type(icp["lead_type"]),
                "priority_ru": _ru_priority(icp["priority"]),
                "icp_reason": icp["reason"],
                "hypothesis": hypothesis,
                "opener": opener,
                "company_inn": company_inn,
                "company_ogrn": company_ogrn,
                "company_legal_name": company_legal_name,
                "legal_form": legal_form,
                "inn_source": inn_source,
                "company_email": company_email,
                "company_phone": company_phone,
                "contacts_source": contacts_source,
                "contact_confidence": contact_confidence,
            }
        )

    if not leads_to_save:
        await message.answer("Компании нашлись, но без контактов. Попробуй другой запрос.")
        return

    save_leads(leads_to_save)
    total_pages = max(1, math.ceil(len(display_items) / PAGE_SIZE))
    first_chunk = display_items[:PAGE_SIZE]

    text = [f"<b>Найденные компании</b>\n<b>Страница:</b> 1/{total_pages}\n"]
    for idx, lead in enumerate(first_chunk, start=1):
        text.append(format_lead_card(idx, lead))

    sent = await message.answer(
        "\n\n".join(text),
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=pagination_keyboard("search_page", 0, total_pages),
    )
    SEARCH_RESULTS_CACHE[sent.message_id] = display_items
