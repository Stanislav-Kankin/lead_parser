import asyncio
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
from telegram_signals.exporter import export_signals_to_xlsx
from telegram_signals.repository import (
    get_business_like_messages,
    get_discussion_leads,
    get_review_leads,
    get_signals,
    get_target_leads,
)
from telegram_signals.service import collect_signals
from utils.domain_normalizer import normalize_domain

router = Router()
SEARCH_RESULTS_CACHE: dict[int, list[dict]] = {}

TG_MESSAGE_LIMIT = 3900


def _trim_text(value: str | None, limit: int = 220) -> str:
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
    if second.isdigit() or second == "all":
        page_raw, segment = second, third
    else:
        segment, page_raw = second, third
    page = 0 if page_raw == "all" else max(0, int(page_raw))
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
        "<b>Telegram Signal Miner</b>\n\nВыбери сегмент для поиска сигналов или один из режимов просмотра.",
        reply_markup=telegram_signals_menu(),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("tg_collect:"))
async def tg_collect(callback: CallbackQuery):
    segment = (callback.data or "").split(":", 1)[1]
    segment_ru = _ru_segment(segment)
    await callback.answer("Запускаю поиск…")
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
        f"Поиск завершён. Создано: <b>{result['created']}</b>, обновлено: <b>{result['updated']}</b>.",
        parse_mode="HTML",
        reply_markup=telegram_signals_menu(),
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
    text = _build_signal_page("Telegram-сигналы", items, page, total_pages, _ru_segment(segment_filter or "all"))

    await _send_or_edit(
        callback,
        text,
        reply_markup=pagination_keyboard("tg_list", page, total_pages, extra=(segment_filter or "all")),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("tg_targets:"))
async def tg_targets(callback: CallbackQuery):
    segment_filter, page = _parse_segment_page(callback.data or "")
    items = get_target_leads(segment=segment_filter, limit=None)
    if not items:
        await _send_or_edit(callback, "Пока целевых лидов нет. Сначала запусти поиск по сегменту.", reply_markup=telegram_signals_menu())
        await callback.answer()
        return

    total_pages = max(1, math.ceil(len(items) / PAGE_SIZE))
    page = min(page, total_pages - 1)
    text = _build_signal_page("Целевые лиды", items, page, total_pages, _ru_segment(segment_filter or "all"))

    await _send_or_edit(
        callback,
        text,
        reply_markup=pagination_keyboard("tg_targets", page, total_pages, extra=(segment_filter or "all")),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("tg_review:"))
async def tg_review(callback: CallbackQuery):
    segment_filter, page = _parse_segment_page(callback.data or "")
    items = get_review_leads(segment=segment_filter, limit=None)
    if not items:
        await _send_or_edit(callback, "Пока лидов на проверку нет. Сначала запусти поиск по сегменту.", reply_markup=telegram_signals_menu())
        await callback.answer()
        return

    total_pages = max(1, math.ceil(len(items) / PAGE_SIZE))
    page = min(page, total_pages - 1)
    text = _build_signal_page("Лиды на проверку", items, page, total_pages, _ru_segment(segment_filter or "all"))

    await _send_or_edit(
        callback,
        text,
        reply_markup=pagination_keyboard("tg_review", page, total_pages, extra=(segment_filter or "all")),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("tg_actionable:"))
async def tg_actionable(callback: CallbackQuery):
    segment_filter, page = _parse_segment_page(callback.data or "")
    items = get_signals(segment=segment_filter, limit=None, lead_fit_in=["target", "review"])
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


def format_signal_card(idx: int, signal) -> str:
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
        f"<b>Тип:</b> {escape_html(message_type)} / {escape_html(conversation_type)}\n"
        f"<b>Автор-профиль:</b> {escape_html(author_type)}\n"
        f"<b>Уровень:</b> {escape_html(_ru_signal_level(signal.signal_level))}\n"
        f"<b>Lead score:</b> {primary_score} | <b>Signal score:</b> {signal.signal_score}\n"
        f"<b>Контакт:</b> {escape_html(getattr(signal, 'contact_hint', None) or signal.author_username or signal.chat_url or '-')}\n"
        f"<b>Компания/сайт:</b> {escape_html(company)}\n"
        f"<b>Почему в выдаче:</b> {escape_html(why)}\n"
        f"<b>Фрагмент:</b> {escape_html(_trim_text(signal.text_excerpt or '-', 240))}\n"
        f"<b>Заход:</b> {escape_html(_trim_text(signal.recommended_opener or '-', 180))}\n"
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
        "ecom_direct_growth": "Свой сайт / Direct",
        "manufacturer_secondary": "Производители",
    }
    return mapping.get(value or "", value or "-")


def _ru_signal_level(value: str | None) -> str:
    mapping = {"high": "Высокий", "medium": "Средний", "low": "Низкий"}
    return mapping.get(value or "", value or "-")


def _ru_lead_fit(value: str | None) -> str:
    mapping = {
        "target": "Целевой",
        "review": "На проверку",
        "contractor": "Подрядчик",
        "noise": "Шум",
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
