import asyncio
import html
import logging
import math
from datetime import datetime

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

from bot.keyboards import PAGE_SIZE, main_menu, pagination_keyboard
from enrichment.domain_analyzer import analyze_domain
from enrichment.inn_client import get_company_by_domain
from scoring.hypothesis_classifier import build_hypothesis
from scoring.icp_classifier import classify_icp
from sources.domain_search import search_domains_multi
from sources.query_builder import build_queries
from storage.lead_repository import get_last_leads, save_leads
from utils.domain_normalizer import normalize_domain

logger = logging.getLogger(__name__)
router = Router()
SEARCH_RESULTS_CACHE: dict[int, list[dict]] = {}


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


@router.callback_query(F.data == "page_info")
async def page_info(callback: CallbackQuery):
    await callback.answer()


@router.callback_query(F.data.startswith("last_leads:"))
async def last_leads(callback: CallbackQuery):
    page = _parse_page(callback.data)
    leads = get_last_leads(limit=40, only_with_contacts=True)
    if not leads:
        await callback.message.answer("Пока нет лидов с контактами.")
        await callback.answer()
        return

    total_pages = max(1, math.ceil(len(leads) / PAGE_SIZE))
    page = min(page, total_pages - 1)
    chunk = leads[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]

    text = [f"<b>Последние лиды</b>\n<b>Страница:</b> {page + 1}/{total_pages}\n"]
    for idx, lead in enumerate(chunk, start=page * PAGE_SIZE + 1):
        text.append(format_lead_card(idx, {
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
            "employees": lead.employees,
        }))

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

    logger.info("[handle_query] query=%r", query)
    await message.answer("Ищу компании...")
    queries = build_queries(query)
    logger.info("[handle_query] generated_queries=%s", len(queries))

    try:
        raw_results = await search_domains_multi(queries=queries, per_query_limit=10, total_limit=25)
    except asyncio.TimeoutError:
        logger.warning("[handle_query] search_timeout query=%r", query)
        await message.answer("Поиск завис по таймауту. Попробуй ещё раз или упрости запрос.")
        return
    except Exception as exc:
        logger.exception("[handle_query] search_error query=%r", query)
        await message.answer(f"Ошибка поиска: {exc}")
        return

    logger.info("[handle_query] found_domains=%s", len(raw_results))
    if not raw_results:
        await message.answer("Ничего не найдено. Попробуй другой запрос.")
        return

    leads_to_save = []
    display_items = []

    for idx, item in enumerate(raw_results, start=1):
        domain = normalize_domain(item.get("domain"))
        if not domain:
            continue

        logger.info("[handle_query] analyze domain=%s (%s/%s)", domain, idx, len(raw_results))
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
        should_use_helper = bool(
            analysis.get("company_inn")
            or analysis.get("company_legal_name")
            or icp["is_icp"]
            or icp["priority"] in {"high", "medium"}
        )
        if should_use_helper:
            helper_data = await get_company_by_domain(domain)

        company_email = analysis.get("email") or _pick_value(helper_data, "email")
        company_phone = analysis.get("phone") or _pick_value(helper_data, "phone")
        company_inn = analysis.get("company_inn")
        company_ogrn = analysis.get("company_ogrn")
        company_legal_name = analysis.get("company_legal_name")
        legal_form = analysis.get("legal_form")
        inn_source = _ru_inn_source(analysis.get("inn_source"))
        has_contacts = bool(company_email or company_phone)

        if not has_contacts:
            logger.info("[handle_query] skip_no_contacts domain=%s", domain)
            continue

        contacts_source = _build_contacts_source(helper_data, analysis)
        contact_confidence = _get_contact_confidence(company_inn, company_legal_name, company_email, company_phone)
        sales_ready_score = _sales_ready_score(icp["is_icp"], company_email, company_phone, company_inn, company_legal_name)

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
            "sales_ready_score": sales_ready_score,
            "last_enriched_at": datetime.utcnow() if helper_data else None,
        }

        leads_to_save.append(lead)
        display_items.append(lead)

    display_items.sort(
        key=lambda x: (
            x.get("sales_ready_score", 0),
            x.get("is_icp", False),
            bool(x.get("company_inn")),
            x.get("contact_confidence") == "high",
            x.get("priority") == "high",
        ),
        reverse=True,
    )

    save_stats = save_leads(leads_to_save) if leads_to_save else {"created": 0, "updated": 0, "skipped": 0}

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
        "\n<b>Итог</b>\n"
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
    title = lead.get("title") or lead.get("company_name") or lead.get("domain")
    signals = _format_reason(lead.get("icp_reason"))
    legal_name = _shorten(lead.get("company_legal_name"), 90)
    hypothesis = _shorten(lead.get("hypothesis"), 140)
    opener = _shorten(lead.get("opener"), 170)

    lines = [
        f"<b>{idx}. {escape(lead.get('domain') or '-')}</b>",
        f"<b>Название:</b> {escape(title)}",
        f"<b>ICP:</b> {'Да' if lead.get('is_icp') else 'Нет'}",
        f"<b>Тип:</b> {escape(lead.get('lead_type_ru') or _ru_lead_type(lead.get('lead_type')))}",
        f"<b>Приоритет:</b> {escape(lead.get('priority_ru') or _ru_priority(lead.get('priority')))}",
        "",
        "<b>Контакты</b>",
        f"<b>Email:</b> {escape(lead.get('company_email') or '-')}",
        f"<b>Телефон:</b> {escape(lead.get('company_phone') or '-')}",
        f"<b>Источник контакта:</b> {escape(_ru_contacts_source(lead.get('contacts_source')))}",
        f"<b>Надёжность контакта:</b> {escape(_ru_confidence(lead.get('contact_confidence')))}",
    ]

    if lead.get("employees"):
        lines.append(f"<b>Сотрудники:</b> {escape(str(lead.get('employees')))}")

    lines.extend([
        "",
        "<b>Реквизиты</b>",
        f"<b>ИНН:</b> {escape(lead.get('company_inn') or '-')}",
        f"<b>ОГРН:</b> {escape(lead.get('company_ogrn') or '-')}",
        f"<b>Юр. лицо:</b> {escape(legal_name or '-')}",
        f"<b>Форма:</b> {escape(lead.get('legal_form') or '-')}",
        f"<b>Источник ИНН:</b> {escape(lead.get('inn_source') or '-')}",
    ])

    if lead.get("is_icp"):
        lines.extend([
            "",
            "<b>Гипотеза</b>",
            f"<b>Гипотеза:</b> {escape(hypothesis or '-')}",
            f"<b>Заход:</b> {escape(opener or '-')}",
        ])

    lines.extend([
        "",
        "<b>Сигналы</b>",
        signals,
    ])
    return "\n".join(lines)



def _format_reason(reason: str | None) -> str:
    if not reason:
        return "<b>+</b> -\n<b>-</b> -"

    positive = []
    negative = []
    for part in reason.split(";"):
        part = part.strip()
        if part.startswith("positive:"):
            positive = [x.strip() for x in part.removeprefix("positive:").split(",") if x.strip()]
        elif part.startswith("negative:"):
            negative = [x.strip() for x in part.removeprefix("negative:").split(",") if x.strip()]

    pos = ", ".join(positive) if positive else "-"
    neg = ", ".join(negative) if negative else "-"
    return f"<b>+</b> {escape(pos)}\n<b>-</b> {escape(neg)}"



def _sales_ready_score(is_icp: bool, email: str | None, phone: str | None, inn: str | None, legal_name: str | None) -> int:
    score = 0
    if is_icp:
        score += 5
    if email:
        score += 2
    if phone:
        score += 2
    if inn:
        score += 2
    if legal_name:
        score += 1
    return score



def _build_contacts_source(helper_data: dict | None, analysis: dict) -> str:
    has_site = bool(analysis.get("email") or analysis.get("phone"))
    has_helper = bool(_pick_value(helper_data, "email") or _pick_value(helper_data, "phone"))
    if has_site and has_helper:
        return "site+helper_api"
    if has_site:
        return "site"
    if has_helper:
        return "helper_api"
    return "-"



def _get_contact_confidence(company_inn: str | None, company_legal_name: str | None, company_email: str | None, company_phone: str | None) -> str:
    if company_inn or company_legal_name:
        return "high"
    if company_email and company_phone:
        return "medium"
    if company_email or company_phone:
        return "low"
    return "low"



def _pick_value(data: dict | None, key: str):
    if not data:
        return None
    value = data.get(key)
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return value



def _parse_page(data: str | None) -> int:
    if not data or ":" not in data:
        return 0
    try:
        return max(0, int(data.split(":", 1)[1]))
    except ValueError:
        return 0



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



def _ru_contacts_source(value: str | None) -> str:
    mapping = {
        "site": "Сайт",
        "helper_api": "Helper API",
        "site+helper_api": "Сайт + Helper API",
        "-": "-",
        None: "-",
    }
    return mapping.get(value, value or "-")



def _ru_confidence(value: str | None) -> str:
    mapping = {"high": "Высокая", "medium": "Средняя", "low": "Низкая"}
    return mapping.get(value or "", value or "-")



def _ru_inn_source(value: str | None) -> str | None:
    mapping = {"site_requisites": "Сайт / реквизиты"}
    return mapping.get(value, value)



def _shorten(value: str | None, limit: int) -> str | None:
    if not value:
        return None
    text = " ".join(str(value).split())
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"



def escape(value) -> str:
    if value is None:
        return "-"
    return html.escape(str(value))
