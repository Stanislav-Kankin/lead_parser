import asyncio
import math
from typing import Any

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

from bot.keyboards import main_menu, pagination_keyboard
from enrichment.domain_analyzer import analyze_domain
from scoring.hypothesis_classifier import build_hypothesis
from scoring.icp_classifier import classify_icp
from sources.domain_search import search_domains_multi
from sources.query_builder import build_queries
from storage.lead_repository import count_leads, get_last_leads, save_leads

router = Router()

PAGE_SIZE = 5
SEARCH_CACHE: dict[int, list[dict[str, Any]]] = {}



def _format_lead_card(idx: int, lead: dict[str, Any]) -> str:
    return (
        f"{idx}. {lead['domain']}\n"
        f"Название: {lead.get('company_name') or lead.get('title') or '-'}\n"
        f"ICP: {'да' if lead.get('is_icp') else 'нет'}\n"
        f"Тип: {lead.get('lead_type_label') or '-'}\n"
        f"Приоритет: {lead.get('priority_label') or '-'}\n"
        f"Email: {lead.get('company_email') or '-'}\n"
        f"Телефон: {lead.get('company_phone') or '-'}\n"
        f"Причина: {lead.get('icp_reason') or '-'}\n"
        f"Гипотеза: {lead.get('hypothesis_label') or '-'}\n"
        f"Заход: {lead.get('opener') or '-'}\n"
    )



def _render_search_page(user_id: int, page: int) -> tuple[str, int]:
    items = SEARCH_CACHE.get(user_id, [])
    total_pages = max(1, math.ceil(len(items) / PAGE_SIZE))
    page = max(0, min(page, total_pages - 1))
    start = page * PAGE_SIZE
    end = start + PAGE_SIZE

    lines = [f"Найденные компании — страница {page + 1}/{total_pages}\n"]
    for idx, item in enumerate(items[start:end], start=start + 1):
        lines.append(_format_lead_card(idx, item))

    icp_count = sum(1 for item in items if item.get("is_icp"))
    contacts_count = sum(1 for item in items if item.get("company_email") or item.get("company_phone"))
    lines.append(f"Всего найдено доменов: {len(items)}")
    lines.append(f"ICP среди найденных: {icp_count}")
    lines.append(f"С контактами: {contacts_count}")
    return "\n".join(lines), total_pages



def _render_last_leads_page(page: int) -> tuple[str, int]:
    total = count_leads()
    total_pages = max(1, math.ceil(total / PAGE_SIZE))
    page = max(0, min(page, total_pages - 1))
    leads = get_last_leads(limit=PAGE_SIZE, offset=page * PAGE_SIZE)

    lines = [f"Последние лиды — страница {page + 1}/{total_pages}\n"]
    if not leads:
        lines.append("Пока лидов нет.")
    else:
        for idx, lead in enumerate(leads, start=page * PAGE_SIZE + 1):
            lines.append(
                f"{idx}. {lead.domain}\n"
                f"Название: {lead.company_name or lead.title or '-'}\n"
                f"Статус: {lead.status or 'new'}\n"
                f"ICP: {'да' if lead.is_icp else 'нет'}\n"
                f"Тип: {lead.lead_type or '-'}\n"
                f"Приоритет: {lead.priority or '-'}\n"
                f"Email: {lead.company_email or '-'}\n"
                f"Телефон: {lead.company_phone or '-'}\n"
                f"Причина: {lead.icp_reason or '-'}\n"
                f"Гипотеза: {lead.hypothesis or '-'}\n"
            )
    return "\n".join(lines), total_pages


@router.message(Command("start"))
async def start(message: Message):
    await message.answer(
        "Lead Parser\n\n"
        "Нажми кнопку ниже или просто пришли поисковый запрос.\n"
        "Пример: косметика оптом",
        reply_markup=main_menu(),
    )


@router.callback_query(F.data == "find_companies")
async def find_companies(callback: CallbackQuery):
    await callback.message.answer("Пришли поисковый запрос.\nПример: косметика оптом")
    await callback.answer()


@router.callback_query(F.data.startswith("last_leads:"))
async def last_leads(callback: CallbackQuery):
    page = int(callback.data.split(":", 1)[1])
    text, total_pages = _render_last_leads_page(page)
    await callback.message.answer(
        text,
        disable_web_page_preview=True,
        reply_markup=pagination_keyboard("last_leads", page, total_pages),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("search_page:"))
async def search_page(callback: CallbackQuery):
    page = int(callback.data.split(":", 1)[1])
    user_id = callback.from_user.id
    text, total_pages = _render_search_page(user_id, page)
    await callback.message.answer(
        text,
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
        raw_results = await search_domains_multi(
            queries=queries,
            per_query_limit=10,
            total_limit=25,
        )
    except asyncio.TimeoutError:
        await message.answer("Поиск завис по таймауту. Попробуй еще раз или упрости запрос.")
        return
    except Exception as e:
        await message.answer(f"Ошибка поиска: {e}")
        return

    if not raw_results:
        await message.answer("Ничего не найдено. Попробуй другой запрос.")
        return

    leads_to_save: list[dict[str, Any]] = []
    result_cards: list[dict[str, Any]] = []

    for item in raw_results:
        domain = item["domain"]
        company_name = item.get("company_name")
        site = await analyze_domain(domain)
        icp = classify_icp(
            title=site.get("title"),
            domain=domain,
            company_name=company_name,
            meta_description=site.get("meta_description"),
            text=site.get("text"),
        )
        hypothesis_code, hypothesis_label, opener = build_hypothesis(
            title=site.get("title"),
            is_icp=icp["is_icp"],
            company_name=company_name,
            meta_description=site.get("meta_description"),
            icp_score=icp["icp_score"],
            text=site.get("text"),
        )

        payload = {
            "query": query,
            "company_name": company_name or site.get("title") or domain,
            "domain": domain,
            "source": item.get("source", "ddgs"),
            "is_icp": icp["is_icp"],
            "icp_reason": icp["icp_reason"],
            "icp_score": icp["icp_score"],
            "lead_type": icp["lead_type"],
            "priority": icp["priority"],
            "hypothesis": hypothesis_label,
            "opener": opener,
            "title": site.get("title"),
            "meta_description": site.get("meta_description"),
            "company_email": site.get("email"),
            "company_phone": site.get("phone"),
            "status": "new",
        }
        leads_to_save.append(payload)
        result_cards.append({
            **payload,
            "lead_type_label": icp["lead_type_label"],
            "priority_label": icp["priority_label"],
            "hypothesis_label": hypothesis_label,
            "hypothesis_code": hypothesis_code,
        })

    created, updated = save_leads(leads_to_save)
    SEARCH_CACHE[message.from_user.id] = result_cards
    text, total_pages = _render_search_page(message.from_user.id, 0)
    text += f"\nСоздано новых лидов: {created}\nОбновлено существующих лидов: {updated}"

    await message.answer(
        text,
        disable_web_page_preview=True,
        reply_markup=pagination_keyboard("search_page", 0, total_pages),
    )
