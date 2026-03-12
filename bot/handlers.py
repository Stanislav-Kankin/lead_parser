import asyncio

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

from bot.keyboards import main_menu
from enrichment.domain_analyzer import fetch_site_title
from scoring.hypothesis_classifier import build_hypothesis
from scoring.icp_classifier import classify_icp
from sources.domain_search import search_domains_multi
from sources.query_builder import build_queries
from storage.lead_repository import get_last_leads, save_leads

router = Router()


@router.message(Command("start"))
async def start(message: Message):
    await message.answer(
        "Lead Parser\n\n"
        "Нажми кнопку ниже или просто пришли поисковый запрос.\n"
        "Пример: производство мебели",
        reply_markup=main_menu(),
    )


@router.callback_query(F.data == "find_companies")
async def find_companies(callback: CallbackQuery):
    await callback.message.answer("Пришли поисковый запрос.\nПример: косметика оптом")
    await callback.answer()


@router.callback_query(F.data == "last_leads")
async def last_leads(callback: CallbackQuery):
    leads = get_last_leads(limit=10)
    if not leads:
        await callback.message.answer("Пока лидов нет.")
        await callback.answer()
        return

    lines = ["Последние лиды:\n"]
    for idx, lead in enumerate(leads, start=1):
        lines.append(
            f"{idx}. {lead.domain}\n"
            f"ICP: {'да' if lead.is_icp else 'нет'}\n"
            f"Причина: {lead.icp_reason or '-'}\n"
            f"Гипотеза: {lead.hypothesis or '-'}\n"
        )
    await callback.message.answer("\n".join(lines), disable_web_page_preview=True)
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

    leads_to_save = []
    response_lines = ["Найденные компании:\n"]

    for idx, item in enumerate(raw_results[:10], start=1):
        domain = item["domain"]
        company_name = item.get("company_name")
        title = await fetch_site_title(domain)
        is_icp, icp_reason = classify_icp(title=title, domain=domain, company_name=company_name)
        hypothesis = build_hypothesis(title=title, is_icp=is_icp, company_name=company_name)

        leads_to_save.append({
            "query": query,
            "company_name": company_name or title or domain,
            "domain": domain,
            "source": item.get("source", "ddgs"),
            "is_icp": is_icp,
            "icp_reason": icp_reason,
            "hypothesis": hypothesis,
            "title": title,
        })

        response_lines.append(
            f"{idx}. {domain}\n"
            f"Название: {(company_name or title or '-')[:90]}\n"
            f"ICP: {'да' if is_icp else 'нет'}\n"
            f"Причина: {icp_reason}\n"
            f"Гипотеза: {hypothesis or '-'}\n"
        )

    saved = save_leads(leads_to_save)
    response_lines.append(f"Всего найдено доменов: {len(raw_results)}")
    response_lines.append(f"Сохранено новых лидов: {saved}")

    await message.answer("\n".join(response_lines), disable_web_page_preview=True)
