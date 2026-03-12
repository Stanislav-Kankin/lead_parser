import asyncio

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

from bot.keyboards import main_menu
from enrichment.domain_analyzer import analyze_domain
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
            f"Статус: {lead.status or 'new'}\n"
            f"ICP: {'да' if lead.is_icp else 'нет'}\n"
            f"Тип: {lead.lead_type or '-'}\n"
            f"Приоритет: {lead.priority or '-'}\n"
            f"Email: {lead.company_email or '-'}\n"
            f"Телефон: {lead.company_phone or '-'}\n"
            f"Причина: {lead.icp_reason or '-'}\n"
            f"Гипотеза: {lead.hypothesis or '-'}\n"
        )
    await callback.message.answer("\n".join(lines), disable_web_page_preview=True)
    await callback.answer()


async def _analyze_result(item: dict, query: str) -> dict:
    domain = item["domain"]
    company_name = item.get("company_name")

    analyzed = await analyze_domain(domain)
    emails = analyzed.get("emails") or []
    phones = analyzed.get("phones") or []

    is_icp, icp_reason, icp_score, lead_type, priority = classify_icp(
        title=analyzed.get("title"),
        domain=domain,
        company_name=company_name,
        description=analyzed.get("description"),
        h1=analyzed.get("h1"),
        text=analyzed.get("text"),
        has_contacts=bool(emails or phones),
    )
    hypothesis = build_hypothesis(
        title=(analyzed.get("title") or analyzed.get("description")),
        is_icp=is_icp,
        company_name=company_name,
    )

    return {
        "query": query,
        "company_name": company_name or analyzed.get("title") or domain,
        "domain": domain,
        "domain_normalized": item.get("domain_normalized") or domain,
        "source": item.get("source", "ddgs"),
        "is_icp": is_icp,
        "icp_reason": f"score={icp_score}; {icp_reason}",
        "hypothesis": hypothesis,
        "title": analyzed.get("title"),
        "lead_type": lead_type,
        "priority": priority,
        "company_email": emails[0] if emails else None,
        "company_phone": phones[0] if phones else None,
        "status": "new",
    }


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

    analyzed_leads = await asyncio.gather(*[_analyze_result(item, query) for item in raw_results])
    created, updated = save_leads(analyzed_leads)

    response_lines = ["Найденные компании:\n"]
    for idx, lead in enumerate(analyzed_leads[:10], start=1):
        response_lines.append(
            f"{idx}. {lead['domain']}\n"
            f"Название: {(lead.get('company_name') or '-')[:90]}\n"
            f"ICP: {'да' if lead['is_icp'] else 'нет'}\n"
            f"Тип: {lead.get('lead_type') or '-'}\n"
            f"Приоритет: {lead.get('priority') or '-'}\n"
            f"Email: {lead.get('company_email') or '-'}\n"
            f"Телефон: {lead.get('company_phone') or '-'}\n"
            f"Причина: {lead.get('icp_reason') or '-'}\n"
            f"Гипотеза: {lead.get('hypothesis') or '-'}\n"
        )

    icp_count = sum(1 for lead in analyzed_leads if lead["is_icp"])
    with_contacts_count = sum(1 for lead in analyzed_leads if lead.get("company_email") or lead.get("company_phone"))

    response_lines.append(f"Всего найдено доменов: {len(raw_results)}")
    response_lines.append(f"ICP среди найденных: {icp_count}")
    response_lines.append(f"С контактами: {with_contacts_count}")
    response_lines.append(f"Создано новых лидов: {created}")
    response_lines.append(f"Обновлено существующих лидов: {updated}")

    await message.answer("\n".join(response_lines), disable_web_page_preview=True)
