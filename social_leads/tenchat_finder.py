from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from html import unescape
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from models.lead import Lead
from sources.url_search import normalize_url, search_urls_multi
from storage.lead_repository import get_web_leads
from storage.social_lead_repository import save_social_leads

logger = logging.getLogger(__name__)

DEFAULT_TENCHAT_PRESET = "mass_people"

TENCHAT_SEARCH_PRESETS = {
    "mass_people": {
        "label": "Массовый поиск ЛПР",
        "description": "Широкий поиск собственников, CEO, коммерческих директоров, маркетинга и e-commerce по товарным бизнесам.",
        "queries": [
            'site:tenchat.ru "основатель" "отзывы" "бренд"',
            'site:tenchat.ru "собственник" "отзывы" "производство"',
            'site:tenchat.ru "генеральный директор" "отзывы" "интернет-магазин"',
            'site:tenchat.ru "директор по маркетингу" "отзывы" "бренд"',
            'site:tenchat.ru "руководитель e-commerce" "отзывы" "маркетплейсы"',
        ],
    },
    "project_people": {
        "label": "ЛПР по Web-проекту",
        "description": "Берет компании из выбранного web-проекта и ищет владельцев, CEO, маркетинг и e-commerce в TenChat.",
        "queries": [
            'site:tenchat.ru "основатель" "бренд" "производитель"',
            'site:tenchat.ru "генеральный директор" "производитель"',
            'site:tenchat.ru "директор по маркетингу" "бренд"',
        ],
    },
    "manufacturers_people": {
        "label": "Руководители производителей",
        "description": "Общий поиск собственников и директоров производственных компаний.",
        "queries": [
            'site:tenchat.ru "основатель" "собственное производство"',
            'site:tenchat.ru "собственник" "производитель"',
            'site:tenchat.ru "генеральный директор" "производство"',
            'site:tenchat.ru "коммерческий директор" "производитель"',
            'site:tenchat.ru "директор по развитию" "российский бренд"',
        ],
    },
    "ecom_people": {
        "label": "Ecom / маркетинг",
        "description": "Ищет руководителей e-commerce и маркетинга у брендов и интернет-магазинов.",
        "queries": [
            'site:tenchat.ru "директор по маркетингу" "интернет-магазин"',
            'site:tenchat.ru "руководитель e-commerce" "бренд"',
            'site:tenchat.ru "e-commerce директор" "wildberries" "ozon"',
            'site:tenchat.ru "маркетинг" "бренд" "маркетплейсы"',
            'site:tenchat.ru "руководитель маркетинга" "производитель"',
        ],
    },
    "project_people_wide": {
        "label": "Широко по Web-проекту",
        "description": "Больше вариаций по каждой web-компании: роли, бренд, ecom, маркетплейсы, direct и развитие.",
        "queries": [
            'site:tenchat.ru "основатель" "интернет-магазин" "бренд"',
            'site:tenchat.ru "собственник" "wildberries" "ozon"',
            'site:tenchat.ru "генеральный директор" "российский бренд"',
            'site:tenchat.ru "руководитель e-commerce" "маркетплейсы"',
            'site:tenchat.ru "директор по развитию" "direct"',
        ],
    },
    "brand_owners": {
        "label": "Собственники брендов",
        "description": "Ищет основателей и собственников товарных брендов.",
        "queries": [
            'site:tenchat.ru "основатель бренда"',
            'site:tenchat.ru "собственник бренда"',
            'site:tenchat.ru "основатель" "российский бренд"',
            'site:tenchat.ru "сооснователь" "бренд" "товары"',
            'site:tenchat.ru "владелец бренда" "маркетплейсы"',
        ],
    },
    "marketplace_sellers": {
        "label": "Селлеры WB/Ozon",
        "description": "Ищет владельцев и руководителей селлерского бизнеса на маркетплейсах.",
        "queries": [
            'site:tenchat.ru "собственник" "wildberries"',
            'site:tenchat.ru "основатель" "ozon"',
            'site:tenchat.ru "селлер" "генеральный директор"',
            'site:tenchat.ru "продавец на wildberries" "собственник"',
            'site:tenchat.ru "маркетплейсы" "директор по развитию"',
        ],
    },
    "beauty_fashion": {
        "label": "Beauty / fashion",
        "description": "Косметика, одежда, обувь, аксессуары, fashion-бренды.",
        "queries": [
            'site:tenchat.ru "основатель" "бренд косметики"',
            'site:tenchat.ru "собственник" "косметика" "wildberries"',
            'site:tenchat.ru "основатель" "бренд одежды"',
            'site:tenchat.ru "директор по маркетингу" "fashion бренд"',
            'site:tenchat.ru "производство одежды" "генеральный директор"',
        ],
    },
    "food_household": {
        "label": "Food / home / household",
        "description": "Продукты, товары для дома, бытовая химия, текстиль.",
        "queries": [
            'site:tenchat.ru "основатель" "продукты питания"',
            'site:tenchat.ru "собственник" "товары для дома"',
            'site:tenchat.ru "генеральный директор" "бытовая химия"',
            'site:tenchat.ru "производитель" "текстиль" "директор"',
            'site:tenchat.ru "интернет-магазин" "товары для дома" "основатель"',
        ],
    },
    "direct_brands": {
        "label": "Direct / D2C бренды",
        "description": "Руководители брендов с собственным сайтом, direct и интернет-магазином.",
        "queries": [
            'site:tenchat.ru "основатель" "d2c"',
            'site:tenchat.ru "интернет-магазин" "собственник"',
            'site:tenchat.ru "директор по маркетингу" "direct"',
            'site:tenchat.ru "руководитель e-commerce" "интернет-магазин"',
            'site:tenchat.ru "бренд" "собственный сайт" "основатель"',
        ],
    },
}

TENCHAT_SEARCH_PRESETS = {
    key: TENCHAT_SEARCH_PRESETS[key]
    for key in [
        "mass_people",
        "project_people_wide",
        "project_people",
        "brand_owners",
        "marketplace_sellers",
        "ecom_people",
        "manufacturers_people",
        "beauty_fashion",
        "food_household",
        "direct_brands",
    ]
}

DECISION_ROLES = [
    "основатель",
    "сооснователь",
    "собственник",
    "владелец",
    "предприниматель",
    "ceo",
    "генеральный директор",
    "исполнительный директор",
    "управляющий партнер",
    "коммерческий директор",
    "директор по маркетингу",
    "руководитель маркетинга",
    "директор по продажам",
    "директор по развитию",
    "руководитель e-commerce",
    "e-commerce директор",
    "ecommerce директор",
    "head of ecommerce",
]

PROJECT_CONTEXT_TERMS = [
    "бренд",
    "производитель",
    "интернет-магазин",
    "маркетплейсы",
    "wildberries",
    "ozon",
    "e-commerce",
    "direct",
    "d2c",
    "директ",
    "рост продаж",
]

PEOPLE_SEARCH_ROLES = [
    "основатель",
    "сооснователь",
    "собственник",
    "владелец",
    "предприниматель",
    "CEO",
    "генеральный директор",
    "коммерческий директор",
    "директор по маркетингу",
    "руководитель маркетинга",
    "директор по развитию",
    "руководитель e-commerce",
]

PEOPLE_SEARCH_CONTEXTS = [
    "бренд",
    "производитель",
    "производство",
    "собственное производство",
    "интернет-магазин",
    "e-commerce",
    "маркетплейсы",
    "wildberries",
    "ozon",
    "direct",
    "d2c",
    "косметика",
    "одежда",
    "текстиль",
    "товары для дома",
    "продукты питания",
    "бытовая химия",
]

PRESET_QUERY_MATRIX = {
    "mass_people": {
        "roles": PEOPLE_SEARCH_ROLES,
        "contexts": PEOPLE_SEARCH_CONTEXTS,
        "max_queries": 120,
    },
    "brand_owners": {
        "roles": ["основатель", "сооснователь", "собственник", "владелец", "CEO", "генеральный директор"],
        "contexts": ["бренд", "российский бренд", "товарный бренд", "косметика", "одежда", "товары для дома", "продукты питания"],
        "max_queries": 80,
    },
    "marketplace_sellers": {
        "roles": ["собственник", "основатель", "CEO", "генеральный директор", "руководитель e-commerce"],
        "contexts": ["wildberries", "ozon", "маркетплейсы", "селлер", "интернет-магазин", "бренд"],
        "max_queries": 70,
    },
    "ecom_people": {
        "roles": ["директор по маркетингу", "руководитель маркетинга", "руководитель e-commerce", "коммерческий директор", "директор по развитию"],
        "contexts": ["интернет-магазин", "e-commerce", "бренд", "direct", "d2c", "маркетплейсы", "wildberries", "ozon"],
        "max_queries": 80,
    },
    "manufacturers_people": {
        "roles": ["основатель", "собственник", "генеральный директор", "коммерческий директор", "директор по развитию"],
        "contexts": ["производитель", "производство", "собственное производство", "бренд", "товары", "FMCG"],
        "max_queries": 70,
    },
}

ROLE_SIGNALS = {
    "собственник": 28,
    "основатель": 28,
    "сооснователь": 24,
    "фаундер": 24,
    "ceo": 24,
    "генеральный директор": 26,
    "управляющий партнер": 22,
    "коммерческий директор": 20,
    "директор по маркетингу": 20,
    "руководитель маркетинга": 18,
    "директор по развитию": 18,
    "руководитель e-commerce": 18,
    "e-commerce директор": 18,
    "ecommerce": 14,
    "маркетолог": 8,
}

ICP_SIGNALS = {
    "производитель": 16,
    "производство": 14,
    "собственное производство": 22,
    "бренд": 16,
    "российский бренд": 20,
    "товары": 8,
    "fmcg": 12,
    "косметика": 10,
    "одежда": 9,
    "обувь": 9,
    "товары для дома": 10,
    "текстиль": 10,
    "бытовая химия": 10,
    "продукты питания": 10,
    "интернет-магазин": 12,
    "маркетплейс": 9,
    "wildberries": 10,
    "ozon": 10,
}

GROWTH_CONTEXT_SIGNALS = {
    "маркетплейс": 8,
    "маркетплейсы": 8,
    "wildberries": 8,
    "wb": 6,
    "ozon": 8,
    "direct": 7,
    "d2c": 8,
    "интернет-магазин": 8,
    "брендовый спрос": 10,
    "повторные продажи": 8,
    "рост продаж": 7,
    "масштабирование": 8,
    "маржа": 8,
    "комиссии": 8,
    "директ": 7,
    "рся": 7,
}

NEGATIVE_SIGNALS = {
    "маркетинговое агентство": -35,
    "рекламное агентство": -35,
    "digital-агентство": -35,
    "мы агентство": -35,
    "наше агентство": -30,
    "оказываю услуги": -28,
    "оказываем услуги": -28,
    "веду рекламу": -24,
    "настраиваю рекламу": -24,
    "ищу клиентов": -30,
    "беру проекты": -28,
    "фрилансер": -24,
    "таргетолог": -20,
    "директолог": -16,
    "seo": -14,
    "hr": -16,
    "рекрутер": -18,
    "ищу работу": -22,
    "вакансии": -18,
    "курс": -12,
    "обучение": -12,
}

ARTICLE_MARKERS = [
    "как ",
    "почему ",
    "что такое",
    "гайд",
    "инструкция",
    "чек-лист",
    "топ-",
    "новости",
    "разбор",
    "кейс",
    "способ",
    "по шагам",
    "квиз",
]

INN_PATTERN = re.compile(r"(?:инн|ИНН)[^\d]{0,20}(\d{10}|\d{12})", re.IGNORECASE)
OGRN_PATTERN = re.compile(r"(?:огрн|ОГРН)[^\d]{0,20}(\d{13}|\d{15})", re.IGNORECASE)


@dataclass(frozen=True)
class QueryItem:
    query: str
    web_lead_id: int | None = None
    web_domain: str | None = None
    web_title: str | None = None
    web_company_name: str | None = None
    web_legal_name: str | None = None
    web_inn: str | None = None
    web_icp_score: int = 0


async def collect_people_leads(
    *,
    custom_queries: str | None = None,
    preset: str = DEFAULT_TENCHAT_PRESET,
    total_limit: int = 40,
    per_query_limit: int = 7,
    concurrency: int = 6,
    project_id: int | None = None,
    project_name: str | None = None,
    project_limit: int = 25,
) -> dict:
    query_items = build_people_query_items(
        custom_queries=custom_queries,
        preset=preset,
        project_id=project_id,
        project_limit=project_limit,
    )
    queries = [item.query for item in query_items]
    query_map = {item.query: item for item in query_items}
    search_limit = max(total_limit, min(total_limit * 3, 600))
    raw_candidates = await search_urls_multi(
        queries,
        per_query_limit=per_query_limit,
        total_limit=search_limit,
        allowed_domains={"tenchat.ru"},
    )
    candidates = _filter_people_candidates(raw_candidates)[: max(1, int(total_limit or 40))]

    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def enrich(candidate: dict) -> dict | None:
        async with semaphore:
            query_item = query_map.get(str(candidate.get("source_query") or ""))
            page = await _fetch_public_page(candidate.get("url"))
            company_page = {}
            company_url = _best_company_url(page)
            if company_url:
                company_page = await _fetch_public_page(company_url)
            item = _build_social_lead(
                candidate,
                page,
                company_page=company_page,
                company_url=company_url,
                query_item=query_item,
                project_id=project_id,
            )
            if not _is_actionable_people_candidate(item):
                return None
            return item

    enriched = [item for item in await asyncio.gather(*(enrich(candidate) for candidate in candidates)) if item]
    save_stats = save_social_leads(enriched, project_id=project_id, project_name=project_name)
    return {
        "queries": len(queries),
        "preset": preset,
        "project_id": project_id,
        "project_name": project_name,
        "candidates": len(candidates),
        "raw_candidates": len(raw_candidates),
        "analyzed": len(candidates),
        "kept": len(enriched),
        "created": save_stats.get("created", 0),
        "updated": save_stats.get("updated", 0),
        "skipped": save_stats.get("skipped", 0),
    }


def build_people_query_items(
    *,
    custom_queries: str | None = None,
    preset: str = DEFAULT_TENCHAT_PRESET,
    project_id: int | None = None,
    project_limit: int = 25,
) -> list[QueryItem]:
    items: list[QueryItem] = []
    seen: set[str] = set()

    if project_id:
        web_leads = get_web_leads(
            limit=max(1, min(150, int(project_limit or 25))),
            project_id=project_id,
            min_score=35,
        )
        for lead in web_leads:
            for query in _queries_for_web_lead(lead, preset=preset):
                if query in seen:
                    continue
                seen.add(query)
                items.append(
                    QueryItem(
                        query=query,
                        web_lead_id=lead.id,
                        web_domain=lead.domain_normalized or lead.domain,
                        web_title=lead.title or lead.company_name,
                        web_company_name=lead.company_name,
                        web_legal_name=lead.focus_legal_name or lead.company_legal_name,
                        web_inn=lead.company_inn,
                        web_icp_score=int(lead.icp_score or 0),
                    )
                )

    for line in [line.strip() for line in (custom_queries or "").splitlines() if line.strip()]:
        query = line if "site:" in line else f"site:tenchat.ru {line}"
        if query not in seen:
            seen.add(query)
            items.append(QueryItem(query=query))

    for query in _generated_people_queries_for_preset(preset):
        if query not in seen:
            seen.add(query)
            items.append(QueryItem(query=query))

    preset_config = TENCHAT_SEARCH_PRESETS.get(preset, TENCHAT_SEARCH_PRESETS[DEFAULT_TENCHAT_PRESET])
    for query in preset_config["queries"]:
        if query not in seen:
            seen.add(query)
            items.append(QueryItem(query=query))

    return items[:220]


def _generated_people_queries_for_preset(preset: str) -> list[str]:
    config = PRESET_QUERY_MATRIX.get(preset)
    if not config:
        return []
    roles = list(config.get("roles") or [])
    contexts = list(config.get("contexts") or [])
    max_queries = int(config.get("max_queries") or 80)
    queries: list[str] = []
    for role in roles:
        for context in contexts:
            queries.append(f'site:tenchat.ru "{role}" "отзывы" "{context}"')
            queries.append(f'site:tenchat.ru "{role}" "{context}" "ООО"')
    return list(dict.fromkeys(queries))[:max_queries]


async def _fetch_public_page(url: str | None) -> dict:
    if not url:
        return {}
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36"
        )
    }
    try:
        async with httpx.AsyncClient(timeout=12, follow_redirects=True, headers=headers) as client:
            response = await client.get(url)
            response.raise_for_status()
    except Exception as exc:
        logger.info("[tenchat_finder] fetch_failed url=%s error=%s", url, exc)
        return {}

    raw_html = response.text
    soup = BeautifulSoup(raw_html, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    title = (soup.title.string if soup.title and soup.title.string else "")[:300]
    h1 = " ".join(tag.get_text(" ", strip=True) for tag in soup.find_all("h1")[:2])
    text = " ".join(soup.get_text(" ", strip=True).split())
    company_urls = _extract_company_urls(soup, str(response.url))
    return {
        "url": str(response.url),
        "title": unescape(title),
        "h1": h1,
        "text": unescape(text[:70000]),
        "full_text": unescape(text),
        "company_urls": company_urls,
        "company_inn": _extract_inn(raw_html) or _extract_inn(text),
        "company_ogrn": _extract_ogrn(raw_html) or _extract_ogrn(text),
        "company_legal_name": _extract_legal_name(text),
    }


def _build_social_lead(
    candidate: dict,
    page: dict,
    *,
    company_page: dict | None = None,
    company_url: str | None = None,
    query_item: QueryItem | None,
    project_id: int | None,
) -> dict:
    url = normalize_url(candidate.get("url")) or candidate.get("url") or ""
    title = page.get("title") or candidate.get("title") or ""
    snippet = candidate.get("body") or ""
    text = page.get("text") or " ".join([title, snippet])
    company_page = company_page or {}
    company_text = company_page.get("full_text") or company_page.get("text") or ""
    h1 = page.get("h1") or ""
    full_text = " ".join([title, h1, snippet, text, company_text]).lower()
    person_name, role_title = _parse_person_and_role(title, h1)
    company_name = _extract_company(full_text) or (query_item.web_company_name if query_item else None)
    company_inn = (
        company_page.get("company_inn")
        or page.get("company_inn")
        or _extract_inn(full_text)
        or (query_item.web_inn if query_item else None)
    )
    company_ogrn = company_page.get("company_ogrn") or page.get("company_ogrn") or _extract_ogrn(full_text)
    company_legal_name = (
        company_page.get("company_legal_name")
        or page.get("company_legal_name")
        or (query_item.web_legal_name if query_item else None)
        or _extract_legal_name(full_text)
    )
    company_name = company_name or company_legal_name
    profile_url, post_url = _split_tenchat_url(url)
    classification = _classify_people_lead(
        full_text,
        person_name=person_name,
        role_title=role_title,
        company_name=company_name,
        company_inn=company_inn,
        is_profile_url=_is_tenchat_profile_url(url),
        query_item=query_item,
    )
    opener = _build_opener(person_name, role_title, company_name, classification)

    return {
        "source": "tenchat",
        "source_url": profile_url or url,
        "source_query": candidate.get("source_query"),
        "profile_url": profile_url,
        "post_url": post_url,
        "person_name": person_name,
        "role_title": role_title,
        "company_name": company_name,
        "company_inn": company_inn,
        "company_ogrn": company_ogrn,
        "company_legal_name": company_legal_name,
        "company_url": company_url,
        "matched_web_lead_id": query_item.web_lead_id if query_item else None,
        "matched_web_domain": query_item.web_domain if query_item else None,
        "matched_web_title": query_item.web_title if query_item else None,
        "title": title or candidate.get("title"),
        "snippet": snippet,
        "text": text[:12000],
        "opener": opener,
        "project_id": project_id,
        **classification,
    }


def _queries_for_web_lead(lead: Lead, *, preset: str = DEFAULT_TENCHAT_PRESET) -> list[str]:
    names = _company_name_candidates(lead)
    queries: list[str] = []
    is_wide = preset == "project_people_wide"
    max_names = 3 if is_wide else 2
    role_limit = 14 if is_wide else 8
    max_queries = 30 if is_wide else 12
    category_terms = _category_terms_for_lead(lead)

    for name in names[:max_names]:
        for role in DECISION_ROLES[:role_limit]:
            queries.append(f'site:tenchat.ru "{name}" "{role}"')
        if is_wide:
            for term in [*category_terms, *PROJECT_CONTEXT_TERMS][:12]:
                queries.append(f'site:tenchat.ru "{name}" "{term}"')
            queries.append(f'site:tenchat.ru "{name}" "отзывы"')
            queries.append(f'site:tenchat.ru "{name}" "компания"')
    return list(dict.fromkeys(queries))[:max_queries]


def _company_name_candidates(lead: Lead) -> list[str]:
    raw_values = [
        lead.focus_legal_name,
        lead.company_legal_name,
        lead.company_name,
        lead.title,
        _brand_from_domain(lead.domain_normalized or lead.domain),
    ]
    result: list[str] = []
    seen: set[str] = set()
    for value in raw_values:
        clean = _clean_company_query(value)
        if clean and clean.lower() not in seen:
            seen.add(clean.lower())
            result.append(clean)
    return result


def _category_terms_for_lead(lead: Lead) -> list[str]:
    raw = " ".join(
        str(value or "")
        for value in [
            lead.search_category,
            lead.lead_type,
            lead.evidence,
            lead.icp_reason,
            lead.site_assessment,
        ]
    ).lower()
    terms: list[str] = []
    category_map = {
        "космет": "косметика",
        "beauty": "косметика",
        "одеж": "одежда",
        "fashion": "одежда",
        "обув": "обувь",
        "текст": "текстиль",
        "дом": "товары для дома",
        "household": "товары для дома",
        "еда": "продукты питания",
        "продукт": "продукты питания",
        "fmcg": "fmcg",
        "маркетплейс": "маркетплейсы",
        "wildberries": "wildberries",
        "ozon": "ozon",
        "direct": "direct",
        "d2c": "d2c",
    }
    for marker, term in category_map.items():
        if marker in raw and term not in terms:
            terms.append(term)
    return terms


def _extract_company_urls(soup: BeautifulSoup, base_url: str) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for link in soup.find_all("a", href=True):
        href = str(link.get("href") or "")
        text = " ".join(link.get_text(" ", strip=True).split()).lower()
        parsed_path = urlparse(href).path.strip("/")
        first_path_part = parsed_path.split("/", 1)[0] if parsed_path else ""
        is_long_numeric_page = bool(re.fullmatch(r"\d{10,15}", first_path_part))
        has_company_text = any(marker in text for marker in ["ооо", "ао ", "пао", "ип ", "компани", "компания"])
        if not is_long_numeric_page or not has_company_text:
            continue
        absolute = normalize_url(urljoin(base_url, f"/{first_path_part}"))
        if absolute and absolute not in seen:
            seen.add(absolute)
            urls.append(absolute)
    return urls[:5]


def _best_company_url(page: dict) -> str | None:
    urls = page.get("company_urls") or []
    if not urls:
        return None
    return str(urls[0])


def _parse_person_and_role(title: str, h1: str) -> tuple[str | None, str | None]:
    sources = [title, h1, " ".join(part for part in [h1, title] if part).strip()]
    patterns = [
        r"^(?P<name>[А-ЯЁA-Z][^—|]{3,100}?)(?:,\s*[^—|,]{2,40})?,?\s*\d{1,2}\s*(?:лет|года|год)?\s*[—-]\s*(?P<role>[^|]{3,180})",
        r"^(?P<name>[А-ЯЁA-Z][^—|]{3,100})\s*[—-]\s*(?P<role>[^|]{3,180})",
    ]
    for source in sources:
        source = re.sub(r"\s+", " ", source or "").strip()
        source = re.sub(r"\s*\|\s*TenChat.*$", "", source, flags=re.IGNORECASE)
        source = re.sub(r"\s*—\s*TenChat.*$", "", source, flags=re.IGNORECASE)
        for pattern in patterns:
            match = re.search(pattern, source)
            if match:
                return _clean_person_name(match.group("name")), _clean_value(match.group("role"))
    if h1 and _looks_like_person_name(h1):
        return _clean_value(h1), None
    return None, None


def _extract_company(text: str) -> str | None:
    patterns = [
        r"\b(ооо|ао|пао|ип)\s+[«\"]?([^,.;\n]{3,70})",
        r"(?:в компании|основатель|собственник|директор|руководитель)\s+([A-ZА-ЯЁ][A-Za-zА-Яа-яЁё0-9 «»\"-]{3,70})",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            value = _clean_value(" ".join(group for group in match.groups() if group))
            if value and not any(stop in value.lower() for stop in ["tenchat", "маркетинговое агентство", "рекламное агентство"]):
                return value[:120]
    return None


def _extract_legal_name(text: str) -> str | None:
    quoted = re.search(r"\b(ооо|ао|пао|ип)\s+[«\"]([^»\"]{3,140})[»\"]", text, flags=re.IGNORECASE)
    if quoted:
        form = quoted.group(1).upper()
        name = re.sub(r"\s+", " ", quoted.group(2)).strip()
        return f'{form} "{name}"'

    plain = re.search(
        r"\b(ооо|ао|пао|ип)\s+([^,.;\n]{3,120}?)(?=\s+(?:основной|дата|официальная|отзывы|—|-)|[,.;\n]|$)",
        text,
        flags=re.IGNORECASE,
    )
    if plain:
        return _clean_value(" ".join(plain.groups()))
    return None


def _extract_inn(text: str) -> str | None:
    match = INN_PATTERN.search(text)
    return match.group(1) if match else None


def _extract_ogrn(text: str) -> str | None:
    match = OGRN_PATTERN.search(text)
    return match.group(1) if match else None


def _classify_people_lead(
    full_text: str,
    *,
    person_name: str | None,
    role_title: str | None,
    company_name: str | None,
    company_inn: str | None,
    is_profile_url: bool,
    query_item: QueryItem | None,
) -> dict:
    role_text = " ".join([full_text, role_title or ""]).lower()
    role_hits = _hits(role_text, ROLE_SIGNALS)
    icp_hits = _hits(full_text, ICP_SIGNALS)
    growth_hits = _hits(full_text, GROWTH_CONTEXT_SIGNALS)
    negative_hits = _hits(full_text, NEGATIVE_SIGNALS)
    has_web_match = bool(query_item and query_item.web_lead_id)

    score = 0
    score += min(38, sum(weight for _, weight in role_hits))
    score += 24 if has_web_match else 0
    score += min(18, sum(weight for _, weight in icp_hits))
    score += min(12, sum(weight for _, weight in growth_hits))
    score += 8 if person_name else 0
    score += 6 if company_name else 0
    score += 6 if is_profile_url else 0
    score += 8 if company_inn else 0
    if query_item and query_item.web_icp_score >= 70:
        score += 10
    elif query_item and query_item.web_icp_score >= 45:
        score += 6
    score += sum(weight for _, weight in negative_hits)

    if not is_profile_url and not person_name:
        score = min(score, 35)
    if _looks_like_generic_article(full_text) and not is_profile_url:
        score = min(score, 25)
    if negative_hits and not has_web_match:
        score = min(score, 40)
    if not role_hits and not has_web_match:
        score = min(score, 42)
    score = max(0, min(100, score))

    if score >= 70:
        lead_fit = "decision_maker"
        cjm_stage = "people_outreach"
    elif score >= 45:
        lead_fit = "possible_decision_maker"
        cjm_stage = "manual_check"
    else:
        lead_fit = "weak_people_match"
        cjm_stage = "low_confidence"

    why_parts = []
    if role_hits:
        why_parts.append("роль/влияние: " + _format_hits(role_hits, 5))
    if has_web_match:
        web_label = query_item.web_title or query_item.web_domain or "web-компания"
        why_parts.append(f"найден по компании из web-проекта: {web_label}")
    if query_item and query_item.web_icp_score:
        why_parts.append(f"web ICP score: {query_item.web_icp_score}")
    if company_inn:
        why_parts.append(f"есть ИНН: {company_inn}")
    if icp_hits:
        why_parts.append("ICP-признаки: " + _format_hits(icp_hits, 6))
    if growth_hits:
        why_parts.append("контекст роста: " + _format_hits(growth_hits, 6))
    if negative_hits:
        why_parts.append("минусы: " + _format_hits(negative_hits, 4))

    likely_icp = _format_hits(icp_hits, 8) or (query_item.web_title if query_item else "нужна ручная проверка")
    pain_detected = _format_hits(growth_hits, 8) or "прямую боль не нашли, но профиль похож на ЛПР ICP"

    return {
        "lead_score": score,
        "lead_fit": lead_fit,
        "likely_icp": likely_icp,
        "pain_detected": pain_detected,
        "cjm_stage": cjm_stage,
        "why_relevant": "; ".join(why_parts) or "слабый сигнал, нужна ручная проверка",
        "outreach_angle": _build_angle(query_item=query_item, growth_hits=growth_hits),
    }


def _build_angle(*, query_item: QueryItem | None, growth_hits: list[tuple[str, int]]) -> str:
    company = (query_item.web_title or query_item.web_domain or "компании") if query_item else ""
    if query_item and query_item.web_icp_score >= 45:
        return (
            f"Зайти через контекст компании {company}: коротко проверить, кто отвечает за рост вне одной площадки, "
            "и предложить безопасную диагностику модели MP/direct без обещаний быстрых продаж."
        )
    if growth_hits:
        return "Зайти через тему роста: маркетплейсы, direct, спрос и экономика. Начать с вопроса, актуальна ли задача снижать зависимость от одной площадки."
    return "Зайти мягко как к ЛПР: уточнить, кто отвечает за рост и каналы продаж, без продажи в первом сообщении."


def _is_actionable_people_candidate(item: dict) -> bool:
    score = int(item.get("lead_score") or 0)
    has_person_context = bool(item.get("person_name") or item.get("role_title"))
    has_web_match = bool(item.get("matched_web_lead_id"))
    has_profile = bool(item.get("profile_url"))
    if score < 38:
        return False
    if has_web_match and (has_person_context or has_profile):
        return True
    if score >= 45 and has_person_context and has_profile:
        return True
    return False


def _looks_like_generic_article(text: str) -> bool:
    head = text[:800]
    return any(marker in head for marker in ARTICLE_MARKERS)


def _looks_like_person_name(value: str | None) -> bool:
    if not value:
        return False
    cleaned = _clean_value(value) or ""
    if len(cleaned) > 70:
        return False
    lowered = cleaned.lower()
    if any(marker in lowered for marker in ["реклама", "директ", "маркетплейс", "wildberries", "ozon", "как ", "способ"]):
        return False
    parts = [part for part in re.split(r"\s+", cleaned) if part]
    if not 2 <= len(parts) <= 3:
        return False
    return all(re.match(r"^[А-ЯЁA-Z][А-Яа-яЁёA-Za-z-]{1,30}$", part) for part in parts)


def _build_opener(person_name: str | None, role_title: str | None, company_name: str | None, item: dict) -> str:
    greeting = f"{person_name.split()[0]}, добрый день." if person_name else "Добрый день."
    company_part = f" по {company_name}" if company_name else ""
    role_part = f" Вижу, что вы близко к роли {role_title.lower()}." if role_title else ""
    return (
        f"{greeting}{role_part} Заметил контекст{company_part} и аккуратно проверяю гипотезу: "
        "у брендов и производителей рост часто упирается не в одну настройку рекламы, а в предел модели MP/direct. "
        "Подскажите, у вас сейчас есть задача снижать зависимость от одной площадки или развивать спрос вне текущего канала?"
    )


TENCHAT_NON_PROFILE_PATHS = {
    "about",
    "api",
    "blog",
    "career",
    "companies",
    "contacts",
    "documents",
    "feed",
    "jobs",
    "media",
    "people",
    "premium",
    "resume",
    "search",
    "static",
    "support",
    "tags",
    "vacancies",
}


def _first_tenchat_path_part(url: str | None) -> tuple[str, str] | None:
    if not url:
        return None
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    if not host.endswith("tenchat.ru"):
        return None
    path = parsed.path.strip("/")
    if not path:
        return "", ""
    first = path.split("/", 1)[0]
    return first, path


def _is_tenchat_company_url(url: str | None) -> bool:
    path_info = _first_tenchat_path_part(url)
    if not path_info:
        return False
    first, path = path_info
    return path == first and bool(re.fullmatch(r"\d{10,15}", first))


def _filter_people_candidates(candidates: list[dict]) -> list[dict]:
    result: list[dict] = []
    seen_profiles: set[str] = set()
    for candidate in candidates:
        url = normalize_url(candidate.get("url")) or str(candidate.get("url") or "")
        profile_url, _post_url = _split_tenchat_url(url)
        if not profile_url:
            continue
        profile_key = profile_url.lower().rstrip("/")
        if profile_key in seen_profiles:
            continue
        seen_profiles.add(profile_key)
        item = dict(candidate)
        item["original_url"] = url
        item["url"] = profile_url
        result.append(item)
    return result


def _split_tenchat_url(url: str) -> tuple[str | None, str | None]:
    parsed = urlparse(url)
    if not parsed.netloc:
        return None, None
    path = parsed.path.strip("/")
    if not path:
        return None, None
    first = path.split("/", 1)[0]
    profile_url = f"{parsed.scheme or 'https'}://{parsed.netloc}/{first}" if _is_tenchat_profile_path(first) else None
    post_url = url if not profile_url or path != first else None
    return profile_url, post_url


def _is_tenchat_profile_url(url: str | None) -> bool:
    path_info = _first_tenchat_path_part(url)
    if not path_info:
        return False
    first, path = path_info
    return path == first and _is_tenchat_profile_path(first)


def _is_tenchat_profile_path(first: str) -> bool:
    if not first:
        return False
    lowered = first.lower()
    if lowered in TENCHAT_NON_PROFILE_PATHS:
        return False
    if re.fullmatch(r"\d{10,15}", first):
        return False
    if re.fullmatch(r"\d{4,9}", first):
        return True
    return bool(re.fullmatch(r"[A-Za-z0-9_.-]{3,50}", first))


def _brand_from_domain(domain: str | None) -> str | None:
    if not domain:
        return None
    host = (urlparse(domain if "://" in domain else f"https://{domain}").netloc or domain).lower()
    host = host[4:] if host.startswith("www.") else host
    root = host.split(".")[0]
    return root if len(root) >= 3 else None


def _clean_company_query(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = re.sub(r"\s+", " ", str(value)).strip()
    cleaned = re.sub(r"\b(ооо|ао|пао|зао|ип)\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = cleaned.strip(" -—|,.;:«»\"'")
    cleaned = re.sub(r"\s+", " ", cleaned)
    if len(cleaned) < 3 or len(cleaned) > 70:
        return None
    if any(stop in cleaned.lower() for stop in ["интернет-магазин", "официальный сайт", "купить", "каталог"]):
        return None
    return cleaned


def _hits(text: str, signals: dict[str, int]) -> list[tuple[str, int]]:
    return [(signal, weight) for signal, weight in signals.items() if signal in text]


def _format_hits(items: list[tuple[str, int]], limit: int = 8) -> str:
    return ", ".join(signal for signal, _ in items[:limit])


def _clean_value(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = re.sub(r"\s+", " ", value).strip(" -—|,.;:«»\"'")
    return cleaned or None


def _clean_person_name(value: str | None) -> str | None:
    cleaned = _clean_value(value)
    if not cleaned:
        return None
    cleaned = cleaned.split(",", 1)[0]
    parts = cleaned.split()
    if len(parts) >= 4 and parts[:2] == parts[2:4]:
        parts = parts[:2] + parts[4:]
    return " ".join(parts[:3]) or None
