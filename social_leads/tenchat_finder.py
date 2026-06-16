from __future__ import annotations

import asyncio
import logging
import re
from html import unescape
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from sources.url_search import normalize_url, search_urls_multi
from storage.social_lead_repository import save_social_leads

logger = logging.getLogger(__name__)

DEFAULT_TENCHAT_QUERIES = [
    'site:tenchat.ru "собственник" "маркетплейс"',
    'site:tenchat.ru "основатель" "Wildberries" "Ozon"',
    'site:tenchat.ru "директор по маркетингу" "маркетплейсы"',
    'site:tenchat.ru "производитель" "официальный сайт" "TenChat"',
    'site:tenchat.ru "российский бренд" "маркетплейсы"',
    'site:tenchat.ru "собственное производство" "бренд"',
    'site:tenchat.ru "комиссии маркетплейсов" "бренд"',
    'site:tenchat.ru "маржа" "Wildberries" "Ozon"',
    'site:tenchat.ru "внешний трафик" "маркетплейсы"',
    'site:tenchat.ru "direct" "бренд" "маркетплейсы"',
]

ROLE_SIGNALS = {
    "собственник": 20,
    "основатель": 20,
    "фаундер": 18,
    "ceo": 18,
    "генеральный директор": 17,
    "директор": 12,
    "коммерческий директор": 16,
    "директор по маркетингу": 16,
    "head of marketing": 16,
    "маркетолог": 8,
    "e-commerce": 12,
    "ecommerce": 12,
    "руководитель": 10,
}

ICP_SIGNALS = {
    "производитель": 16,
    "производство": 14,
    "собственное производство": 20,
    "бренд": 14,
    "российский бренд": 18,
    "товары": 8,
    "косметика": 10,
    "одежда": 8,
    "обувь": 8,
    "товары для дома": 10,
    "бытовая химия": 10,
    "продукты питания": 10,
    "fmcg": 10,
    "интернет-магазин": 8,
    "официальный сайт": 7,
}

PAIN_SIGNALS = {
    "маркетплейс": 9,
    "маркетплейсы": 9,
    "wildberries": 10,
    "wb": 8,
    "ozon": 10,
    "комиссии": 10,
    "маржа": 10,
    "ставки": 8,
    "аукцион": 9,
    "cac": 10,
    "дрр": 10,
    "direct": 7,
    "внешний трафик": 10,
    "рост продаж": 7,
    "масштабирование": 8,
    "спрос": 7,
    "брендовый спрос": 10,
    "повторные продажи": 8,
    "потолок": 12,
}

NEGATIVE_SIGNALS = {
    "агентство": -20,
    "маркетинговое агентство": -25,
    "smm": -15,
    "таргетолог": -18,
    "seo": -16,
    "настройка рекламы": -18,
    "продвижение сайтов": -18,
    "фрилансер": -16,
    "консультант": -10,
    "коуч": -12,
    "hr": -10,
    "вакансии": -12,
    "ищу работу": -18,
}


def build_people_queries(custom_queries: str | None = None) -> list[str]:
    lines = [line.strip() for line in (custom_queries or "").splitlines() if line.strip()]
    if not lines:
        return DEFAULT_TENCHAT_QUERIES
    result = []
    for line in lines:
        result.append(line if "site:" in line else f"site:tenchat.ru {line}")
    return result


async def collect_people_leads(
    *,
    custom_queries: str | None = None,
    total_limit: int = 40,
    per_query_limit: int = 8,
    concurrency: int = 6,
) -> dict:
    queries = build_people_queries(custom_queries)
    candidates = await search_urls_multi(
        queries,
        per_query_limit=per_query_limit,
        total_limit=total_limit,
        allowed_domains={"tenchat.ru"},
    )

    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def enrich(candidate: dict) -> dict | None:
        async with semaphore:
            page = await _fetch_public_page(candidate.get("url"))
            item = _build_social_lead(candidate, page)
            if not _is_actionable_people_candidate(item):
                return None
            if int(item.get("lead_score") or 0) < 38:
                return None
            return item

    enriched = [item for item in await asyncio.gather(*(enrich(candidate) for candidate in candidates)) if item]
    save_stats = save_social_leads(enriched)
    return {
        "queries": len(queries),
        "candidates": len(candidates),
        "analyzed": len(candidates),
        "kept": len(enriched),
        "created": save_stats.get("created", 0),
        "updated": save_stats.get("updated", 0),
        "skipped": save_stats.get("skipped", 0),
    }


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

    soup = BeautifulSoup(response.text, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    title = (soup.title.string if soup.title and soup.title.string else "")[:300]
    h1 = " ".join(tag.get_text(" ", strip=True) for tag in soup.find_all("h1")[:2])
    text = " ".join(soup.get_text(" ", strip=True).split())
    return {"title": unescape(title), "h1": h1, "text": unescape(text[:18000])}


def _build_social_lead(candidate: dict, page: dict) -> dict:
    url = normalize_url(candidate.get("url")) or candidate.get("url") or ""
    title = page.get("title") or candidate.get("title") or ""
    snippet = candidate.get("body") or ""
    text = page.get("text") or " ".join([title, snippet])
    full_text = " ".join([title, page.get("h1") or "", snippet, text]).lower()
    person_name, role_title = _parse_person_and_role(title, page.get("h1") or "")
    company_name = _extract_company(full_text)
    classification = _classify_people_lead(full_text, person_name=person_name, role_title=role_title, company_name=company_name)
    profile_url, post_url = _split_tenchat_url(url)
    opener = _build_opener(person_name, role_title, classification)

    return {
        "source": "tenchat",
        "source_url": url,
        "source_query": candidate.get("source_query"),
        "profile_url": profile_url,
        "post_url": post_url,
        "person_name": person_name,
        "role_title": role_title,
        "company_name": company_name,
        "title": title or candidate.get("title"),
        "snippet": snippet,
        "text": text[:12000],
        "opener": opener,
        **classification,
    }


def _parse_person_and_role(title: str, h1: str) -> tuple[str | None, str | None]:
    source = " ".join(part for part in [h1, title] if part).strip()
    source = re.sub(r"\s+", " ", source)
    source = re.sub(r"\s*\|\s*TenChat.*$", "", source, flags=re.IGNORECASE)
    source = re.sub(r"\s*—\s*TenChat.*$", "", source, flags=re.IGNORECASE)

    patterns = [
        r"^(?P<name>[А-ЯЁA-Z][^—|,]{3,80}),?\s*\d{0,2}\s*(?:лет|года|год)?\s*[—-]\s*(?P<role>[^|]{3,180})",
        r"^(?P<name>[А-ЯЁA-Z][^—|]{3,80})\s*[—-]\s*(?P<role>[^|]{3,180})",
    ]
    for pattern in patterns:
        match = re.search(pattern, source)
        if match:
            return _clean_value(match.group("name")), _clean_value(match.group("role"))
    if h1 and len(h1) < 90:
        return _clean_value(h1), None
    return None, None


def _extract_company(text: str) -> str | None:
    patterns = [
        r"(?:в|компания|бренд|производитель)\s+(ооо\s+[«\"]?[^,.;\n]{3,70})",
        r"(?:в|компания|бренд|производитель)\s+([a-zа-яё0-9][^,.;\n]{3,70})",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            value = _clean_value(match.group(1))
            if value and not any(stop in value.lower() for stop in ["tenchat", "маркетинг", "агентство"]):
                return value[:120]
    return None


def _classify_people_lead(
    full_text: str,
    *,
    person_name: str | None,
    role_title: str | None,
    company_name: str | None,
) -> dict:
    role_hits = _hits(full_text, ROLE_SIGNALS)
    icp_hits = _hits(full_text, ICP_SIGNALS)
    pain_hits = _hits(full_text, PAIN_SIGNALS)
    negative_hits = _hits(full_text, NEGATIVE_SIGNALS)

    score = (
        min(30, sum(weight for _, weight in role_hits))
        + min(30, sum(weight for _, weight in icp_hits))
        + min(30, sum(weight for _, weight in pain_hits))
        + sum(weight for _, weight in negative_hits)
    )
    if person_name:
        score += 5
    if role_title:
        score += 5
    if company_name:
        score += 6
    if icp_hits and pain_hits:
        score += 8
    if role_hits and icp_hits:
        score += 7
    if not person_name and not role_title:
        score = min(score, 42)
    if not person_name and not role_title and not company_name:
        score = min(score, 30)
    if not role_hits:
        score -= 10
    if not icp_hits:
        score -= 8
    if _looks_like_generic_article(full_text) and not person_name:
        score -= 18
    score = max(0, min(100, score))

    if score >= 70:
        lead_fit = "hot_people_icp"
        cjm_stage = "consideration"
    elif score >= 45:
        lead_fit = "warm_people_icp"
        cjm_stage = "awareness"
    else:
        lead_fit = "weak_signal"
        cjm_stage = "signal_only"

    pain_detected = _format_hits(pain_hits, 8) or "явная боль не найдена, есть косвенный ICP-сигнал"
    likely_icp = _format_hits(icp_hits, 8) or "нужно проверить вручную"
    why_parts = []
    if role_hits:
        why_parts.append("роль/влияние: " + _format_hits(role_hits, 5))
    if icp_hits:
        why_parts.append("ICP: " + _format_hits(icp_hits, 6))
    if pain_hits:
        why_parts.append("боль/триггер: " + _format_hits(pain_hits, 6))
    if negative_hits:
        why_parts.append("минусы: " + _format_hits(negative_hits, 4))
    why_relevant = "; ".join(why_parts) or "слабый сигнал, нужна ручная проверка"
    outreach_angle = _build_angle(pain_hits, icp_hits)

    return {
        "lead_score": score,
        "lead_fit": lead_fit,
        "likely_icp": likely_icp,
        "pain_detected": pain_detected,
        "cjm_stage": cjm_stage,
        "why_relevant": why_relevant,
        "outreach_angle": outreach_angle,
    }


def _build_angle(pain_hits: list[tuple[str, int]], icp_hits: list[tuple[str, int]]) -> str:
    pain = _format_hits(pain_hits, 4)
    icp = _format_hits(icp_hits, 4)
    if "внешний трафик" in pain or "direct" in pain:
        return "Зайти через безопасный тест direct/внешнего спроса без замены текущих каналов."
    if "комиссии" in pain or "маржа" in pain or "аукцион" in pain:
        return "Зайти через экономику роста: как снизить давление маркетплейс-аукциона и не ломать текущую модель."
    if "потолок" in pain or "масштабирование" in pain:
        return "Зайти через потолок текущей модели и рамку контролируемого эксперимента."
    if icp:
        return "Мягко проверить, есть ли задача роста вне одной площадки и контроля спроса."
    return "Начать с короткого диагностического вопроса без продажи."


def _is_actionable_people_candidate(item: dict) -> bool:
    has_person = bool(item.get("person_name") or item.get("role_title"))
    has_company = bool(item.get("company_name"))
    score = int(item.get("lead_score") or 0)
    if has_person and score >= 38:
        return True
    if has_company and score >= 45:
        return True
    return False


def _looks_like_generic_article(text: str) -> bool:
    article_markers = [
        "как ",
        "почему ",
        "что такое",
        "гид ",
        "инструкция",
        "чек-лист",
        "новости",
        "разбор",
        "кейс",
    ]
    return any(marker in text[:700] for marker in article_markers)


def _build_opener(person_name: str | None, role_title: str | None, item: dict) -> str:
    greeting = f"{person_name.split()[0]}, добрый день." if person_name else "Добрый день."
    role_hint = f" По профилю вижу, что вы близко к теме {role_title.lower()}." if role_title else ""
    return (
        f"{greeting}{role_hint} Зацепился за контекст вокруг {item.get('pain_detected')}. "
        "У брендов и производителей в такой точке часто вопрос не в том, чтобы резко менять каналы, "
        "а в том, где уже потолок текущей модели и какой следующий шаг можно проверить без риска. "
        "Интересно, вы сейчас это для себя как формулируете?"
    )


def _split_tenchat_url(url: str) -> tuple[str | None, str | None]:
    parsed = urlparse(url)
    if not parsed.netloc:
        return None, None
    path = parsed.path.strip("/")
    if not path:
        return url, None
    first = path.split("/", 1)[0]
    profile_url = f"{parsed.scheme or 'https'}://{parsed.netloc}/{first}"
    post_url = url if path and ("?" in url or re.search(r"\d", first)) else None
    return profile_url, post_url


def _hits(text: str, signals: dict[str, int]) -> list[tuple[str, int]]:
    return [(signal, weight) for signal, weight in signals.items() if signal in text]


def _format_hits(items: list[tuple[str, int]], limit: int = 8) -> str:
    return ", ".join(signal for signal, _ in items[:limit])


def _clean_value(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = re.sub(r"\s+", " ", value).strip(" -—|,.;:«»\"'")
    return cleaned or None
