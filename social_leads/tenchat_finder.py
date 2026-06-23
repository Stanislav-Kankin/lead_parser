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

TENCHAT_SEARCH_PRESETS = {
    "ad_demand": {
        "label": "Прямой спрос на рекламу",
        "queries": [
            'site:tenchat.ru "ищу агентство" "яндекс директ"',
            'site:tenchat.ru "нужен подрядчик" "реклама" "интернет-магазин"',
            'site:tenchat.ru "посоветуйте директолога" "интернет-магазин"',
            'site:tenchat.ru "кто настраивает яндекс директ" "бренд"',
            'site:tenchat.ru "запустить рекламу" "производитель"',
            'site:tenchat.ru "ищем специалиста" "яндекс директ" "бренд"',
            'site:tenchat.ru "нужен маркетолог" "wildberries" "ozon"',
            'site:tenchat.ru "подрядчик по performance" "бренд"',
        ],
    },
    "mp_external_traffic": {
        "label": "Внешний трафик на MP",
        "queries": [
            'site:tenchat.ru "внешний трафик" "wildberries" "ozon"',
            'site:tenchat.ru "трафик на маркетплейсы" "бренд"',
            'site:tenchat.ru "реклама на wildberries" "производитель"',
            'site:tenchat.ru "реклама на ozon" "бренд"',
            'site:tenchat.ru "промостраницы" "маркетплейсы"',
        ],
    },
    "direct_ecom": {
        "label": "Директ / РСЯ для ecom",
        "queries": [
            'site:tenchat.ru "яндекс директ" "интернет-магазин"',
            'site:tenchat.ru "рся" "интернет-магазин"',
            'site:tenchat.ru "директ не окупается" "интернет-магазин"',
            'site:tenchat.ru "дорогие заявки" "яндекс директ"',
            'site:tenchat.ru "реклама на сайт" "производитель"',
        ],
    },
    "growth_pain": {
        "label": "Боль экономики роста",
        "queries": [
            'site:tenchat.ru "растет дрр" "wildberries"',
            'site:tenchat.ru "растет cac" "бренд"',
            'site:tenchat.ru "комиссии маркетплейсов" "маржа"',
            'site:tenchat.ru "маркетплейсы не масштабируются"',
            'site:tenchat.ru "сайт не продает" "бренд"',
        ],
    },
}

DEFAULT_TENCHAT_PRESET = "ad_demand"

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

DEMAND_INTENT_SIGNALS = {
    "ищу": 14,
    "ищем": 14,
    "нужен": 14,
    "нужна": 14,
    "нужно": 10,
    "посоветуйте": 16,
    "порекомендуйте": 16,
    "кто умеет": 18,
    "кто может": 14,
    "подрядчик": 14,
    "агентство": 12,
    "директолог": 16,
    "специалист": 10,
    "настроить": 10,
    "запустить рекламу": 18,
    "вести рекламу": 14,
    "помогите": 12,
}

AD_CHANNEL_SIGNALS = {
    "яндекс директ": 18,
    "директ": 12,
    "рся": 14,
    "промостраницы": 14,
    "внешний трафик": 16,
    "трафик на маркетплейсы": 16,
    "performance": 12,
    "перфоманс": 12,
    "реклама": 10,
    "таргет": 8,
    "лиды": 8,
}

NEGATIVE_SIGNALS = {
    "маркетинговое агентство": -25,
    "мы агентство": -30,
    "наше агентство": -30,
    "рекламное агентство": -25,
    "оказываем услуги": -25,
    "наши услуги": -20,
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


def build_people_queries(custom_queries: str | None = None, preset: str = DEFAULT_TENCHAT_PRESET) -> list[str]:
    lines = [line.strip() for line in (custom_queries or "").splitlines() if line.strip()]
    base_queries = TENCHAT_SEARCH_PRESETS.get(preset, TENCHAT_SEARCH_PRESETS[DEFAULT_TENCHAT_PRESET])["queries"]
    if not lines:
        return list(base_queries)
    result = []
    for line in lines:
        result.append(line if "site:" in line else f"site:tenchat.ru {line}")
    return list(dict.fromkeys([*result, *base_queries]))


async def collect_people_leads(
    *,
    custom_queries: str | None = None,
    preset: str = DEFAULT_TENCHAT_PRESET,
    total_limit: int = 40,
    per_query_limit: int = 8,
    concurrency: int = 6,
) -> dict:
    queries = build_people_queries(custom_queries, preset=preset)
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
        "preset": preset,
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
    intent_hits = _hits(full_text, DEMAND_INTENT_SIGNALS)
    channel_hits = _hits(full_text, AD_CHANNEL_SIGNALS)
    negative_hits = _hits(full_text, NEGATIVE_SIGNALS)

    score = (
        min(28, sum(weight for _, weight in intent_hits))
        + min(24, sum(weight for _, weight in channel_hits))
        + min(18, sum(weight for _, weight in icp_hits))
        + min(14, sum(weight for _, weight in pain_hits))
        + min(12, sum(weight for _, weight in role_hits))
        + sum(weight for _, weight in negative_hits)
    )
    if person_name:
        score += 5
    if role_title:
        score += 5
    if company_name:
        score += 6
    if icp_hits and pain_hits:
        score += 5
    if intent_hits and channel_hits:
        score += 12
    if channel_hits and icp_hits:
        score += 8
    if role_hits and (intent_hits or icp_hits):
        score += 5
    direct_demand = bool(intent_hits and channel_hits)
    strategic_demand = bool(channel_hits and icp_hits and pain_hits)
    if not person_name and not role_title:
        score = min(score, 42)
    if not person_name and not role_title and not company_name:
        score = min(score, 30)
    if not direct_demand and not strategic_demand:
        score = min(score, 34)
    if not icp_hits:
        score -= 6
    if _looks_like_generic_article(full_text) and not person_name:
        score -= 18
    score = max(0, min(100, score))

    if score >= 70 and direct_demand:
        lead_fit = "hot_ad_demand"
        cjm_stage = "direct_request"
    elif score >= 45 and (direct_demand or strategic_demand):
        lead_fit = "warm_ad_demand"
        cjm_stage = "channel_search"
    else:
        lead_fit = "weak_signal"
        cjm_stage = "signal_only"

    pain_parts = []
    if intent_hits:
        pain_parts.append("намерение: " + _format_hits(intent_hits, 5))
    if channel_hits:
        pain_parts.append("канал: " + _format_hits(channel_hits, 5))
    if pain_hits:
        pain_parts.append("боль: " + _format_hits(pain_hits, 5))
    pain_detected = "; ".join(pain_parts) or "явный рекламный запрос не найден"
    likely_icp = _format_hits(icp_hits, 8) or "нужно проверить вручную"
    why_parts = []
    if role_hits:
        why_parts.append("роль/влияние: " + _format_hits(role_hits, 5))
    if intent_hits:
        why_parts.append("прямой спрос: " + _format_hits(intent_hits, 5))
    if channel_hits:
        why_parts.append("канал рекламы: " + _format_hits(channel_hits, 5))
    if icp_hits:
        why_parts.append("ICP: " + _format_hits(icp_hits, 6))
    if pain_hits:
        why_parts.append("боль/триггер: " + _format_hits(pain_hits, 6))
    if negative_hits:
        why_parts.append("минусы: " + _format_hits(negative_hits, 4))
    why_relevant = "; ".join(why_parts) or "слабый сигнал, нужна ручная проверка"
    outreach_angle = _build_angle(pain_hits, icp_hits, intent_hits, channel_hits)

    return {
        "lead_score": score,
        "lead_fit": lead_fit,
        "likely_icp": likely_icp,
        "pain_detected": pain_detected,
        "cjm_stage": cjm_stage,
        "why_relevant": why_relevant,
        "outreach_angle": outreach_angle,
    }


def _build_angle(
    pain_hits: list[tuple[str, int]],
    icp_hits: list[tuple[str, int]],
    intent_hits: list[tuple[str, int]] | None = None,
    channel_hits: list[tuple[str, int]] | None = None,
) -> str:
    pain = _format_hits(pain_hits, 4)
    icp = _format_hits(icp_hits, 4)
    intent = _format_hits(intent_hits or [], 4)
    channel = _format_hits(channel_hits or [], 4)
    if intent and channel:
        return "Зайти через прямой запрос: коротко уточнить задачу, канал, текущую экономику и предложить безопасную диагностику без обещания быстрых продаж."
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
    lead_fit = str(item.get("lead_fit") or "")
    is_ad_demand = lead_fit in {"hot_ad_demand", "warm_ad_demand"}
    if has_person and is_ad_demand and score >= 38:
        return True
    if has_company and is_ad_demand and score >= 45:
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
    if item.get("lead_fit") in {"hot_ad_demand", "warm_ad_demand"}:
        return (
            f"{greeting}{role_hint} Увидел, что у вас всплыла задача по рекламе: {item.get('pain_detected')}. "
            "Чтобы не предлагать абстрактный Директ, я бы сначала посмотрел связку: продукт, площадки, сайт/direct и экономика первого заказа. "
            "Если актуально, могу коротко подсказать, где обычно быстро видно, есть ли смысл тестировать канал."
        )
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
