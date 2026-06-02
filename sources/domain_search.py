import asyncio
import logging
from urllib.parse import urlparse

from ddgs import DDGS

logger = logging.getLogger(__name__)

BAD_DOMAINS = {
    "youtube.com",
    "vk.com",
    "dzen.ru",
    "yandex.ru",
    "google.com",
    "2gis.ru",
    "avito.ru",
    "t.me",
    "telegram.me",
    "instagram.com",
    "facebook.com",
    "market.yandex.ru",
    "ozon.ru",
    "wildberries.ru",
    "duckduckgo.com",
    "dns-shop.ru",
    "citilink.ru",
    "eldorado.ru",
    "mvideo.ru",
    "aliexpress.com",
    "aliexpress.ru",
    "amazon.com",
    "irecommend.ru",
    "otzovik.com",
    "wikipedia.org",
    "wiktionary.org",
    "youtube.ru",
    "hh.ru",
    "superjob.ru",
    "rabota.ru",
    "vc.ru",
    "habr.com",
    "spark-interfax.ru",
    "rusprofile.ru",
    "list-org.com",
    "sbis.ru",
    "zachestnyibiznes.ru",
    "tiu.ru",
    "pulscen.ru",
    "blizko.ru",
    "all.biz",
    "insales.ru",
    "nethouse.ru",
    "tilda.cc",
    "tilda.ws",
    "umi.ru",
    "ecwid.com",
    "u11.ru",
    "volvo-club.by",
}


def normalize_domain(url: str) -> str | None:
    if not url:
        return None

    parsed = urlparse(url)
    host = (parsed.netloc or "").lower().strip()
    if not host and parsed.path:
        host = parsed.path.lower().strip()

    if host.startswith("www."):
        host = host[4:]

    return host or None


def is_bad_domain(domain: str) -> bool:
    return any(domain == bad or domain.endswith("." + bad) for bad in BAD_DOMAINS)


def _search_one_sync(query: str, per_query_limit: int = 10) -> list[dict]:
    logger.info("[domain_search] query=%r", query)
    try:
        with DDGS(timeout=8, verify=False) as ddgs:
            results = ddgs.text(
                query,
                region="ru-ru",
                safesearch="off",
                backend="auto",
                max_results=per_query_limit,
            )
    except Exception as exc:
        logger.warning("[domain_search] query_failed query=%r error=%s", query, exc)
        return []

    items = []
    for item in results or []:
        href = item.get("href")
        domain = normalize_domain(href)
        if not domain or is_bad_domain(domain):
            continue
        items.append(
            {
                "company_name": item.get("title"),
                "domain": domain,
                "url": href,
                "source": "ddgs",
                "source_query": query,
            }
        )
    logger.info("[domain_search] query_done query=%r raw=%s kept=%s", query, len(results or []), len(items))
    return items


async def _search_one(query: str, per_query_limit: int, semaphore: asyncio.Semaphore) -> list[dict]:
    async with semaphore:
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(_search_one_sync, query, per_query_limit),
                timeout=18,
            )
        except asyncio.TimeoutError:
            logger.warning("[domain_search] query_timeout query=%r", query)
            return []


async def search_domains_multi(queries: list[str], per_query_limit: int = 10, total_limit: int = 25) -> list[dict]:
    logger.info("[domain_search] start queries=%s per_query_limit=%s total_limit=%s", len(queries), per_query_limit, total_limit)
    semaphore = asyncio.Semaphore(2)
    batches = await asyncio.gather(*(_search_one(query, per_query_limit, semaphore) for query in queries))

    collected = []
    seen_domains = set()
    for batch in batches:
        for item in batch:
            domain = item.get("domain")
            if not domain or domain in seen_domains:
                continue
            seen_domains.add(domain)
            collected.append(item)
            if len(collected) >= total_limit:
                logger.info("[domain_search] reached_limit collected=%s", len(collected))
                return collected

    logger.info("[domain_search] done total=%s", len(collected))
    return collected
