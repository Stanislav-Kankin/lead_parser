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


def _search_sync(queries: list[str], per_query_limit: int = 10, total_limit: int = 25) -> list[dict]:
    collected = []
    seen_domains = set()

    logger.info("[domain_search] start queries=%s per_query_limit=%s total_limit=%s", len(queries), per_query_limit, total_limit)

    with DDGS(timeout=10, verify=False) as ddgs:
        for query in queries:
            logger.info("[domain_search] query=%r", query)
            try:
                results = ddgs.text(
                    query,
                    region="ru-ru",
                    safesearch="off",
                    backend="auto",
                    max_results=per_query_limit,
                )
            except Exception as exc:
                logger.warning("[domain_search] query_failed query=%r error=%s", query, exc)
                continue

            query_added = 0
            for item in results or []:
                href = item.get("href")
                domain = normalize_domain(href)
                if not domain or is_bad_domain(domain) or domain in seen_domains:
                    continue

                seen_domains.add(domain)
                collected.append({
                    "company_name": item.get("title"),
                    "domain": domain,
                    "url": href,
                    "source": "ddgs",
                    "source_query": query,
                })
                query_added += 1

                if len(collected) >= total_limit:
                    logger.info("[domain_search] reached_limit collected=%s", len(collected))
                    return collected

            logger.info("[domain_search] query_done query=%r added=%s total=%s", query, query_added, len(collected))

    logger.info("[domain_search] done total=%s", len(collected))
    return collected


async def search_domains_multi(queries: list[str], per_query_limit: int = 10, total_limit: int = 25) -> list[dict]:
    return await asyncio.wait_for(
        asyncio.to_thread(_search_sync, queries, per_query_limit, total_limit),
        timeout=30,
    )
