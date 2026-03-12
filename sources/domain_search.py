import asyncio

from ddgs import DDGS

from utils.domain_normalizer import normalize_domain

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
    "pulscen.ru",
    "satu.kz",
    "tiu.ru",
    "prom.ua",
    "deal.by",
    "flagma.ru",
}

BAD_DOMAIN_PARTS = (
    "market",
    "marketplace",
    "catalog",
    "forum",
    "wiki",
    "youtube",
    "reviews",
)


def is_bad_domain(domain: str) -> bool:
    if any(domain == bad or domain.endswith("." + bad) for bad in BAD_DOMAINS):
        return True
    return any(part in domain for part in BAD_DOMAIN_PARTS)


def _search_sync(queries: list[str], per_query_limit: int = 10, total_limit: int = 25) -> list[dict]:
    collected: list[dict] = []
    seen_domains: set[str] = set()

    with DDGS(timeout=15, verify=False) as ddgs:
        for query in queries:
            try:
                results = ddgs.text(
                    query,
                    region="ru-ru",
                    safesearch="off",
                    backend="auto",
                    max_results=per_query_limit,
                )
            except Exception:
                continue

            for item in results or []:
                href = item.get("href")
                domain = normalize_domain(href)
                if not domain or is_bad_domain(domain) or domain in seen_domains:
                    continue

                seen_domains.add(domain)
                collected.append({
                    "company_name": item.get("title"),
                    "domain": domain,
                    "domain_normalized": domain,
                    "url": href,
                    "source": "ddgs",
                    "source_query": query,
                    "snippet": item.get("body"),
                })

                if len(collected) >= total_limit:
                    return collected

    return collected


async def search_domains_multi(queries: list[str], per_query_limit: int = 10, total_limit: int = 25) -> list[dict]:
    return await asyncio.wait_for(
        asyncio.to_thread(_search_sync, queries, per_query_limit, total_limit),
        timeout=40,
    )
