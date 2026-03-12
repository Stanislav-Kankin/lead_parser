from urllib.parse import urlparse

from ddgs import DDGS


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


async def search_domains_multi(queries: list[str], per_query_limit: int = 12, total_limit: int = 35) -> list[dict]:
    collected = []
    seen_domains = set()

    with DDGS(timeout=20, verify=False) as ddgs:
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
                    "url": href,
                    "source": "ddgs",
                    "source_query": query,
                })

                if len(collected) >= total_limit:
                    return collected

    return collected
