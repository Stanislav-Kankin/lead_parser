from urllib.parse import quote, urlparse

import httpx
from bs4 import BeautifulSoup


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
}


def normalize_domain(url: str) -> str | None:
    if not url:
        return None

    parsed = urlparse(url)
    host = parsed.netloc.lower().strip()
    if not host and parsed.path:
        host = parsed.path.lower().strip()

    if not host:
        return None

    if host.startswith("www."):
        host = host[4:]

    return host


def is_bad_domain(domain: str) -> bool:
    return any(domain == bad or domain.endswith("." + bad) for bad in BAD_DOMAINS)


async def search_domains(query: str, limit: int = 10) -> list[dict]:
    url = f"https://html.duckduckgo.com/html/?q={quote(query)}"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        )
    }

    async with httpx.AsyncClient(timeout=20, follow_redirects=True, headers=headers) as client:
        response = await client.get(url)
        response.raise_for_status()

    soup = BeautifulSoup(response.text, "lxml")
    results = []
    seen = set()

    for a in soup.select("a.result__a"):
        href = a.get("href")
        domain = normalize_domain(href)
        if not domain or is_bad_domain(domain) or domain in seen:
            continue

        seen.add(domain)
        results.append({
            "company_name": a.get_text(strip=True),
            "domain": domain,
            "url": href,
            "source": "ddg",
        })

        if len(results) >= limit:
            break

    return results
