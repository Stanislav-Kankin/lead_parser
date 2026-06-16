from __future__ import annotations

import asyncio
import logging
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from ddgs import DDGS

logger = logging.getLogger(__name__)


def normalize_url(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url)
    if not parsed.netloc:
        return None

    query_pairs = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=False)
        if not key.lower().startswith("utm_") and key.lower() not in {"yclid", "gclid"}
    ]
    query = urlencode(query_pairs, doseq=True)
    path = parsed.path.rstrip("/") or "/"
    return urlunparse((parsed.scheme or "https", parsed.netloc.lower(), path, "", query, ""))


def _host_allowed(url: str, allowed_domains: set[str] | None) -> bool:
    if not allowed_domains:
        return True
    host = (urlparse(url).netloc or "").lower()
    host = host[4:] if host.startswith("www.") else host
    return any(host == domain or host.endswith("." + domain) for domain in allowed_domains)


def _search_one_sync(query: str, per_query_limit: int = 10, allowed_domains: set[str] | None = None) -> list[dict]:
    logger.info("[url_search] query=%r", query)
    try:
        with DDGS(timeout=10, verify=False) as ddgs:
            results = ddgs.text(
                query,
                region="ru-ru",
                safesearch="off",
                backend="auto",
                max_results=per_query_limit,
            )
    except Exception as exc:
        logger.warning("[url_search] query_failed query=%r error=%s", query, exc)
        return []

    items: list[dict] = []
    for item in results or []:
        href = normalize_url(item.get("href"))
        if not href or not _host_allowed(href, allowed_domains):
            continue
        items.append(
            {
                "url": href,
                "title": item.get("title") or "",
                "body": item.get("body") or "",
                "source": "ddgs",
                "source_query": query,
            }
        )
    logger.info("[url_search] query_done query=%r raw=%s kept=%s", query, len(results or []), len(items))
    return items


async def _search_one(
    query: str,
    per_query_limit: int,
    allowed_domains: set[str] | None,
    semaphore: asyncio.Semaphore,
) -> list[dict]:
    async with semaphore:
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(_search_one_sync, query, per_query_limit, allowed_domains),
                timeout=22,
            )
        except asyncio.TimeoutError:
            logger.warning("[url_search] query_timeout query=%r", query)
            return []


async def search_urls_multi(
    queries: list[str],
    *,
    per_query_limit: int = 10,
    total_limit: int = 40,
    allowed_domains: set[str] | None = None,
) -> list[dict]:
    logger.info(
        "[url_search] start queries=%s per_query_limit=%s total_limit=%s allowed=%s",
        len(queries),
        per_query_limit,
        total_limit,
        sorted(allowed_domains or []),
    )
    semaphore = asyncio.Semaphore(2)
    batches = await asyncio.gather(
        *(_search_one(query, per_query_limit, allowed_domains, semaphore) for query in queries)
    )

    collected: list[dict] = []
    seen_urls: set[str] = set()
    for batch in batches:
        for item in batch:
            url = item.get("url")
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            collected.append(item)
            if len(collected) >= total_limit:
                return collected
    return collected
