import re
from typing import Any
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
PHONE_RE = re.compile(r"(?:\+7|8)[\s\-()]*\d[\d\s\-()]{8,}\d")

CONTACT_PATHS = ("", "/contacts", "/contact", "/kontakty", "/about", "/company")
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}


async def _fetch_html(client: httpx.AsyncClient, url: str) -> str | None:
    try:
        response = await client.get(url)
        response.raise_for_status()
        content_type = response.headers.get("content-type", "").lower()
        if "text/html" not in content_type:
            return None
        return response.text
    except Exception:
        return None


def _clean_phone(phone: str) -> str:
    return re.sub(r"\s+", " ", phone).strip()


def _extract_text_blocks(soup: BeautifulSoup) -> dict[str, str | None]:
    title = soup.title.get_text(" ", strip=True)[:200] if soup.title else None

    description = None
    meta = soup.find("meta", attrs={"name": "description"})
    if meta and meta.get("content"):
        description = meta.get("content", "").strip()[:300]

    h1 = soup.find("h1")
    h1_text = h1.get_text(" ", strip=True)[:200] if h1 else None

    body_text = soup.get_text(" ", strip=True)
    compact_text = re.sub(r"\s+", " ", body_text)[:2500]

    return {
        "title": title,
        "description": description,
        "h1": h1_text,
        "text": compact_text,
    }


async def analyze_domain(domain: str) -> dict[str, Any]:
    variants = [f"https://{domain}", f"http://{domain}"]

    async with httpx.AsyncClient(
        timeout=8,
        follow_redirects=True,
        headers=HEADERS,
        verify=False,
    ) as client:
        base_url = None
        home_html = None
        for url in variants:
            home_html = await _fetch_html(client, url)
            if home_html:
                base_url = url
                break

        if not home_html or not base_url:
            return {
                "title": None,
                "description": None,
                "h1": None,
                "text": None,
                "emails": [],
                "phones": [],
            }

        pages = [home_html]
        for path in CONTACT_PATHS[1:]:
            html = await _fetch_html(client, urljoin(base_url, path))
            if html:
                pages.append(html)

    emails: set[str] = set()
    phones: set[str] = set()
    title = description = h1 = text = None

    for idx, html in enumerate(pages):
        soup = BeautifulSoup(html, "lxml")
        blocks = _extract_text_blocks(soup)

        if idx == 0:
            title = blocks["title"]
            description = blocks["description"]
            h1 = blocks["h1"]
            text = blocks["text"]

        emails.update(email.lower() for email in EMAIL_RE.findall(html))
        phones.update(_clean_phone(phone) for phone in PHONE_RE.findall(html))

    return {
        "title": title,
        "description": description,
        "h1": h1,
        "text": text,
        "emails": sorted(emails)[:3],
        "phones": sorted(phones)[:3],
    }
