import re

import httpx
from bs4 import BeautifulSoup

from utils.domain_normalizer import normalize_domain

EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
PHONE_RE = re.compile(r"(?:\+?7|8)[\s\-()]*\d[\d\s\-()]{8,}")
BAD_EMAIL_PARTS = {"example.com", "email.com", "noreply", "no-reply", "sentry", "test@", "rating@", "info@example"}
BAD_PHONE_VALUES = {"0000000000", "1111111111", "1234567890", "1010101010", "8000000000"}


async def analyze_domain(domain: str) -> dict:
    normalized = normalize_domain(domain)
    if not normalized:
        return _empty_result()

    variants = [f"https://{normalized}", f"http://{normalized}"]
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        )
    }

    timeout = httpx.Timeout(10.0, connect=4.0)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=headers, verify=False) as client:
        for url in variants:
            try:
                response = await client.get(url)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, "lxml")

                title = soup.title.get_text(" ", strip=True)[:200] if soup.title else None
                description = None
                meta_desc = soup.find("meta", attrs={"name": "description"})
                if meta_desc and meta_desc.get("content"):
                    description = meta_desc["content"].strip()[:320]

                h1 = None
                h1_tag = soup.find("h1")
                if h1_tag:
                    h1 = h1_tag.get_text(" ", strip=True)[:200]

                text = soup.get_text(" ", strip=True)
                text = re.sub(r"\s+", " ", text)[:6000]

                email = _extract_email(response.text)
                phone = _extract_phone(text or response.text)

                return {
                    "title": title,
                    "description": description,
                    "h1": h1,
                    "text": text,
                    "email": email,
                    "phone": phone,
                }
            except Exception:
                continue

    return _empty_result()


def _empty_result() -> dict:
    return {
        "title": None,
        "description": None,
        "h1": None,
        "text": "",
        "email": None,
        "phone": None,
    }


def _extract_email(text: str | None) -> str | None:
    if not text:
        return None

    for match in EMAIL_RE.findall(text):
        email = match.strip().lower()
        if any(bad in email for bad in BAD_EMAIL_PARTS):
            continue
        if email.endswith(('.png', '.jpg', '.jpeg', '.svg', '.webp', '.gif')):
            continue
        return email
    return None


def _extract_phone(text: str | None) -> str | None:
    if not text:
        return None

    for match in PHONE_RE.findall(text):
        phone = re.sub(r"\s+", " ", match).strip()
        digits = re.sub(r"\D", "", phone)
        if len(digits) < 10 or len(digits) > 15:
            continue
        if digits[-10:] in BAD_PHONE_VALUES:
            continue
        if len(set(digits)) <= 3:
            continue
        return phone
    return None
