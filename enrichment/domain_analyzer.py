import re

import httpx
from bs4 import BeautifulSoup

EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
PHONE_RE = re.compile(r"(?:\+?\d[\d\s()\-]{8,}\d)")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}


def _clean_text(value: str | None, limit: int = 400) -> str | None:
    if not value:
        return None
    text = re.sub(r"\s+", " ", value).strip()
    return text[:limit] if text else None



def _pick_email(text: str) -> str | None:
    for email in EMAIL_RE.findall(text or ""):
        lowered = email.lower()
        if any(x in lowered for x in ["example.com", "sentry", "noreply", "no-reply"]):
            continue
        return lowered[:200]
    return None



def _pick_phone(text: str) -> str | None:
    for raw in PHONE_RE.findall(text or ""):
        digits = re.sub(r"\D", "", raw)
        if len(digits) < 10 or len(digits) > 15:
            continue
        return raw.strip()[:50]
    return None


async def analyze_domain(domain: str) -> dict:
    variants = [f"https://{domain}", f"http://{domain}"]

    async with httpx.AsyncClient(timeout=10, follow_redirects=True, headers=HEADERS, verify=False) as client:
        for url in variants:
            try:
                response = await client.get(url)
                response.raise_for_status()
                html = response.text[:500000]
                soup = BeautifulSoup(html, "lxml")

                title = soup.title.get_text(" ", strip=True) if soup.title else None
                meta = soup.find("meta", attrs={"name": "description"})
                meta_description = meta.get("content") if meta else None
                h1 = soup.find("h1")
                h1_text = h1.get_text(" ", strip=True) if h1 else None
                body_text = soup.get_text(" ", strip=True)
                sample_text = _clean_text(" ".join(filter(None, [title, meta_description, h1_text, body_text[:3000]])), 3000)

                return {
                    "title": _clean_text(title, 200),
                    "meta_description": _clean_text(meta_description, 300),
                    "h1": _clean_text(h1_text, 200),
                    "text": sample_text,
                    "email": _pick_email(html),
                    "phone": _pick_phone(html),
                }
            except Exception:
                continue

    return {
        "title": None,
        "meta_description": None,
        "h1": None,
        "text": None,
        "email": None,
        "phone": None,
    }
