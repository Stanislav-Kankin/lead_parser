import logging
import re
import time
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from utils.domain_normalizer import normalize_domain

logger = logging.getLogger(__name__)

EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
PHONE_RE = re.compile(r"(?:\+?7|8)[\s\-()]*\d[\d\s\-()]{8,}")
INN_RE = re.compile(r"(?:инн|inn)\s*[:№#]?\s*(\d{10}|\d{12})", re.IGNORECASE)
OGRN_RE = re.compile(r"(?:огрн|ogrn)\s*[:№#]?\s*(\d{13}|\d{15})", re.IGNORECASE)
LEGAL_NAME_RE = re.compile(r"\b((?:ООО|АО|ПАО|ЗАО|ИП)\s*[«\"]?[^\n\r\t<>]{2,120})", re.IGNORECASE)

BAD_EMAIL_PARTS = {"example.com", "email.com", "noreply", "no-reply", "sentry", "test@", "rating@", "info@example"}
BAD_PHONE_VALUES = {"0000000000", "1111111111", "1234567890", "1010101010", "8000000000"}
SECONDARY_PATHS = ["/contacts", "/rekvizity"]


async def analyze_domain(domain: str) -> dict:
    normalized = normalize_domain(domain)
    if not normalized:
        return _empty_result()

    logger.info("[domain_analyzer] start domain=%s", normalized)
    started = time.perf_counter()

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"}
    timeout = httpx.Timeout(4.0, connect=2.5)
    result = _empty_result()

    async with httpx.AsyncClient(headers=headers, timeout=timeout, follow_redirects=True, verify=False) as client:
        homepage = await _fetch_page(client, f"https://{normalized}")
        if homepage:
            result = _merge_result(result, homepage)

        need_more = not (result.get("email") or result.get("phone")) or not (result.get("company_inn") or result.get("company_legal_name"))
        if need_more:
            for path in SECONDARY_PATHS:
                page = await _fetch_page(client, urljoin(f"https://{normalized}", path))
                if page:
                    result = _merge_result(result, page)
                if (result.get("email") or result.get("phone")) and (result.get("company_inn") or result.get("company_legal_name")):
                    break

    elapsed = time.perf_counter() - started
    logger.info(
        "[domain_analyzer] done domain=%s email=%s phone=%s inn=%s ogrn=%s elapsed=%.2fs",
        normalized,
        bool(result.get("email")),
        bool(result.get("phone")),
        bool(result.get("company_inn")),
        bool(result.get("company_ogrn")),
        elapsed,
    )
    return result


async def _fetch_page(client: httpx.AsyncClient, url: str) -> dict | None:
    try:
        logger.info("[domain_analyzer] fetch url=%s", url)
        response = await client.get(url)
        if response.status_code >= 400:
            return None
    except Exception as exc:
        logger.info("[domain_analyzer] fetch_failed url=%s error=%s", url, exc)
        return None

    content_type = response.headers.get("content-type", "")
    if "text/html" not in content_type:
        return None

    html = response.text or ""
    soup = BeautifulSoup(html, "lxml")
    title = soup.title.get_text(" ", strip=True) if soup.title else None
    meta = soup.find("meta", attrs={"name": "description"})
    description = meta.get("content", "").strip() if meta else None
    h1 = soup.find("h1")
    h1_text = h1.get_text(" ", strip=True) if h1 else None

    for tag in soup(["script", "style", "noscript"]):
        tag.extract()

    text = soup.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text)
    text = text[:15000]

    email = _extract_email(text)
    phone = _extract_phone(text)
    inn = _extract_first(INN_RE, text)
    ogrn = _extract_first(OGRN_RE, text)
    legal_name = _extract_legal_name(text)
    legal_form = _extract_legal_form(legal_name)

    return {
        "title": title,
        "description": description,
        "h1": h1_text,
        "text": text,
        "email": email,
        "phone": phone,
        "company_inn": inn,
        "company_ogrn": ogrn,
        "company_legal_name": legal_name,
        "legal_form": legal_form,
        "inn_source": "site_requisites" if inn or legal_name else None,
    }



def _empty_result() -> dict:
    return {
        "title": None,
        "description": None,
        "h1": None,
        "text": "",
        "email": None,
        "phone": None,
        "company_inn": None,
        "company_ogrn": None,
        "company_legal_name": None,
        "legal_form": None,
        "inn_source": None,
    }



def _merge_result(base: dict, page: dict) -> dict:
    for key in ["title", "description", "h1", "email", "phone", "company_inn", "company_ogrn", "company_legal_name", "legal_form", "inn_source"]:
        if not base.get(key) and page.get(key):
            base[key] = page[key]
    merged_text = " ".join(part for part in [base.get("text"), page.get("text")] if part)
    base["text"] = merged_text[:15000]
    return base



def _extract_first(pattern: re.Pattern, text: str | None) -> str | None:
    if not text:
        return None
    match = pattern.search(text)
    if not match:
        return None
    value = match.group(1).strip()
    return value or None



def _extract_legal_name(text: str | None) -> str | None:
    if not text:
        return None
    for match in LEGAL_NAME_RE.finditer(text):
        value = re.sub(r"\s+", " ", match.group(1)).strip(" .,-;:")
        if len(value) < 4:
            continue
        if any(bad in value.lower() for bad in ["политик", "конфиденц", "пользоват"]):
            continue
        return value[:120]
    return None



def _extract_legal_form(legal_name: str | None) -> str | None:
    if not legal_name:
        return None
    upper = legal_name.upper()
    for form in ["ООО", "АО", "ПАО", "ЗАО", "ИП"]:
        if upper.startswith(form):
            return form
    return None



def _extract_email(text: str | None) -> str | None:
    if not text:
        return None
    matches = sorted(EMAIL_RE.findall(text), key=_email_rank, reverse=True)
    for match in matches:
        email = match.strip().lower()
        if any(bad in email for bad in BAD_EMAIL_PARTS):
            continue
        if email.endswith((".png", ".jpg", ".jpeg", ".svg", ".webp", ".gif")):
            continue
        return email
    return None



def _email_rank(email: str) -> int:
    lowered = email.lower()
    score = 0
    if any(prefix in lowered for prefix in ["info@", "sales@", "hello@", "opt@", "b2b@", "zakaz@"]):
        score += 3
    if any(prefix in lowered for prefix in ["support@", "admin@", "office@"]):
        score += 1
    if lowered.endswith("@gmail.com") or lowered.endswith("@mail.ru") or lowered.endswith("@yandex.ru"):
        score -= 1
    return score



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
