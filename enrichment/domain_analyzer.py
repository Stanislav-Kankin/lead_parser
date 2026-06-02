from __future__ import annotations

import logging
import re
import time
from urllib.parse import urljoin, urlparse

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
SECONDARY_PATHS = [
    "/contacts",
    "/contacts/",
    "/contacts.html",
    "/contact",
    "/contact/",
    "/contact-us",
    "/kontakty",
    "/kontakty/",
    "/kontakty.html",
    "/kontakti",
    "/kontakt",
    "/rekvizity",
    "/rekvizity/",
    "/about/contacts",
    "/about/kontakty",
    "/company/contacts",
    "/company/kontakty",
    "/company/rekvizity",
    "/about",
    "/company",
    "/o-kompanii",
    "/filialy",
    "/stores",
    "/gde-kupit",
    "/catalog",
    "/katalog",
    "/shop",
    "/magazin",
    "/products",
    "/produktsiya",
    "/cart",
    "/basket",
    "/korzina",
]

CATALOG_HINTS = (
    "catalog",
    "katalog",
    "shop",
    "magazin",
    "products",
    "produktsiya",
    "tovary",
    "collection",
    "category",
    "каталог",
    "магазин",
    "продукция",
    "товары",
    "коллекция",
)
CART_HINTS = (
    "cart",
    "basket",
    "korzina",
    "checkout",
    "order",
    "оформить заказ",
    "корзина",
    "в корзину",
)
BUY_HINTS = ("купить", "заказать", "в корзину", "оформить", "добавить в корзину", "buy", "add to cart")
LEADGEN_HINTS = (
    "оставить заявку",
    "получить консультацию",
    "заказать звонок",
    "обратный звонок",
    "свяжитесь с нами",
    "рассчитать стоимость",
)
PRODUCT_HINTS = ("артикул", "sku", "наличии", "цена", "руб", "₽", "характеристики", "размер", "объем")
PRICE_RE = re.compile(r"\b\d{2,7}\s*(?:₽|руб\.?|р\.)\b", re.IGNORECASE)


async def analyze_domain(domain: str) -> dict:
    normalized = normalize_domain(domain)
    if not normalized:
        return _empty_result()

    logger.info("[domain_analyzer] start domain=%s", normalized)
    started = time.perf_counter()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0 Safari/537.36"
    }
    timeout = httpx.Timeout(6.0, connect=3.0)
    result = _empty_result()

    async with httpx.AsyncClient(headers=headers, timeout=timeout, follow_redirects=True, verify=False) as client:
        homepage = await _fetch_page(client, f"https://{normalized}")
        if not homepage:
            homepage = await _fetch_page(client, f"http://{normalized}")
        if homepage:
            result = _merge_result(result, homepage)

        contact_urls = _build_followup_urls(normalized, result.get("contact_links") or [])
        for url in contact_urls:
            if _has_enough_contacts(result):
                break
            page = await _fetch_page(client, url)
            if page:
                result = _merge_result(result, page)

    elapsed = time.perf_counter() - started
    logger.info(
        "[domain_analyzer] done domain=%s email=%s phone=%s inn=%s elapsed=%.2fs",
        normalized,
        bool(result.get("email")),
        bool(result.get("phone")),
        bool(result.get("company_inn")),
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

    attr_email = _extract_email_from_links(soup)
    attr_phone = _extract_phone_from_links(soup)
    contact_links = _extract_contact_links(soup, str(response.url))

    for tag in soup(["script", "style", "noscript"]):
        tag.extract()

    text = soup.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text)
    text = text[:25000]

    email = attr_email or _extract_email(text)
    phone = attr_phone or _extract_phone(text)
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
        "contact_links": contact_links,
        **_analyze_commerce(soup, text),
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
        "contact_links": [],
        "has_catalog": False,
        "has_cart": False,
        "ecommerce_score": 0,
        "site_type": None,
        "site_assessment": None,
    }


def _has_enough_text(result: dict) -> bool:
    return len(result.get("text") or "") >= 8000


def _has_enough_contacts(result: dict) -> bool:
    return bool(result.get("email") and result.get("phone"))


def _merge_result(base: dict, page: dict) -> dict:
    for key in ["title", "description", "h1", "email", "phone", "company_inn", "company_ogrn", "company_legal_name", "legal_form", "inn_source"]:
        if not base.get(key) and page.get(key):
            base[key] = page[key]
    base["contact_links"] = _merge_unique(base.get("contact_links") or [], page.get("contact_links") or [], limit=12)
    for key in ["has_catalog", "has_cart"]:
        base[key] = bool(base.get(key) or page.get(key))
    if int(page.get("ecommerce_score") or 0) > int(base.get("ecommerce_score") or 0):
        base["ecommerce_score"] = int(page.get("ecommerce_score") or 0)
        base["site_type"] = page.get("site_type")
        base["site_assessment"] = page.get("site_assessment")
    merged_text = " ".join(part for part in [base.get("text"), page.get("text")] if part)
    base["text"] = merged_text[:25000]
    return base


def _merge_unique(first: list[str], second: list[str], limit: int = 20) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in [*first, *second]:
        key = str(value).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(key)
        if len(result) >= limit:
            break
    return result


def _build_followup_urls(domain: str, discovered_links: list[str]) -> list[str]:
    base = f"https://{domain}"
    urls = []
    urls.extend(discovered_links)
    urls.extend(urljoin(base, path) for path in SECONDARY_PATHS)

    result: list[str] = []
    seen: set[str] = set()
    for url in urls:
        parsed = urlparse(str(url))
        if parsed.scheme not in {"http", "https"}:
            continue
        if parsed.netloc and normalize_domain(parsed.netloc) != domain:
            continue
        clean = parsed._replace(fragment="").geturl()
        key = clean.rstrip("/").lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(clean)
    return result[:28]


def _extract_contact_links(soup: BeautifulSoup, current_url: str) -> list[str]:
    hints = (
        "contact",
        "contacts",
        "kontakty",
        "kontakti",
        "kontakt",
        "rekvizity",
        "requisites",
        "filial",
        "stores",
        "где купить",
        "контакт",
        "реквизит",
        "филиал",
        "магазин",
    )
    links: list[str] = []
    for link in soup.find_all("a", href=True):
        href = str(link.get("href") or "").strip()
        if not href or href.startswith(("mailto:", "tel:", "javascript:", "#")):
            continue
        label = f"{href} {link.get_text(' ', strip=True)}".lower()
        if any(hint in label for hint in hints):
            links.append(urljoin(current_url, href))
    return _merge_unique([], links, limit=12)


def _extract_email_from_links(soup: BeautifulSoup) -> str | None:
    values: list[str] = []
    for link in soup.find_all("a", href=True):
        href = str(link.get("href") or "")
        if href.lower().startswith("mailto:"):
            values.append(href.split(":", 1)[1].split("?", 1)[0])
    return _extract_email(" ".join(values))


def _extract_phone_from_links(soup: BeautifulSoup) -> str | None:
    values: list[str] = []
    for link in soup.find_all("a", href=True):
        href = str(link.get("href") or "")
        if href.lower().startswith("tel:"):
            values.append(href.split(":", 1)[1])
    return _extract_phone(" ".join(values))


def _analyze_commerce(soup: BeautifulSoup, text: str) -> dict:
    lowered_text = (text or "").lower()
    link_values = []
    for link in soup.find_all("a", href=True):
        value = f"{link.get('href', '')} {link.get_text(' ', strip=True)}".lower()
        link_values.append(value)
    link_blob = " ".join(link_values[:500])

    button_blob = " ".join(
        tag.get_text(" ", strip=True).lower()
        for tag in soup.find_all(["button", "a", "input"])
        if tag.get_text(" ", strip=True) or tag.get("value")
    )
    button_blob = f"{button_blob} {' '.join(str(tag.get('value', '')).lower() for tag in soup.find_all('input'))}"

    catalog_hits = sum(1 for hint in CATALOG_HINTS if hint in link_blob or hint in lowered_text)
    cart_hits = sum(1 for hint in CART_HINTS if hint in link_blob or hint in lowered_text or hint in button_blob)
    buy_hits = sum(1 for hint in BUY_HINTS if hint in lowered_text or hint in button_blob)
    leadgen_hits = sum(1 for hint in LEADGEN_HINTS if hint in lowered_text)
    product_hits = sum(1 for hint in PRODUCT_HINTS if hint in lowered_text)
    price_hits = len(PRICE_RE.findall(lowered_text[:20000]))
    product_link_hits = sum(
        1
        for value in link_values[:500]
        if any(part in value for part in ["/product", "/tovar", "/catalog/", "/katalog/", "/shop/"])
    )

    has_catalog = catalog_hits > 0 or product_link_hits >= 3
    has_cart = cart_hits > 0 or buy_hits >= 2

    score = 0
    if has_catalog:
        score += 25
    if has_cart:
        score += 25
    if product_link_hits >= 5:
        score += 12
    if price_hits >= 3:
        score += 15
    elif price_hits:
        score += 7
    if product_hits >= 3:
        score += 10
    if buy_hits:
        score += 8
    if leadgen_hits >= 2 and not has_catalog:
        score -= 8

    score = max(0, min(100, score))

    if has_catalog and has_cart and score >= 55:
        site_type = "ecommerce_site"
        assessment = "есть каталог и признаки корзины/покупки; сайт похож на direct/ecom-канал"
    elif has_catalog:
        site_type = "catalog_site"
        assessment = "есть каталог/товарные разделы, но корзина неочевидна; похоже на каталог или B2B-продажи"
    elif leadgen_hits >= 2:
        site_type = "leadgen_landing"
        assessment = "каталог и корзина не найдены; сайт больше похож на лидоген-лендинг"
    elif score >= 25:
        site_type = "commerce_possible"
        assessment = "есть отдельные коммерческие признаки, но структура продаж неочевидна"
    else:
        site_type = "corporate_site"
        assessment = "каталог, корзина и явная ecom-логика не найдены"

    return {
        "has_catalog": has_catalog,
        "has_cart": has_cart,
        "ecommerce_score": score,
        "site_type": site_type,
        "site_assessment": assessment,
    }


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
        if any(bad in value.lower() for bad in ["политик", "конфиденц", "пользоват", "согласие"]):
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
    if any(prefix in lowered for prefix in ["info@", "sales@", "hello@", "opt@", "b2b@", "zakaz@", "shop@"]):
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
