import os
from typing import Any

import httpx

from utils.domain_normalizer import domains_for_lookup

HELPER_API_BASE_URL = os.getenv("HELPER_API_BASE_URL", "http://127.0.0.1:8010").rstrip("/")


async def get_company_by_domain(domain: str) -> dict[str, Any] | None:
    candidates = domains_for_lookup(domain)
    if not candidates:
        return None

    timeout = httpx.Timeout(12.0, connect=4.0)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        for idx, candidate in enumerate(candidates):
            data = await _lookup_domain(client, candidate)
            if data:
                data.setdefault("lookup_domain", candidate)
                data.setdefault("lookup_strategy", "root_domain" if idx > 0 else "full_domain")
                return data
    return None


async def _lookup_domain(client: httpx.AsyncClient, domain: str) -> dict[str, Any] | None:
    try:
        response = await client.get(f"{HELPER_API_BASE_URL}/lookup/by-domain", params={"domain": domain})
    except Exception:
        return None

    if response.status_code >= 400:
        return None

    try:
        data = response.json()
    except Exception:
        return None

    if not isinstance(data, dict) or data.get("error"):
        return None

    normalized = _normalize_payload(data)
    if not normalized.get("email") and not normalized.get("phone") and not normalized.get("employees"):
        return None
    return normalized



def _normalize_payload(data: dict[str, Any]) -> dict[str, Any]:
    return {
        "email": _clean_email(data.get("email")),
        "phone": _clean_phone(data.get("phone")),
        "website": _clean_text(data.get("website")),
        "employees": _clean_text(data.get("employees")),
    }



def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None



def _clean_email(value: Any) -> str | None:
    text = _clean_text(value)
    if not text:
        return None
    lowered = text.lower()
    if any(bad in lowered for bad in ["example.com", "email.com", "noreply", "no-reply", "sentry", "test@"]):
        return None
    if "@" not in lowered or "." not in lowered.split("@")[-1]:
        return None
    return lowered



def _clean_phone(value: Any) -> str | None:
    text = _clean_text(value)
    if not text:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) < 10 or len(digits) > 15:
        return None
    if len(set(digits)) <= 2:
        return None
    return text
