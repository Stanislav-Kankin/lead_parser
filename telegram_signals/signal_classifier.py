from __future__ import annotations

import re
from typing import Any

from telegram_signals.keywords import (
    DIRECT_KEYWORDS,
    INTENT_KEYWORDS,
    NEGATIVE_KEYWORDS,
    PAIN_KEYWORDS,
    POSITIVE_KEYWORDS,
    SERVICE_AD_KEYWORDS,
)

URL_RE = re.compile(r"(?:https?://)?(?:www\.)?([a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)+)")
TG_HANDLE_RE = re.compile(r"(?<!\w)@([A-Za-z0-9_]{5,})")
COMPANY_PATTERNS = [
    re.compile(r"мы\s*[—-]\s*([^\n\r]{3,80})", re.IGNORECASE),
    re.compile(r"наш бренд\s*[—-]?\s*([^\n\r]{3,80})", re.IGNORECASE),
    re.compile(r"компания\s*[—-]?\s*([^\n\r]{3,80})", re.IGNORECASE),
]

ALLOWED_MESSAGE_TYPES = {"pain", "need_contractor", "direct_growth", "brand_signal"}


def _contains_any(text_l: str, keywords: list[str]) -> list[str]:
    found: list[str] = []
    for kw in keywords:
        if kw in text_l:
            found.append(kw)
    return found


def _extract_website_hint(text: str) -> str | None:
    match = URL_RE.search(text or "")
    if not match:
        return None
    return match.group(1).lower()


def _extract_contact_hint(text: str, author_username: str | None = None) -> str | None:
    if author_username:
        return f"@{author_username}"
    match = TG_HANDLE_RE.search(text or "")
    if match:
        return f"@{match.group(1)}"
    return None


def _clean_hint(value: str | None) -> str | None:
    if not value:
        return None
    value = value.strip(" -—:,.\n\r\t")
    return value[:120] if value else None


def _extract_company_hint(text: str) -> str | None:
    text = text or ""
    for pattern in COMPANY_PATTERNS:
        match = pattern.search(text)
        if match:
            value = _clean_hint(match.group(1))
            if value and len(value.split()) <= 10:
                return value
    return None


def _detect_message_type(text_l: str, positive: list[str], pain: list[str], intent: list[str], negative: list[str]) -> str:
    if _contains_any(text_l, SERVICE_AD_KEYWORDS):
        return "service_ad"
    if any(x in text_l for x in ["вакансия", "резюме", "ищу сотрудника", "требуется"]):
        return "vacancy"
    if negative and not pain and not intent:
        return "noise"
    if intent:
        return "need_contractor"
    if pain:
        return "pain"
    if _contains_any(text_l, DIRECT_KEYWORDS):
        return "direct_growth"
    if any(x in text_l for x in ["производитель", "собственное производство", "свой бренд", "наш бренд", "бренд"]):
        return "brand_signal"
    if positive:
        return "marketplace_context"
    return "noise"


def build_recommended_opener(text_l: str, message_type: str) -> str:
    if message_type == "pain":
        return (
            "Вижу у вас в обсуждении всплывает проблема экономики: комиссии, маржа или зависимость от маркетплейсов. "
            "Обычно в такой точке полезно не обсуждать каналы по отдельности, а смотреть, как собрать управляемый direct-канал и вернуть контроль над спросом."
        )
    if message_type == "need_contractor":
        return (
            "Вижу запрос на подрядчика или рост канала. Логичный заход — не про рекламу вообще, а про то, где сейчас ломается экономика: CAC, ROMI, зависимость от WB/Ozon и потенциал собственного сайта."
        )
    if message_type == "direct_growth":
        return (
            "Похоже, у компании уже есть задача усилить собственный сайт или direct-продажи. Здесь сильный заход — гипотеза не про трафик сам по себе, а про сборку канала продаж с понятной экономикой и масштабированием."
        )
    if message_type == "brand_signal":
        return (
            "Похоже, это бренд или производитель, для которого может быть актуален рост вне маркетплейсов. Можно зайти с гипотезой про независимость от площадок, контроль спроса и развитие собственного канала."
        )
    return (
        "Есть косвенный сигнал, что компании может быть актуален рост direct-продаж или снижение зависимости от маркетплейсов."
    )


def classify_signal(text: str, segment: str, author_username: str | None = None) -> dict[str, Any]:
    text = text or ""
    text_l = text.lower()

    positive = _contains_any(text_l, POSITIVE_KEYWORDS)
    intent = _contains_any(text_l, INTENT_KEYWORDS)
    pain = _contains_any(text_l, PAIN_KEYWORDS)
    negative = _contains_any(text_l, NEGATIVE_KEYWORDS)
    direct = _contains_any(text_l, DIRECT_KEYWORDS)

    message_type = _detect_message_type(text_l, positive, pain, intent, negative)

    icp_score = 0
    if any(x in text_l for x in ["бренд", "производитель", "собственное производство", "опт", "поставщик"]):
        icp_score += 3
    if any(x in text_l for x in ["свой сайт", "интернет-магазин", "d2c", "direct"]):
        icp_score += 2
    if any(x in text_l for x in ["wildberries", "wb", "ozon", "маркетплейс", "маркетплейсы"]):
        icp_score += 2

    pain_score = len(pain) * 3
    if segment == "ecom_marketplace_pain" and any(x in text_l for x in ["wildberries", "wb", "ozon", "маркетплейс", "маркетплейсы"]):
        pain_score += 2
    if segment == "ecom_direct_growth" and direct:
        pain_score += 1

    intent_score = len(intent) * 4
    if any(x in text_l for x in ["ищем", "нужен", "кто поможет", "посоветуйте", "рекомендуйте"]):
        intent_score += 1

    contact_hint = _extract_contact_hint(text, author_username)
    website_hint = _extract_website_hint(text)
    company_hint = _extract_company_hint(text)

    contactability_score = 0
    if contact_hint:
        contactability_score += 3
    if website_hint:
        contactability_score += 2
    if company_hint:
        contactability_score += 1

    score = icp_score + pain_score + intent_score + contactability_score + len(positive)
    score -= len(negative) * 4
    if message_type in {"service_ad", "vacancy", "noise"}:
        score -= 8

    is_actionable = (
        message_type in ALLOWED_MESSAGE_TYPES
        and (pain_score >= 3 or intent_score >= 4)
        and contactability_score >= 2
        and score >= 8
    )

    if message_type in {"service_ad", "vacancy", "noise"}:
        level = "low"
    elif score >= 14 or is_actionable:
        level = "high"
    elif score >= 8:
        level = "medium"
    else:
        level = "low"

    matches = list(dict.fromkeys(positive + intent + pain + negative + direct))

    return {
        "score": score,
        "level": level,
        "matches": matches,
        "recommended_opener": build_recommended_opener(text_l, message_type),
        "positive": positive,
        "intent": intent,
        "negative": negative,
        "pain": pain,
        "direct": direct,
        "message_type": message_type,
        "icp_score": icp_score,
        "pain_score": pain_score,
        "intent_score": intent_score,
        "contactability_score": contactability_score,
        "contact_hint": contact_hint,
        "company_hint": company_hint,
        "website_hint": website_hint,
        "is_actionable": is_actionable,
    }
