from __future__ import annotations

import re

from telegram_signals.keywords import (
    BRAND_KEYWORDS,
    BUSINESS_HINT_KEYWORDS,
    CONTRACTOR_HINT_KEYWORDS,
    DIRECT_KEYWORDS,
    INTENT_KEYWORDS,
    NOISE_KEYWORDS,
    PAIN_KEYWORDS,
)


def _contains_any(text_l: str, keywords: list[str]) -> list[str]:
    return [kw for kw in keywords if kw in text_l]


def _extract_company_hint(text: str) -> str | None:
    patterns = [
        r"мы\s+[-—]\s+([^\n\.]{3,80})",
        r"компания\s+[-—:]\s*([^\n\.]{3,80})",
        r"бренд\s+[-—:]\s*([^\n\.]{3,80})",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.I)
        if match:
            value = match.group(1).strip(" -—:.,")
            if len(value) >= 3:
                return value[:120]
    return None


def _extract_website_hint(text_l: str) -> str | None:
    match = re.search(r"(https?://\S+|www\.\S+|[a-z0-9-]+\.(ru|com|shop|store|online))", text_l, flags=re.I)
    if not match:
        return None
    return match.group(1)[:120]


def _guess_author_type(text_l: str) -> tuple[str, int, int]:
    contractor_hits = _contains_any(text_l, CONTRACTOR_HINT_KEYWORDS)
    business_hits = _contains_any(text_l, BUSINESS_HINT_KEYWORDS)

    owner_likelihood_score = len(business_hits) * 2
    contractor_penalty = len(contractor_hits) * 3

    if contractor_penalty >= owner_likelihood_score + 2:
        return "contractor", owner_likelihood_score, contractor_penalty
    if owner_likelihood_score >= contractor_penalty + 1:
        return "business", owner_likelihood_score, contractor_penalty
    return "unknown", owner_likelihood_score, contractor_penalty


def _guess_conversation_type(text_l: str, has_intent: bool, has_pain: bool, author_type: str) -> str:
    if has_intent:
        return "help_request"
    if has_pain:
        return "complaint"
    if "?" in text_l or any(x in text_l for x in ["кто", "как", "зачем", "почему", "посоветуйте"]):
        return "question"
    if author_type == "contractor":
        return "promo_post"
    return "discussion"


def build_recommended_opener(text_l: str, message_type: str, lead_fit: str) -> str:
    if lead_fit == "target":
        return (
            "Вижу, что здесь есть реальный бизнесовый сигнал. "
            "Я бы заходил не с каналами, а с короткой гипотезой: где маркетинг не доходит до денег, "
            "как снизить зависимость от маркетплейсов и собрать управляемый direct-канал."
        )
    if message_type == "pain":
        return (
            "Вижу, что у вас обсуждается экономика канала и зависимость от маркетплейсов. "
            "В таких кейсах имеет смысл заходить не с рекламой как таковой, а с гипотезой по direct-каналу и ROMI."
        )
    if message_type == "need_contractor":
        return (
            "Вижу запрос на решение или подрядчика. "
            "Тут можно заходить через короткую гипотезу: где маркетинг не доходит до денег и как собрать управляемый direct-канал."
        )
    if message_type == "direct_growth":
        return (
            "Похоже, у компании есть интерес к своему сайту и direct. "
            "Тут уместен заход через рост продаж в собственном канале и снижение зависимости от маркетплейсов."
        )
    if message_type == "brand_signal":
        return (
            "Вижу признаки бренда/производства. "
            "Уместен заход через рост собственного канала продаж и контроль экономики маркетинга."
        )
    return "Есть релевантный e-commerce сигнал. Нужен аккуратный ручной разбор."


def _build_lead_fit(
    *,
    message_type: str,
    author_type_guess: str,
    signal_score: int,
    pain_hits: list[str],
    intent_hits: list[str],
    direct_hits: list[str],
    brand_hits: list[str],
    contactability_score: int,
) -> tuple[str, str]:
    if message_type in {"service_ad", "vacancy"} or author_type_guess == "contractor":
        return "contractor", "ignore"

    has_business_signal = bool(pain_hits or intent_hits or direct_hits or brand_hits)

    if (
        message_type in {"pain", "need_contractor", "direct_growth", "brand_signal"}
        and has_business_signal
        and signal_score >= 8
    ):
        if pain_hits or intent_hits or direct_hits:
            if contactability_score >= 1:
                return "target", "outreach_now"
            return "review", "research_company"
        return "review", "research_company"

    if has_business_signal and signal_score >= 5:
        return "review", "manual_review"

    return "noise", "ignore"


def classify_signal(
    text: str,
    segment: str,
    *,
    context_text: str = "",
    author_username: str | None = None,
    chat_title: str | None = None,
    chat_username: str | None = None,
) -> dict:
    text = text or ""
    context_text = context_text or ""
    text_l = text.lower()
    context_l = context_text.lower()
    full_l = f"{text_l}\n{context_l}"

    pain_hits = _contains_any(full_l, PAIN_KEYWORDS)
    intent_hits = _contains_any(full_l, INTENT_KEYWORDS)
    direct_hits = _contains_any(full_l, DIRECT_KEYWORDS)
    brand_hits = _contains_any(full_l, BRAND_KEYWORDS)
    noise_hits = _contains_any(full_l, NOISE_KEYWORDS)

    author_type_guess, owner_likelihood_score, contractor_penalty = _guess_author_type(full_l)

    pain_score = len(pain_hits) * 3
    intent_score = len(intent_hits) * 4
    icp_score = len(direct_hits) * 2 + len(brand_hits) * 2
    context_score = 2 if context_text.strip() else 0
    if any(x in context_l for x in ["кто посоветует", "посоветуйте", "не окупается", "ищем", "у нас", "кто работал", "подскажите", "есть ли смысл"]):
        context_score += 2

    promo_penalty = 0
    if any(x in text_l for x in ["напишите в лс", "есть кейсы", "помогаем", "делаем под ключ", "инфографика", "дизайн карточек"]):
        promo_penalty += 5

    if noise_hits:
        message_type = "noise"
    elif "вакан" in full_l or "резюме" in full_l:
        message_type = "vacancy"
    elif author_type_guess == "contractor" and not pain_hits and not intent_hits:
        message_type = "service_ad"
    elif intent_hits:
        message_type = "need_contractor"
    elif direct_hits:
        message_type = "direct_growth"
    elif pain_hits:
        message_type = "pain"
    elif brand_hits:
        message_type = "brand_signal"
    else:
        message_type = "noise"

    conversation_type = _guess_conversation_type(full_l, bool(intent_hits), bool(pain_hits), author_type_guess)

    matched = pain_hits + intent_hits + direct_hits + brand_hits
    seen = []
    matched_keywords = []
    for kw in matched:
        if kw not in seen:
            seen.append(kw)
            matched_keywords.append(kw)

    contactability_score = 0
    contact_hint = None
    if author_username:
        contactability_score += 3
        contact_hint = f"@{author_username}"
    elif chat_username:
        contactability_score += 1
        contact_hint = f"https://t.me/{chat_username}"

    website_hint = _extract_website_hint(text)
    if website_hint:
        contactability_score += 2

    company_hint = _extract_company_hint(text)
    if company_hint:
        contactability_score += 1

    segment_bonus = 0
    if segment == "ecom_marketplace_pain" and any(x in full_l for x in ["wildberries", "wb", "ozon", "маркетплейс"]):
        segment_bonus += 3
    if segment == "ecom_direct_growth" and direct_hits:
        segment_bonus += 3
    if segment == "manufacturer_secondary" and brand_hits:
        segment_bonus += 3

    final_lead_score = (
        icp_score
        + pain_score
        + intent_score
        + context_score
        + owner_likelihood_score
        + contactability_score
        + segment_bonus
        - promo_penalty
        - contractor_penalty
    )

    signal_score = max(final_lead_score, 0)

    if signal_score >= 16:
        signal_level = "high"
    elif signal_score >= 9:
        signal_level = "medium"
    else:
        signal_level = "low"

    lead_fit, next_step = _build_lead_fit(
        message_type=message_type,
        author_type_guess=author_type_guess,
        signal_score=signal_score,
        pain_hits=pain_hits,
        intent_hits=intent_hits,
        direct_hits=direct_hits,
        brand_hits=brand_hits,
        contactability_score=contactability_score,
    )
    is_actionable = lead_fit == "target"

    reasons = []
    if pain_hits:
        reasons.append("есть боль по экономике/зависимости")
    if intent_hits:
        reasons.append("есть запрос на решение/подрядчика")
    if direct_hits:
        reasons.append("есть интерес к direct/сайту")
    if brand_hits:
        reasons.append("есть признаки бренда/производства")
    if author_type_guess == "business":
        reasons.append("текст больше похож на бизнес, чем на подрядчика")
    if context_score >= 2:
        reasons.append("есть контекст обсуждения")
    if contactability_score >= 3:
        reasons.append("есть прямой контакт автора")
    elif contactability_score >= 1:
        reasons.append("есть хотя бы косвенный канал контакта")

    why = "; ".join(reasons[:4])

    return {
        "matched_keywords": ",".join(matched_keywords),
        "signal_score": signal_score,
        "signal_level": signal_level,
        "message_type": message_type,
        "conversation_type": conversation_type,
        "author_type_guess": author_type_guess,
        "icp_score": icp_score,
        "pain_score": pain_score,
        "intent_score": intent_score,
        "context_score": context_score,
        "owner_likelihood_score": owner_likelihood_score,
        "promo_penalty": promo_penalty,
        "contractor_penalty": contractor_penalty,
        "final_lead_score": signal_score,
        "contactability_score": contactability_score,
        "lead_fit": lead_fit,
        "next_step": next_step,
        "is_actionable": 1 if is_actionable else 0,
        "company_hint": company_hint,
        "website_hint": website_hint,
        "contact_hint": contact_hint,
        "why_actionable": why,
        "recommended_opener": build_recommended_opener(full_l, message_type, lead_fit),
    }
