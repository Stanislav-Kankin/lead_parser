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


FIRST_PERSON_PAIN_PATTERNS = [
    "у нас",
    "мы ищем",
    "ищем подрядчика",
    "ищем агентство",
    "нужен подрядчик",
    "нужен маркетолог",
    "посоветуйте",
    "кто работал",
    "кто реально",
    "не окупается",
    "не сходится",
    "маржа падает",
    "комиссия съедает",
    "хотим свой сайт",
    "хотим direct",
    "нужен сайт",
    "зависим от wb",
    "зависим от ozon",
    "как уйти",
]

EXPERT_CONTENT_PATTERNS = [
    "что нужно знать",
    "вот мой ответ",
    "разбер",
    "в этой статье",
    "я написал",
    "сегодня поговорим",
    "объясняю",
    "рассказываю",
    "кейс",
    "подкаст",
    "видео",
    "новость",
]

MARKET_OBSERVATION_PATTERNS = [
    "рынок",
    "повышает комиссии",
    "маркетплейсы зачищают",
    "государство",
    "аналитика",
    "исследование",
    "новый закон",
    "новые правила",
]

SUPPLIER_AD_PATTERNS = [
    "отгрузка оптом",
    "со склада",
    "цена с ндс",
    "разные вкусы",
    "в наличии",
    "минимальный заказ",
    "прайс",
    "оптовики",
    "поставщики",
]

CHANNEL_AUTHOR_PATTERNS = [
    "подписывайтесь",
    "в канале",
    "в следующем посте",
    "в комментариях",
    "поставьте реакцию",
]

CONTRACTOR_STRONG_PATTERNS = [
    "мы агентство",
    "наше агентство",
    "я маркетолог",
    "я таргетолог",
    "я директолог",
    "веду клиентов",
    "ведем клиентов",
    "берем проекты",
    "есть кейсы",
    "напишите в лс",
    "помогаем брендам",
    "помогаем селлерам",
    "настраиваем рекламу",
    "ведем рекламу",
]

OWNER_CONTEXT_PATTERNS = [
    "у нас бренд",
    "наш бренд",
    "мы производим",
    "свое производство",
    "наш сайт",
    "свой сайт",
    "мы продаем",
    "продаем на wb",
    "продаем на ozon",
    "мы селлер",
]

QUESTION_PATTERNS = ["кто", "как", "зачем", "почему", "посоветуйте", "подскажите"]

CHANGE_EVENT_PATTERNS = [
    "запускаем сайт",
    "делаем сайт",
    "запускаем интернет-магазин",
    "хотим развивать сайт",
    "хотим direct",
    "ищем маркетолога",
    "ищем подрядчика",
    "ищем агентство",
    "нужен подрядчик",
    "нужен маркетолог",
    "нужен директолог",
    "нужен трафик",
    "как лить трафик",
    "как вести трафик",
    "кто работал с direct",
    "кто работал с сайтом",
    "хотим меньше зависеть от wb",
    "хотим меньше зависеть от ozon",
]

WEAK_REVIEW_PATTERNS = [
    "экосистема чатов",
    "ищете поставщика",
    "закрытых чатах",
    "пишите ценник",
    "оптовики",
    "поставщики",
    "закупаем",
    "продаем оптом",
    "минимальный заказ",
    "прайс",
]


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
    contractor_hits = _contains_any(text_l, CONTRACTOR_HINT_KEYWORDS) + _contains_any(text_l, CONTRACTOR_STRONG_PATTERNS)
    business_hits = _contains_any(text_l, BUSINESS_HINT_KEYWORDS) + _contains_any(text_l, OWNER_CONTEXT_PATTERNS)

    owner_likelihood_score = len(business_hits) * 2
    contractor_penalty = len(contractor_hits) * 3

    if any(pattern in text_l for pattern in CONTRACTOR_STRONG_PATTERNS):
        contractor_penalty += 4

    if any(pattern in text_l for pattern in OWNER_CONTEXT_PATTERNS):
        owner_likelihood_score += 3

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
    if "?" in text_l or any(x in text_l for x in QUESTION_PATTERNS):
        return "question"
    if author_type == "contractor":
        return "promo_post"
    return "discussion"


def _detect_primary_pain_tag(full_l: str) -> str | None:
    if any(x in full_l for x in ["не окупается", "не сходится", "дорогой трафик", "сливаем бюджет", "cac"]):
        return "ads_not_profitable"
    if any(x in full_l for x in ["wb", "ozon", "маркетплейс", "комиссия", "зависим"]):
        return "marketplace_dependency"
    if any(x in full_l for x in ["маржа", "юнит экономика", "юнит-экономика", "экономика проекта"]):
        return "margin_pressure"
    if any(x in full_l for x in ["сайт", "direct", "интернет-магазин", "трафик на сайт"]):
        return "direct_growth_need"
    if any(x in full_l for x in CHANGE_EVENT_PATTERNS):
        return "change_event"
    return None


def build_recommended_opener(text_l: str, message_type: str, lead_fit: str) -> str:
    pain_tag = _detect_primary_pain_tag(text_l)
    if lead_fit == "target":
        if pain_tag == "ads_not_profitable":
            return (
                "Вижу, что у вас упирается экономика рекламы. Обычно проблема не только в канале, "
                "а в том, как распределён спрос между маркетплейсами и direct."
            )
        if pain_tag == "marketplace_dependency":
            return (
                "Похоже, у вас есть зависимость от маркетплейсов. В таких кейсах обычно помогает не просто реклама, "
                "а сборка управляемого direct-канала, чтобы не жить только за счёт WB/Ozon."
            )
        if pain_tag == "margin_pressure":
            return (
                "Вижу сигнал по марже и экономике. Я бы заходил не с услугой, а с гипотезой: где именно теряется ROMI "
                "и можно ли вернуть рост через direct и более управляемый спрос."
            )
        if pain_tag in {"direct_growth_need", "change_event"}:
            return (
                "Вижу интерес к развитию сайта/direct. Здесь обычно важно быстро понять: сайт уже даёт продажи или "
                "сейчас основная зависимость от маркетплейсов."
            )
        return (
            "Заходить не с услугой, а с гипотезой: где именно съедается маржа, "
            "как снизить зависимость от маркетплейсов и собрать управляемый direct-канал."
        )
    if lead_fit == "review":
        return (
            "Сначала проверить компанию, сайт и роль автора. Если это бренд, производитель или seller, "
            "идти через гипотезу роста direct и экономики рекламы."
        )
    if message_type == "market_intelligence":
        return "Это лучше использовать как рыночный инсайт и источник гипотез, а не как прямой outreach."
    return "Нужен ручной разбор цепочки сообщений."


def _build_lead_fit(
    *,
    message_type: str,
    author_type_guess: str,
    signal_score: int,
    first_person_pain_score: int,
    contactability_score: int,
    conversation_score: int,
) -> tuple[str, str]:
    if message_type in {"service_ad", "vacancy", "supplier_ad"} or author_type_guess == "contractor":
        return "contractor", "ignore"

    if message_type == "self_pain":
        if contactability_score >= 1 and signal_score >= 12 and conversation_score >= 2:
            return "target", "outreach_now"
        return "review", "research_company"

    if message_type == "peer_question":
        if signal_score >= 9:
            return "review", "research_company"
        return "noise", "ignore"

    if message_type in {"expert_content", "market_intelligence"}:
        return "noise", "ignore"

    if first_person_pain_score >= 4 and signal_score >= 9:
        return "review", "research_company"

    return "noise", "ignore"


def classify_signal(
    text: str,
    segment: str,
    *,
    context_text: str = "",
    conversation_text: str = "",
    author_username: str | None = None,
    chat_title: str | None = None,
    chat_username: str | None = None,
    reply_depth: int = 0,
) -> dict:
    text = text or ""
    context_text = context_text or ""
    conversation_text = conversation_text or ""

    text_l = text.lower()
    context_l = context_text.lower()
    conversation_l = conversation_text.lower()
    full_l = "\n".join(part for part in [text_l, context_l, conversation_l] if part)
    chat_title_l = (chat_title or "").lower()

    pain_hits = _contains_any(full_l, PAIN_KEYWORDS)
    intent_hits = _contains_any(full_l, INTENT_KEYWORDS)
    direct_hits = _contains_any(full_l, DIRECT_KEYWORDS)
    brand_hits = _contains_any(full_l, BRAND_KEYWORDS)
    noise_hits = _contains_any(text_l, NOISE_KEYWORDS)

    author_type_guess, owner_likelihood_score, contractor_penalty = _guess_author_type(full_l)

    first_person_hits = [p for p in FIRST_PERSON_PAIN_PATTERNS if p in full_l]
    expert_hits = [p for p in EXPERT_CONTENT_PATTERNS if p in text_l]
    market_hits = [p for p in MARKET_OBSERVATION_PATTERNS if p in text_l]
    supplier_hits = [p for p in SUPPLIER_AD_PATTERNS if p in text_l]
    channel_hits = [p for p in CHANNEL_AUTHOR_PATTERNS if p in text_l]
    change_event_hits = [p for p in CHANGE_EVENT_PATTERNS if p in full_l]
    weak_review_hits = [p for p in WEAK_REVIEW_PATTERNS if p in full_l]

    first_person_pain_score = len(first_person_hits) * 4
    pain_score = len(pain_hits) * 3 + first_person_pain_score
    intent_score = len(intent_hits) * 4 + len(change_event_hits) * 2
    icp_score = len(direct_hits) * 2 + len(brand_hits) * 2
    if any(x in full_l for x in ["wb", "ozon", "маркетплейс", "селлер", "seller", "sku", "карточк", "интернет-магазин", "сайт"]):
        icp_score += 2

    context_score = 0
    if context_text.strip():
        context_score += 2
    if conversation_text.strip() and conversation_text.strip() != text.strip():
        context_score += 2
    if any(x in full_l for x in ["кто посоветует", "посоветуйте", "не окупается", "ищем", "у нас", "кто работал", "подскажите", "есть ли смысл"]):
        context_score += 2
    if reply_depth >= 1:
        context_score += min(reply_depth, 3)

    conversation_score = 0
    if reply_depth >= 1:
        conversation_score += 2
    if context_text.strip():
        conversation_score += 1
    if pain_hits and intent_hits:
        conversation_score += 2
    if first_person_hits and (pain_hits or direct_hits or brand_hits):
        conversation_score += 2

    promo_penalty = 0
    if any(x in text_l for x in ["напишите в лс", "есть кейсы", "помогаем", "делаем под ключ", "инфографика", "дизайн карточек"]):
        promo_penalty += 6
    if weak_review_hits:
        promo_penalty += 5
    promo_penalty += len(expert_hits) * 2
    promo_penalty += len(channel_hits) * 2

    if "рекламщик" in chat_title_l or "capital" in chat_title_l:
        promo_penalty += 3

    if noise_hits:
        message_type = "noise"
    elif "вакан" in text_l or "резюме" in text_l:
        message_type = "vacancy"
    elif supplier_hits or weak_review_hits:
        message_type = "supplier_ad"
    elif author_type_guess == "contractor" and not pain_hits and not intent_hits and not change_event_hits:
        message_type = "service_ad"
    elif first_person_hits and (pain_hits or intent_hits or direct_hits or brand_hits or change_event_hits):
        message_type = "self_pain"
    elif change_event_hits and (direct_hits or brand_hits or intent_hits):
        message_type = "self_pain"
    elif any(x in full_l for x in ["кто посоветует", "посоветуйте", "кто работал", "подскажите", "есть ли смысл"]):
        message_type = "peer_question"
    elif expert_hits:
        message_type = "expert_content"
    elif market_hits and not first_person_hits:
        message_type = "market_intelligence"
    else:
        message_type = "noise"

    conversation_type = _guess_conversation_type(full_l, bool(intent_hits), bool(pain_hits), author_type_guess)

    matched = pain_hits + intent_hits + direct_hits + brand_hits + first_person_hits
    seen: list[str] = []
    matched_keywords: list[str] = []
    for keyword in matched:
        if keyword not in seen:
            seen.append(keyword)
            matched_keywords.append(keyword)

    pain_detected = sorted({kw for kw in pain_hits + first_person_hits})
    icp_detected = sorted({kw for kw in direct_hits + brand_hits})

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
    if segment == "ecom_direct_growth" and (direct_hits or change_event_hits):
        segment_bonus += 3
    if segment == "manufacturer_secondary" and brand_hits:
        segment_bonus += 3

    final_lead_score = (
        icp_score
        + pain_score
        + intent_score
        + context_score
        + conversation_score
        + owner_likelihood_score
        + contactability_score
        + segment_bonus
        - promo_penalty
        - contractor_penalty
    )

    signal_score = max(final_lead_score, 0)

    if signal_score >= 18:
        signal_level = "high"
    elif signal_score >= 10:
        signal_level = "medium"
    else:
        signal_level = "low"

    lead_fit, next_step = _build_lead_fit(
        message_type=message_type,
        author_type_guess=author_type_guess,
        signal_score=signal_score,
        first_person_pain_score=first_person_pain_score,
        contactability_score=contactability_score,
        conversation_score=conversation_score,
        pain_score=pain_score,
        icp_score=icp_score,
        intent_score=intent_score,
    )
    is_actionable = lead_fit == "target"

    reasons = []
    if first_person_hits:
        reasons.append("есть first-person сигнал боли/запроса")
    if pain_hits:
        reasons.append("есть боль по экономике/марже/зависимости")
    if intent_hits:
        reasons.append("есть запрос на решение или подрядчика")
    if change_event_hits:
        reasons.append("есть change-event: компания явно что-то меняет")
    if direct_hits:
        reasons.append("есть интерес к сайту или direct-каналу")
    if brand_hits:
        reasons.append("есть признаки бренда или производства")
    if author_type_guess == "business":
        reasons.append("текст больше похож на бизнес, чем на подрядчика")
    if conversation_score >= 2:
        reasons.append("сигнал подтверждается цепочкой обсуждения")
    if contactability_score >= 3:
        reasons.append("есть прямой контакт автора")
    elif contactability_score >= 1:
        reasons.append("есть хотя бы косвенный канал контакта")

    why = "; ".join(reasons[:5])

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
        "conversation_score": conversation_score,
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
        "pain_detected": ",".join(pain_detected),
        "icp_detected": ",".join(icp_detected),
        "why_actionable": why,
        "recommended_opener": build_recommended_opener(full_l, message_type, lead_fit),
    }
