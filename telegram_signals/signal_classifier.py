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
    "у меня",
    "мой",
    "мои",
    "наши",
    "наш",
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

EDITORIAL_PATTERNS = [
    "мы подготовили",
    "делимся",
    "рассказываем",
    "смотрите",
    "в статье",
    "в материале",
    "новость",
    "новости",
    "подборка",
    "гайд",
    "разбор",
    "читайте",
    "вышел материал",
    "вышла статья",
    "в нашем канале",
    "подписывайтесь",
    "ставьте реакции",
    "собрали для вас",
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
    "поставьте реакцию",
    "подписка",
]

OFFICIAL_MARKETPLACE_PATTERNS = [
    "официальный канал",
    "новости wildberries",
    "новости ozon",
    "скидки",
    "распродажа",
    "промокод",
    "товары дня",
    "акция",
    "доставка",
    "покупатели",
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

OWNER_ROLE_PATTERNS = [
    "владелец",
    "собственник",
    "основатель",
    "сооснователь",
    "директор",
    "гендир",
    "ceo",
    "founder",
]

BUSINESS_SCOPE_PATTERNS = [
    "интернет-магазин",
    "магазин",
    "производство",
    "бренд",
    "товар",
    "продажи",
    "маркетплейс",
    "селлер",
    "seller",
    "wb",
    "ozon",
    "сайт",
    "direct",
]

MARKETING_PAIN_PATTERNS = [
    "дорогой трафик",
    "трафик дорогой",
    "не окупается",
    "не сходится",
    "сливаем бюджет",
    "реклама не окупается",
    "маржа падает",
    "комиссия съедает",
    "зависим от wb",
    "зависим от ozon",
    "зависимость от маркетплейсов",
    "хотим свой сайт",
    "хотим direct",
    "как лить трафик",
    "нужен трафик",
    "реклама идет",
    "продажи остановились",
    "продаж нет",
    "позиции есть",
    "трафик есть",
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

LIVE_HELP_PATTERNS = [
    "кто-нибудь может помочь",
    "кто-нибудь может подсказать",
    "подскажите",
    "помогите разобраться",
    "кто сталкивался",
    "в чем может быть причина",
    "в чём может быть причина",
    "что делаем не так",
    "не понимаем",
    "есть ли смысл",
    "как выйти",
    "как масштабировать",
    "как снизить",
    "как запустить",
    "кто посоветует",
    "посоветуйте",
]

OPERATIONAL_PAIN_PATTERNS = [
    "у нас продажи",
    "у нас товар",
    "у нас склад",
    "реклама идет",
    "реклама идёт",
    "продажи остановились",
    "позиции есть",
    "карточка в топе",
    "трафик есть",
    "продаж нет",
    "на складе",
    "маржа",
    "комиссия",
    "окупаемость",
    "сайт",
    "direct",
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


PERSON_NAME_RE = re.compile(r"[a-zа-яё]{3,}[\._-][a-zа-яё]{3,}", flags=re.I)
CHANNEL_HANDLE_HINTS = (
    "wb",
    "ozon",
    "market",
    "seller",
    "ecom",
    "business",
    "biz",
    "brand",
    "agency",
    "media",
    "news",
    "store",
    "shop",
    "marketplace",
    "export",
    "ppt",
    "design",
    "promo",
)
PERSON_AUTHOR_HINTS = (
    "эксперт",
    "агентство",
    "команда",
    "редакция",
    "канал",
    "медиа",
    "студия",
)


def _clean_handle(value: str | None) -> str:
    return (value or "").strip().lstrip("@").lower()


def _looks_like_person_name(value: str | None) -> bool:
    text = (value or "").strip()
    if not text:
        return False
    if any(token in text.lower() for token in PERSON_AUTHOR_HINTS):
        return False
    parts = [part for part in re.split(r"\s+", text) if part]
    if len(parts) >= 2 and all(re.search(r"[A-Za-zА-Яа-яЁё]{2,}", part) for part in parts[:2]):
        return True
    return False


def _looks_like_channel_handle(handle: str) -> bool:
    if not handle:
        return False
    if handle.endswith("bot"):
        return False
    if any(token in handle for token in CHANNEL_HANDLE_HINTS):
        return True
    if handle.count("_") >= 2:
        return True
    return False


def _classify_contact_entity(
    *,
    author_username: str | None,
    chat_username: str | None,
    author_name: str | None,
    chat_title: str | None,
    reply_depth: int,
    message_type: str,
) -> tuple[str, int, int]:
    author_handle = _clean_handle(author_username)
    chat_handle = _clean_handle(chat_username)
    title_l = (chat_title or "").lower()
    person_name = _looks_like_person_name(author_name)
    handle_person_like = bool(author_handle and PERSON_NAME_RE.search(author_handle))

    if author_handle.endswith("bot") or chat_handle.endswith("bot"):
        return "bot", -25, 0

    if author_handle and chat_handle and author_handle == chat_handle:
        return "channel", -16, 0

    if author_handle:
        if _looks_like_channel_handle(author_handle) and not person_name and not handle_person_like:
            return "channel", -14, 0
        if person_name or handle_person_like:
            return "person", 8 if reply_depth >= 1 else 5, 1
        if reply_depth >= 1 and message_type in {"participant_pain", "self_pain", "peer_question"}:
            return "person", 7, 1
        return "unknown", 2, 0

    if chat_handle:
        if "чат" in title_l or "форум" in title_l or reply_depth >= 1:
            return "unknown", -3, 0
        return "channel", -10, 0

    return "unknown", 0, 0


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


def _extract_website_hint(text: str) -> str | None:
    match = re.search(r"(?:https?://)?(?:www\.)?([a-z0-9][a-z0-9\-]{1,62}\.[a-z]{2,})(?:/[^\s]*)?", text, flags=re.I)
    if not match:
        return None
    return match.group(1).lower()


def _guess_author_type(full_l: str) -> tuple[str, int, int]:
    owner_score = 0
    contractor_penalty = 0

    owner_hits = [p for p in OWNER_CONTEXT_PATTERNS if p in full_l]
    role_hits = [p for p in OWNER_ROLE_PATTERNS if p in full_l]
    business_hits = [p for p in BUSINESS_HINT_KEYWORDS if p in full_l]
    contractor_hits = [p for p in CONTRACTOR_HINT_KEYWORDS if p in full_l]
    contractor_strong_hits = [p for p in CONTRACTOR_STRONG_PATTERNS if p in full_l]

    owner_score += len(owner_hits) * 3
    owner_score += len(role_hits) * 2
    owner_score += len(business_hits)

    contractor_penalty += len(contractor_hits) * 2
    contractor_penalty += len(contractor_strong_hits) * 4

    if contractor_penalty >= owner_score + 3:
        return "contractor", max(owner_score, 0), contractor_penalty
    if owner_score >= 3:
        return "business", owner_score, contractor_penalty
    return "unknown", owner_score, contractor_penalty


def _guess_conversation_type(full_l: str, has_intent: bool, has_pain: bool, author_type_guess: str) -> str:
    if has_pain and has_intent:
        return "help_request"
    if has_pain:
        return "complaint"
    if has_intent:
        return "question"
    if author_type_guess == "business":
        return "discussion"
    return "broadcast"


def _detect_primary_pain_tag(text_l: str) -> str | None:
    if any(x in text_l for x in ["не окупается", "сливаем бюджет", "дорогой трафик", "дорогая реклама"]):
        return "ads_not_profitable"
    if any(x in text_l for x in ["зависим от wb", "зависим от ozon", "зависимость от маркетплейсов"]):
        return "marketplace_dependency"
    if any(x in text_l for x in ["маржа падает", "комиссия съедает", "экономика", "маржа"]):
        return "margin_pressure"
    if any(x in text_l for x in ["хотим свой сайт", "хотим direct", "запускаем сайт"]):
        return "direct_growth_need"
    if any(x in text_l for x in CHANGE_EVENT_PATTERNS):
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
        if message_type == "participant_pain":
            return (
                "Вижу ваш комментарий с живой болью. Я бы заходил аккуратно через гипотезу: где именно сейчас ломается "
                "экономика маркетинга и есть ли зависимость от одного канала продаж."
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
    contact_entity_type: str,
    is_person_reachable: int,
    conversation_score: int,
    pain_score: int,
    icp_score: int,
    intent_score: int,
    participant_score: int,
    live_help_score: int,
    operational_score: int,
    editorial_penalty: int,
) -> tuple[str, str]:
    if message_type in {"service_ad", "vacancy", "supplier_ad"} or author_type_guess == "contractor":
        return "contractor", "ignore"

    if contact_entity_type == "bot":
        return "noise", "ignore"

    if message_type in {"expert_content", "market_intelligence", "noise"} and editorial_penalty >= 4:
        return "noise", "ignore"

    strong_target = (
        message_type in {"self_pain", "participant_pain"}
        and pain_score >= 8
        and (live_help_score >= 4 or operational_score >= 3 or intent_score >= 4)
        and (icp_score >= 4 or participant_score >= 4)
        and contactability_score >= 1
        and is_person_reachable == 1
        and contact_entity_type not in {"channel", "bot"}
        and editorial_penalty < 4
    )
    participant_target = (
        message_type == "participant_pain"
        and first_person_pain_score >= 6
        and pain_score >= 8
        and (conversation_score >= 3 or live_help_score >= 4 or operational_score >= 3)
        and is_person_reachable == 1
        and contact_entity_type not in {"channel", "bot"}
        and editorial_penalty < 4
    )

    if strong_target or participant_target:
        return "target", "outreach_now"

    if message_type in {"self_pain", "participant_pain"}:
        if contact_entity_type == "channel":
            if pain_score >= 8 and (live_help_score >= 4 or operational_score >= 3) and editorial_penalty < 4:
                return "review", "research_company"
            return "noise", "ignore"
        if (pain_score >= 6 and (live_help_score >= 2 or operational_score >= 2 or intent_score >= 2)) or (intent_score >= 4 and icp_score >= 2) or (live_help_score >= 3 and icp_score >= 4):
            return "review", "research_company"
        return "noise", "ignore"

    if message_type == "peer_question":
        if contact_entity_type == "channel":
            return "noise", "ignore"
        if live_help_score >= 2 and icp_score >= 2 and editorial_penalty < 4:
            return "review", "research_company"
        return "noise", "ignore"

    if first_person_pain_score >= 4 and signal_score >= 10 and (pain_score >= 5 or intent_score >= 3):
        return "review", "research_company"

    return "noise", "ignore"


def classify_signal(
    text: str,
    segment: str,
    *,
    context_text: str = "",
    conversation_text: str = "",
    author_username: str | None = None,
    author_name: str | None = None,
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
    chat_username_l = (chat_username or "").lower()
    chat_haystack = f"{chat_title_l} {chat_username_l}".strip()

    pain_hits = _contains_any(text_l, PAIN_KEYWORDS)
    intent_hits = _contains_any(text_l, INTENT_KEYWORDS)
    direct_hits = _contains_any(full_l, DIRECT_KEYWORDS)
    brand_hits = _contains_any(full_l, BRAND_KEYWORDS)
    noise_hits = _contains_any(text_l, NOISE_KEYWORDS)

    author_type_guess, owner_likelihood_score, contractor_penalty = _guess_author_type(full_l)

    first_person_hits = [p for p in FIRST_PERSON_PAIN_PATTERNS if p in text_l]
    expert_hits = [p for p in EXPERT_CONTENT_PATTERNS if p in text_l]
    editorial_hits = [p for p in EDITORIAL_PATTERNS if p in text_l]
    market_hits = [p for p in MARKET_OBSERVATION_PATTERNS if p in text_l]
    supplier_hits = [p for p in SUPPLIER_AD_PATTERNS if p in text_l]
    channel_hits = [p for p in CHANNEL_AUTHOR_PATTERNS if p in text_l]
    official_hits = [p for p in OFFICIAL_MARKETPLACE_PATTERNS if p in text_l or p in chat_haystack]
    change_event_hits = [p for p in CHANGE_EVENT_PATTERNS if p in text_l]
    weak_review_hits = [p for p in WEAK_REVIEW_PATTERNS if p in text_l]
    owner_role_hits = [p for p in OWNER_ROLE_PATTERNS if p in full_l]
    business_scope_hits = [p for p in BUSINESS_SCOPE_PATTERNS if p in full_l]
    marketing_pain_hits = [p for p in MARKETING_PAIN_PATTERNS if p in text_l]
    live_help_hits = [p for p in LIVE_HELP_PATTERNS if p in text_l]
    operational_hits = [p for p in OPERATIONAL_PAIN_PATTERNS if p in text_l]

    first_person_pain_score = len(first_person_hits) * 3
    live_help_score = len(live_help_hits) * 3
    operational_score = len(operational_hits)

    participant_score = 0
    if reply_depth >= 1:
        participant_score += 2
    if reply_depth >= 1 and first_person_hits:
        participant_score += 3
    if reply_depth >= 1 and (pain_hits or marketing_pain_hits or live_help_hits):
        participant_score += 2

    pain_score = len(pain_hits) * 3 + len(marketing_pain_hits) * 2 + first_person_pain_score + live_help_score + operational_score
    intent_score = len(intent_hits) * 4 + len(change_event_hits) * 2 + len(live_help_hits) * 2
    icp_score = len(direct_hits) * 2 + len(brand_hits) * 2 + len(owner_role_hits) * 2 + len(business_scope_hits)

    if any(x in full_l for x in ["wb", "ozon", "маркетплейс", "селлер", "seller", "sku", "карточк", "интернет-магазин", "сайт"]):
        icp_score += 2

    context_score = 0
    if context_text.strip():
        context_score += 2
    if conversation_text.strip() and conversation_text.strip() != text.strip():
        context_score += 2
    if reply_depth >= 1:
        context_score += min(reply_depth, 3)
    if live_help_hits:
        context_score += 2

    conversation_score = 0
    if reply_depth >= 1:
        conversation_score += 2
    if context_text.strip():
        conversation_score += 1
    if pain_hits and (intent_hits or live_help_hits):
        conversation_score += 2
    if first_person_hits and (pain_hits or direct_hits or brand_hits or change_event_hits or marketing_pain_hits or live_help_hits):
        conversation_score += 2
    if participant_score >= 4:
        conversation_score += 2

    editorial_penalty = len(editorial_hits) * 3 + len(channel_hits) * 2 + len(official_hits) * 3
    promo_penalty = editorial_penalty
    if any(x in text_l for x in ["напишите в лс", "есть кейсы", "помогаем", "делаем под ключ", "инфографика", "дизайн карточек"]):
        promo_penalty += 6
    if weak_review_hits:
        promo_penalty += 5
    promo_penalty += len(expert_hits) * 2
    if "рекламщик" in chat_title_l or "capital" in chat_title_l:
        promo_penalty += 3

    has_live_problem = bool(first_person_hits or live_help_hits or operational_hits)
    is_editorial = editorial_penalty >= 4 or bool(editorial_hits or channel_hits or official_hits)

    if noise_hits:
        message_type = "noise"
    elif "вакан" in text_l or "резюме" in text_l:
        message_type = "vacancy"
    elif supplier_hits or weak_review_hits:
        message_type = "supplier_ad"
    elif is_editorial and not has_live_problem:
        message_type = "expert_content"
    elif author_type_guess == "contractor" and not pain_hits and not intent_hits and not change_event_hits and not live_help_hits:
        message_type = "service_ad"
    elif reply_depth >= 1 and has_live_problem and (pain_hits or intent_hits or direct_hits or brand_hits or change_event_hits or marketing_pain_hits or live_help_hits):
        message_type = "participant_pain"
    elif has_live_problem and (pain_hits or intent_hits or direct_hits or brand_hits or change_event_hits or marketing_pain_hits or live_help_hits):
        message_type = "self_pain"
    elif live_help_hits and (direct_hits or brand_hits or intent_hits or operational_hits):
        message_type = "peer_question"
    elif expert_hits:
        message_type = "expert_content"
    elif market_hits and not has_live_problem:
        message_type = "market_intelligence"
    else:
        message_type = "noise"

    conversation_type = _guess_conversation_type(full_l, bool(intent_hits or live_help_hits), bool(pain_hits or marketing_pain_hits or operational_hits), author_type_guess)

    matched = pain_hits + intent_hits + direct_hits + brand_hits + first_person_hits + change_event_hits + marketing_pain_hits + live_help_hits + operational_hits
    seen: list[str] = []
    matched_keywords: list[str] = []
    for keyword in matched:
        if keyword not in seen:
            seen.append(keyword)
            matched_keywords.append(keyword)

    pain_detected = sorted({kw for kw in pain_hits + first_person_hits + marketing_pain_hits + live_help_hits + operational_hits})
    icp_detected = sorted({kw for kw in direct_hits + brand_hits + business_scope_hits + owner_role_hits})

    contact_entity_type, contact_entity_score, is_person_reachable = _classify_contact_entity(
        author_username=author_username,
        chat_username=chat_username,
        author_name=author_name,
        chat_title=chat_title,
        reply_depth=reply_depth,
        message_type=message_type,
    )

    contactability_score = 0
    contact_hint = None
    if is_person_reachable and author_username:
        contactability_score += 4
        contact_hint = f"@{author_username}"
    elif author_username:
        contactability_score += 1
        contact_hint = f"@{author_username}"
    elif chat_username:
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
        + contact_entity_score
        + participant_score
        + segment_bonus
        - promo_penalty
        - contractor_penalty
    )

    signal_score = max(final_lead_score, 0)

    if signal_score >= 20:
        signal_level = "high"
    elif signal_score >= 12:
        signal_level = "medium"
    else:
        signal_level = "low"

    lead_fit, next_step = _build_lead_fit(
        message_type=message_type,
        author_type_guess=author_type_guess,
        signal_score=signal_score,
        first_person_pain_score=first_person_pain_score,
        contactability_score=contactability_score,
        contact_entity_type=contact_entity_type,
        is_person_reachable=is_person_reachable,
        conversation_score=conversation_score,
        pain_score=pain_score,
        icp_score=icp_score,
        intent_score=intent_score,
        participant_score=participant_score,
        live_help_score=live_help_score,
        operational_score=operational_score,
        editorial_penalty=editorial_penalty,
    )
    is_actionable = lead_fit == "target"

    reasons = []
    if reply_depth >= 1:
        reasons.append("лид найден в обсуждении / комментариях")
    if live_help_hits:
        reasons.append("есть живой запрос на помощь")
    if first_person_hits:
        reasons.append("есть first-person сигнал боли/запроса")
    if pain_hits or marketing_pain_hits or operational_hits:
        reasons.append("есть боль по экономике/рекламе/продажам")
    if intent_hits:
        reasons.append("есть запрос на решение или подрядчика")
    if change_event_hits:
        reasons.append("есть change-event: компания явно что-то меняет")
    if direct_hits:
        reasons.append("есть интерес к сайту или direct-каналу")
    if brand_hits or business_scope_hits or owner_role_hits:
        reasons.append("есть признаки бренда, бизнеса или роли ЛПР")
    if author_type_guess == "business":
        reasons.append("текст больше похож на бизнес, чем на подрядчика")
    if conversation_score >= 2:
        reasons.append("сигнал подтверждается цепочкой обсуждения")
    if is_person_reachable and contactability_score >= 4:
        reasons.append("есть прямой контакт живого автора")
    elif contact_entity_type == "channel":
        reasons.append("контакт ведет скорее на канал, а не на человека")
    elif contact_entity_type == "bot":
        reasons.append("контакт ведет на бота, а не на человека")
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
        "contact_entity_type": contact_entity_type,
        "contact_entity_score": contact_entity_score,
        "is_person_reachable": is_person_reachable,
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
