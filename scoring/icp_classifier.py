from typing import Any

POSITIVE_SIGNALS = {
    "производитель": 4,
    "производство": 3,
    "собственное производство": 5,
    "завод": 3,
    "фабрика": 3,
    "собственный бренд": 5,
    "бренд": 3,
    "direct to consumer": 4,
    "d2c": 4,
    "собственный сайт": 2,
    "прямые продажи": 3,
    "fmcg": 4,
    "cpg": 4,
    "масштабирование": 2,
    "оптом": 1,
    "оптовый": 1,
    "поставщик": 1,
    "дистрибьютор": 2,
    "b2b": 3,
    "для бизнеса": 2,
    "контрактное производство": 4,
    "private label": 4,
    "официальный сайт": 1,
}

NEGATIVE_SIGNALS = {
    "купить": -4,
    "цена": -3,
    "цены": -3,
    "отзывы": -3,
    "обзор": -4,
    "новости": -2,
    "сравнение": -4,
    "каталог": -3,
    "доставка": -2,
    "дропшиппинг": -4,
    "арбитраж": -3,
    "посредник": -3,
    "перепродажа": -3,
}

TYPE_MAP = {
    "core_icp": "Ядро ICP: бренд / производитель",
    "possible_icp": "Потенциальный ICP",
    "seller_segment": "Селлерский сегмент",
    "low_relevance": "Низкая релевантность",
}

PRIORITY_MAP = {
    "high": "Высокий",
    "medium": "Средний",
    "low": "Низкий",
}


def classify_icp(
    title: str | None,
    domain: str,
    company_name: str | None = None,
    description: str | None = None,
    h1: str | None = None,
    text: str | None = None,
) -> dict[str, Any]:
    full_text = " ".join(
        part for part in [title, description, h1, company_name, domain, text] if part
    ).lower()

    score = 0
    positive_hits: list[str] = []
    negative_hits: list[str] = []

    for signal, weight in POSITIVE_SIGNALS.items():
        if signal in full_text:
            score += weight
            positive_hits.append(signal)

    for signal, weight in NEGATIVE_SIGNALS.items():
        if signal in full_text:
            score += weight
            negative_hits.append(signal)

    is_icp = score >= 3

    if score >= 6:
        lead_type = "core_icp"
        priority = "high"
    elif score >= 3:
        lead_type = "possible_icp"
        priority = "medium"
    elif score >= 1:
        lead_type = "seller_segment"
        priority = "low"
    else:
        lead_type = "low_relevance"
        priority = "low"

    reason_parts = [f"score={score}"]
    if positive_hits:
        reason_parts.append("positive:" + ",".join(dict.fromkeys(positive_hits)))
    if negative_hits:
        reason_parts.append("negative:" + ",".join(dict.fromkeys(negative_hits)))

    return {
        "is_icp": is_icp,
        "icp_reason": "; ".join(reason_parts),
        "lead_type": lead_type,
        "lead_type_ru": TYPE_MAP[lead_type],
        "priority": priority,
        "priority_ru": PRIORITY_MAP[priority],
        "score": score,
    }
