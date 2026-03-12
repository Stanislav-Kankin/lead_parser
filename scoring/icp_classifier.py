from typing import Any

POSITIVE_SIGNALS = {
    "производитель": 4,
    "производство": 3,
    "завод": 3,
    "фабрика": 3,
    "оптом": 2,
    "оптовый": 2,
    "поставщик": 2,
    "дистрибьютор": 2,
    "b2b": 3,
    "для бизнеса": 2,
    "контрактное производство": 4,
    "private label": 4,
    "бренд": 1,
    "официальный сайт": 1,
}

NEGATIVE_SIGNALS = {
    "интернет-магазин": -5,
    "интернет магазин": -5,
    "магазин": -4,
    "купить": -4,
    "цена": -3,
    "цены": -3,
    "отзывы": -3,
    "обзор": -4,
    "новости": -2,
    "сравнение": -4,
    "маркетплейс": -4,
    "каталог": -3,
    "доставка": -2,
}

TYPE_MAP = {
    "manufacturer_or_b2b": "Производитель / B2B",
    "possible_icp": "Потенциальный ICP",
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

    is_icp = score >= 2

    if score >= 4:
        lead_type = "manufacturer_or_b2b"
        priority = "high"
    elif score >= 2:
        lead_type = "possible_icp"
        priority = "medium"
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
