from typing import Any

POSITIVE_RULES = {
    "производство": 3,
    "производитель": 3,
    "завод": 3,
    "оптом": 2,
    "оптовый": 2,
    "поставщик": 2,
    "дистрибьютор": 2,
    "b2b": 2,
    "для бизнеса": 2,
    "компания": 1,
    "бренд": 1,
}

NEGATIVE_RULES = {
    "интернет-магазин": -4,
    "интернет магазин": -4,
    "магазин": -3,
    "купить": -3,
    "доставка": -2,
    "отзывы": -2,
    "обзор": -3,
    "сравнение": -3,
    "новости": -2,
    "маркетплейс": -4,
    "каталог": -2,
    "розница": -2,
}


TYPE_LABELS = {
    "manufacturer_or_b2b": "Производитель / B2B",
    "possible_icp": "Потенциальный ICP",
    "low_relevance": "Низкая релевантность",
}

PRIORITY_LABELS = {
    "high": "Высокий",
    "medium": "Средний",
    "low": "Низкий",
}



def _collect_matches(text: str, rules: dict[str, int]) -> list[str]:
    return [word for word in rules if word in text]



def classify_icp(title: str | None, domain: str, company_name: str | None = None, meta_description: str | None = None, text: str | None = None) -> dict[str, Any]:
    full_text = " ".join(filter(None, [title, company_name, meta_description, text, domain])).lower()

    positives = _collect_matches(full_text, POSITIVE_RULES)
    negatives = _collect_matches(full_text, NEGATIVE_RULES)
    score = sum(POSITIVE_RULES[word] for word in positives) + sum(NEGATIVE_RULES[word] for word in negatives)

    is_icp = score >= 2 and not ("интернет-магазин" in negatives and score < 4)

    if score >= 5:
        lead_type = "manufacturer_or_b2b"
        priority = "high"
    elif is_icp:
        lead_type = "possible_icp"
        priority = "medium"
    else:
        lead_type = "low_relevance"
        priority = "low"

    reason_parts = [f"Оценка: {score}"]
    if positives:
        reason_parts.append(f"Плюсы: {', '.join(positives)}")
    if negatives:
        reason_parts.append(f"Минусы: {', '.join(negatives)}")
    if len(reason_parts) == 1:
        reason_parts.append("Сигналов мало")

    return {
        "is_icp": is_icp,
        "icp_reason": "; ".join(reason_parts),
        "icp_score": score,
        "lead_type": lead_type,
        "priority": priority,
        "lead_type_label": TYPE_LABELS[lead_type],
        "priority_label": PRIORITY_LABELS[priority],
        "positives": positives,
        "negatives": negatives,
    }
