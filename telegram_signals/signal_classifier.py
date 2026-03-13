from __future__ import annotations

from .keywords import (
    PRIMARY_ECOM_KEYWORDS,
    PAIN_KEYWORDS,
    INTENT_KEYWORDS,
    MANUFACTURER_KEYWORDS,
    NEGATIVE_KEYWORDS,
)


def _contains_any(text: str, keywords: list[str]) -> list[str]:
    found = []
    for kw in keywords:
        if kw in text:
            found.append(kw)
    return found


def _segment(text: str, matches: list[str]) -> str:
    if any(k in text for k in ["wildberries", "wb", "ozon", "маркетплейс", "маркетплейсы"]):
        if any(k in text for k in ["свой сайт", "интернет-магазин", "direct", "d2c"]):
            return "ecom_marketplace_pain"
        return "ecom_marketplace_pain"
    if any(k in matches for k in ["свой сайт", "интернет-магазин", "direct", "d2c"]):
        return "ecom_direct_growth"
    if any(k in matches for k in MANUFACTURER_KEYWORDS):
        return "manufacturer_secondary"
    return "ecom_direct_growth"


def _opener(segment: str, text: str) -> str:
    if segment == "ecom_marketplace_pain":
        return (
            "Вижу у вас контекст вокруг зависимости от WB/Ozon и экономики собственного сайта. "
            "Часто в такой точке проблема не в самом трафике, а в том, что direct-канал не собран как управляемая система роста."
        )
    if segment == "ecom_direct_growth":
        return (
            "Похоже, у вас уже есть фокус на росте собственного сайта. Обычно здесь упираются в экономику привлечения и отсутствие связки между спросом, сайтом и повторными продажами."
        )
    return (
        "Для производителей и брендов с собственным продуктом часто узкое место — зависимость от внешних площадок и слабый direct-канал. Можно проверить, есть ли у вас здесь точка роста."
    )


def classify_signal(text: str) -> dict:
    text_l = (text or "").lower()

    primary = _contains_any(text_l, PRIMARY_ECOM_KEYWORDS)
    pain = _contains_any(text_l, PAIN_KEYWORDS)
    intent = _contains_any(text_l, INTENT_KEYWORDS)
    manufacturer = _contains_any(text_l, MANUFACTURER_KEYWORDS)
    negative = _contains_any(text_l, NEGATIVE_KEYWORDS)

    score = 0
    score += len(primary) * 2
    score += len(pain) * 3
    score += len(intent) * 4
    score += len(manufacturer) * 1
    score -= len(negative) * 5

    if any(k in text_l for k in ["wildberries", "wb", "ozon", "маркетплейс", "маркетплейсы"]) and any(k in text_l for k in ["комиссия", "маржа", "зависимость", "невыгодно"]):
        score += 3
    if any(k in text_l for k in ["свой сайт", "интернет-магазин", "direct", "d2c"]) and any(k in text_l for k in ["нужен трафик", "подрядчик", "агентство", "реклама"]):
        score += 3

    if score >= 10:
        level = "high"
    elif score >= 5:
        level = "medium"
    else:
        level = "low"

    matches = primary + pain + intent + manufacturer + negative
    segment = _segment(text_l, matches)
    return {
        "score": score,
        "level": level,
        "matches": matches,
        "segment": segment,
        "opener": _opener(segment, text_l),
    }
