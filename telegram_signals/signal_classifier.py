from telegram_signals.keywords import POSITIVE_KEYWORDS, INTENT_KEYWORDS, NEGATIVE_KEYWORDS


def _contains_any(text_l: str, keywords: list[str]) -> list[str]:
    found = []
    for kw in keywords:
        if kw in text_l:
            found.append(kw)
    return found


def build_recommended_opener(text_l: str, matches: list[str]) -> str:
    if any(x in text_l for x in ["wildberries", "wb", "ozon", "маркетплейс", "маркетплейсы"]):
        return (
            "Вижу у вас обсуждается зависимость от маркетплейсов. "
            "У таких e-commerce брендов часто проблема не в трафике как таковом, "
            "а в том, что собственный сайт не собран как управляемый direct-канал."
        )

    if any(x in text_l for x in ["свой сайт", "интернет-магазин", "d2c", "direct"]):
        return (
            "Похоже, у вас есть задача усилить продажи через собственный сайт. "
            "Часто в такой точке основной резерв — это не просто запуск рекламы, "
            "а сборка управляемого канала direct-продаж."
        )

    if any(x in text_l for x in ["подрядчик", "агентство", "директ", "трафик"]):
        return (
            "Вижу запрос на подрядчика/рост канала. "
            "Можно зайти через короткую гипотезу по экономике трафика и зависимости от маркетплейсов."
        )

    return (
        "Есть сигнал, что у компании может быть задача по росту direct-продаж "
        "или снижению зависимости от маркетплейсов."
    )


def classify_signal(text: str, segment: str) -> dict:
    text_l = (text or "").lower()

    positive = _contains_any(text_l, POSITIVE_KEYWORDS)
    intent = _contains_any(text_l, INTENT_KEYWORDS)
    negative = _contains_any(text_l, NEGATIVE_KEYWORDS)

    score = 0
    score += len(positive) * 2
    score += len(intent) * 4
    score -= len(negative) * 5

    if segment == "ecom_marketplace_pain":
        if any(x in text_l for x in ["wildberries", "wb", "ozon", "маркетплейс", "маркетплейсы"]):
            score += 3
        if any(x in text_l for x in ["комиссия", "маржа", "зависимость", "экономика"]):
            score += 3

    if segment == "ecom_direct_growth":
        if any(x in text_l for x in ["свой сайт", "интернет-магазин", "direct", "d2c"]):
            score += 4

    if segment == "manufacturer_secondary":
        if any(x in text_l for x in ["производитель", "бренд", "опт"]):
            score += 3

    if score >= 10:
        level = "high"
    elif score >= 5:
        level = "medium"
    else:
        level = "low"

    matches = list(dict.fromkeys(positive + intent + negative))

    return {
        "score": score,
        "level": level,
        "matches": matches,
        "recommended_opener": build_recommended_opener(text_l, matches),
        "positive": positive,
        "intent": intent,
        "negative": negative,
    }