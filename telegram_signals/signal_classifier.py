from .keywords import (
    PRIMARY_ECOM_KEYWORDS,
    PAIN_KEYWORDS,
    DIRECT_KEYWORDS,
    INTENT_KEYWORDS,
    NEGATIVE_KEYWORDS,
)


def classify_signal(text: str):
    text_l = text.lower()

    score = 0
    matches = []

    for kw in PRIMARY_ECOM_KEYWORDS:
        if kw in text_l:
            score += 2
            matches.append(kw)

    for kw in PAIN_KEYWORDS:
        if kw in text_l:
            score += 3
            matches.append(kw)

    for kw in DIRECT_KEYWORDS:
        if kw in text_l:
            score += 3
            matches.append(kw)

    for kw in INTENT_KEYWORDS:
        if kw in text_l:
            score += 4
            matches.append(kw)

    for kw in NEGATIVE_KEYWORDS:
        if kw in text_l:
            score -= 5
            matches.append(kw)

    if score >= 8:
        level = "high"
    elif score >= 4:
        level = "medium"
    else:
        level = "low"

    return {
        "score": score,
        "level": level,
        "matches": matches,
    }