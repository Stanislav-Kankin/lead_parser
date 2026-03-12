NEGATIVE_WORDS = {
    "википедия": -4,
    "обзор": -3,
    "отзывы": -3,
    "форум": -4,
    "новости": -3,
    "сравнение": -2,
    "маркетплейс": -5,
    "wildberries": -5,
    "ozon": -5,
    "avito": -5,
}

ECOMMERCE_WORDS = {
    "интернет-магазин": -3,
    "интернет магазин": -3,
    "магазин": -2,
    "купить": -3,
    "каталог": -1,
    "доставка": -1,
    "розница": -2,
}

B2B_WORDS = {
    "для бизнеса": 2,
    "оптом": 2,
    "оптовый": 2,
    "производство": 3,
    "производитель": 3,
    "завод": 3,
    "поставщик": 2,
    "дистрибьютор": 2,
    "b2b": 3,
    "корпоративным клиентам": 2,
    "решения для бизнеса": 2,
    "контрактное производство": 3,
}

BRAND_WORDS = {
    "бренд": 1,
    "торговая марка": 2,
    "официальный сайт": 1,
}


def classify_icp(
    title: str | None,
    domain: str,
    company_name: str | None = None,
    description: str | None = None,
    h1: str | None = None,
    text: str | None = None,
    has_contacts: bool = False,
) -> tuple[bool, str, int, str, str]:
    corpus = " ".join(
        part for part in [title or "", company_name or "", description or "", h1 or "", text or "", domain]
        if part
    ).lower()

    score = 0
    matched_positive: list[str] = []
    matched_negative: list[str] = []

    for word, weight in NEGATIVE_WORDS.items():
        if word in corpus:
            score += weight
            matched_negative.append(word)

    for word, weight in ECOMMERCE_WORDS.items():
        if word in corpus:
            score += weight
            matched_negative.append(word)

    for word, weight in B2B_WORDS.items():
        if word in corpus:
            score += weight
            matched_positive.append(word)

    for word, weight in BRAND_WORDS.items():
        if word in corpus:
            score += weight
            matched_positive.append(word)

    if has_contacts:
        score += 1
        matched_positive.append("contacts")

    if score >= 4:
        lead_type = "manufacturer_or_b2b"
        priority = "high"
    elif score >= 2:
        lead_type = "possible_icp"
        priority = "medium"
    else:
        lead_type = "low_relevance"
        priority = "low"

    is_icp = score >= 2

    if matched_positive and not matched_negative:
        reason = "positive:" + ",".join(matched_positive[:4])
    elif matched_positive or matched_negative:
        reason = "positive:" + ",".join(matched_positive[:3]) + " | negative:" + ",".join(matched_negative[:3])
    else:
        reason = "not_enough_signals"

    return is_icp, reason.strip(" |"), score, lead_type, priority
