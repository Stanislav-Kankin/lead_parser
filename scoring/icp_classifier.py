E_COMMERCE_WORDS = [
    "интернет-магазин",
    "интернет магазин",
    "магазин",
    "купить",
    "каталог",
    "доставка",
    "товары",
    "бренд",
    "официальный сайт",
]

B2B_WORDS = [
    "для бизнеса",
    "оптом",
    "производство",
    "производитель",
    "поставщик",
    "услуги для бизнеса",
    "b2b",
]

NEGATIVE_WORDS = [
    "википедия",
    "обзор",
    "отзывы",
    "форум",
    "новости",
    "сравнение",
]


def classify_icp(title: str | None, domain: str, company_name: str | None = None) -> tuple[bool, str]:
    text = f"{title or ''} {domain} {company_name or ''}".lower()

    if any(word in text for word in NEGATIVE_WORDS):
        return False, "looks_like_content_not_company"

    if any(word in text for word in E_COMMERCE_WORDS):
        return True, "site_looks_like_ecommerce_or_brand"

    if any(word in text for word in B2B_WORDS):
        return True, "site_looks_like_b2b_or_manufacturer"

    return False, "not_enough_signals"
