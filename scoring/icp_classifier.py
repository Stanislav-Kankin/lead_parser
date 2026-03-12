E_COMMERCE_WORDS = [
    "интернет-магазин",
    "магазин",
    "купить",
    "каталог",
    "доставка",
    "товары",
    "бренд",
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


def classify_icp(title: str | None, domain: str) -> tuple[bool, str]:
    text = f"{title or ''} {domain}".lower()

    if any(word in text for word in E_COMMERCE_WORDS):
        return True, "site_looks_like_ecommerce_or_brand"

    if any(word in text for word in B2B_WORDS):
        return True, "site_looks_like_b2b_or_manufacturer"

    return False, "not_enough_signals"
