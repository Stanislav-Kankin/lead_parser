B2B_TEMPLATES = [
    "{q}",
    "{q} оптом",
    "{q} производитель",
    "{q} производство",
    "{q} поставщик",
    "{q} оптовый поставщик",
    "{q} b2b",
    "{q} для бизнеса",
    "{q} официальный сайт производитель",
    "{q} manufacturer",
]

NEGATIVE_HINTS = [
    "wildberries",
    "ozon",
    "avito",
    "маркетплейс",
    "купить",
]


def build_queries(base_query: str) -> list[str]:
    q = " ".join((base_query or "").strip().split())
    if not q:
        return []

    seen: set[str] = set()
    result: list[str] = []

    for template in B2B_TEMPLATES:
        value = template.format(q=q).strip()
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(value)

    # Один «очищающий» запрос против мусора выдачи.
    anti_marketplace = q + " " + " ".join(f'-"{word}"' for word in NEGATIVE_HINTS)
    anti_marketplace_key = anti_marketplace.lower()
    if anti_marketplace_key not in seen:
        result.append(anti_marketplace)

    return result
