BASE_TEMPLATES = [
    "{q}",
    "{q} производитель",
    "{q} производство",
    "{q} оптом",
    "{q} оптовый поставщик",
    "{q} b2b",
    "поставщик {q}",
    "дистрибьютор {q}",
    "завод {q}",
    "бренд {q}",
]

NEGATIVE_SUFFIXES = [
    "-маркетплейс -wildberries -ozon -avito -youtube -vk",
    "-интернет-магазин -розница -отзывы -обзор",
]


def build_queries(base_query: str) -> list[str]:
    q = (base_query or "").strip()
    if not q:
        return []

    result: list[str] = []
    seen: set[str] = set()

    for template in BASE_TEMPLATES:
        base = template.format(q=q).strip()
        variants = [base]
        variants.extend(f"{base} {suffix}" for suffix in NEGATIVE_SUFFIXES)
        for value in variants:
            key = value.lower()
            if key in seen:
                continue
            seen.add(key)
            result.append(value)

    return result[:10]
