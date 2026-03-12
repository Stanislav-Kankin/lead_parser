TEMPLATES = [
    "{q}",
    "интернет магазин {q}",
    "{q} оптом",
    "поставщик {q}",
    "производитель {q}",
    "бренд {q}",
    "{q} официальный сайт",
    "{q} для бизнеса",
]


def build_queries(base_query: str) -> list[str]:
    q = (base_query or "").strip()
    if not q:
        return []

    seen = set()
    result = []
    for template in TEMPLATES:
        value = template.format(q=q).strip()
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(value)
    return result
