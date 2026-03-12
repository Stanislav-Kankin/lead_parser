TEMPLATES = [
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

    queries = [template.format(q=q) for template in TEMPLATES]
    queries.insert(0, q)
    # Убираем дубли с сохранением порядка
    seen = set()
    result = []
    for item in queries:
        normalized = item.lower().strip()
        if normalized in seen:
            continue
        seen.add(normalized)
        result.append(item)
    return result
