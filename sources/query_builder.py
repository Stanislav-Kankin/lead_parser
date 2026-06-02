from __future__ import annotations

from sources.web_query_templates import load_query_templates


ICP1_PRESETS = {
    "fmcg": [
        "российский производитель продуктов питания официальный сайт",
        "производитель напитков официальный сайт интернет-магазин",
        "бренд продуктов питания собственное производство",
        "производитель снеков официальный сайт",
    ],
    "beauty": [
        "российский бренд косметики официальный интернет-магазин",
        "производитель косметики собственное производство",
        "бренд уходовой косметики официальный сайт",
        "производитель бытовой химии официальный сайт",
    ],
    "household": [
        "производитель товаров для дома официальный сайт",
        "российский бренд товаров для дома интернет-магазин",
        "производитель посуды официальный сайт",
        "производитель текстиля для дома официальный сайт",
    ],
    "kids": [
        "производитель детских товаров официальный сайт",
        "российский бренд детских товаров интернет-магазин",
        "производитель игрушек официальный сайт",
    ],
    "fashion": [
        "российский бренд одежды официальный интернет-магазин",
        "производитель одежды собственное производство",
        "бренд обуви официальный сайт",
    ],
    "marketplace_brand": [
        "бренд wildberries официальный сайт производитель",
        "бренд ozon официальный сайт производитель",
        "производитель на wildberries официальный сайт",
        "производитель на ozon официальный сайт",
    ],
    "exhibitors": [
        "участники продэкспо производитель официальный сайт",
        "участники intercharm российский бренд официальный сайт",
        "участники worldfood производитель официальный сайт",
        "участники household expo производитель официальный сайт",
    ],
}

NEGATIVE_SUFFIX = "-маркетплейс -wildberries -ozon -avito -отзывы -обзор -вакансии -pdf"


def preset_queries(preset: str = "all") -> list[str]:
    if preset == "exhibitors":
        return load_query_templates()["exhibition_templates"]
    if preset == "all":
        result: list[str] = []
        for name, queries in ICP1_PRESETS.items():
            if name == "exhibitors":
                result.extend(load_query_templates()["exhibition_templates"])
            else:
                result.extend(queries)
        return result
    return ICP1_PRESETS.get(preset, ICP1_PRESETS["fmcg"])


def build_queries(base_query: str | None = None, preset: str = "all") -> list[str]:
    raw_queries = []
    if base_query:
        raw_queries.extend(line.strip() for line in base_query.splitlines() if line.strip())
    if not raw_queries:
        raw_queries = preset_queries(preset)

    result: list[str] = []
    seen: set[str] = set()
    for query in raw_queries:
        variants = [
            query,
            f"{query} производитель",
            f"{query} собственный бренд",
            f"{query} интернет-магазин",
            f"{query} {NEGATIVE_SUFFIX}",
        ]
        for value in variants:
            key = value.lower()
            if key in seen:
                continue
            seen.add(key)
            result.append(value)
    return result[:18]
