from __future__ import annotations

import json
from pathlib import Path


DEFAULT_EXHIBITION_TEMPLATES = [
    "участники продэкспо производитель официальный сайт",
    "участники intercharm российский бренд официальный сайт",
    "участники worldfood производитель официальный сайт",
    "участники household expo производитель официальный сайт",
]

DEFAULT_CATEGORY_TEMPLATES = [
    "производитель [категория] официальный сайт",
    "российский бренд [категория] интернет-магазин",
    "[категория] собственное производство",
    "производитель [категория] где купить",
    "бренд [категория] wildberries ozon официальный сайт",
]

CONFIG_PATH = Path("storage/web_query_templates.json")


def _lines(text: str | None) -> list[str]:
    return [line.strip() for line in (text or "").splitlines() if line.strip()]


def load_query_templates() -> dict[str, list[str]]:
    if not CONFIG_PATH.exists():
        return {
            "exhibition_templates": DEFAULT_EXHIBITION_TEMPLATES,
            "category_templates": DEFAULT_CATEGORY_TEMPLATES,
        }

    try:
        data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        data = {}

    exhibition_templates = data.get("exhibition_templates") or DEFAULT_EXHIBITION_TEMPLATES
    category_templates = data.get("category_templates") or DEFAULT_CATEGORY_TEMPLATES
    return {
        "exhibition_templates": [str(item).strip() for item in exhibition_templates if str(item).strip()],
        "category_templates": [str(item).strip() for item in category_templates if str(item).strip()],
    }


def save_query_templates(*, exhibition_templates_text: str, category_templates_text: str) -> dict[str, list[str]]:
    data = {
        "exhibition_templates": _lines(exhibition_templates_text) or DEFAULT_EXHIBITION_TEMPLATES,
        "category_templates": _lines(category_templates_text) or DEFAULT_CATEGORY_TEMPLATES,
    }
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return data


def render_category_queries(category: str, templates: list[str] | None = None) -> list[str]:
    value = (category or "").strip()
    if not value:
        return []
    raw_templates = templates or load_query_templates()["category_templates"]
    return [template.replace("[категория]", value).strip() for template in raw_templates if template.strip()]
