from __future__ import annotations

import json
import os
from pathlib import Path
from threading import Lock


DEFAULT_OUTREACH_TEMPLATES = {
    "universal": {
        "label": "Универсальный",
        "text": (
            "{имя}, добрый день. Посмотрел компанию {компания}. Мы помогаем производителям и брендам "
            "искать дополнительные точки роста без резких изменений текущей модели продаж. Подскажите, "
            "у вас сейчас актуальна задача развивать собственный спрос или direct-канал?"
        ),
    },
    "owner": {
        "label": "Собственник / CEO",
        "text": (
            "{имя}, добрый день. Вижу, что вы развиваете {компания}. У компаний с собственным продуктом "
            "на определённом этапе возникает вопрос, как расти дальше и не усиливать зависимость от одной "
            "площадки. У вас сейчас такая задача обсуждается?"
        ),
    },
    "marketing": {
        "label": "Маркетинг / e-commerce",
        "text": (
            "{имя}, добрый день. Обратил внимание на ваш опыт в {компания}. Часто performance продолжает "
            "работать, но каждый следующий шаг по бюджету даёт всё меньший эффект. У вас сейчас больше задача "
            "оптимизации текущих каналов или уже ищете следующий источник роста?"
        ),
    },
    "marketplaces": {
        "label": "Маркетплейсы",
        "text": (
            "{имя}, добрый день. Посмотрел {компания}. У брендов, которые развивают продажи на маркетплейсах, "
            "часто появляется задача снижать зависимость от ставок и комиссий, не ломая работающий канал. "
            "Подскажите, внешний спрос или direct у вас уже в фокусе?"
        ),
    },
}

_LOCK = Lock()


def load_outreach_templates() -> dict[str, dict[str, str]]:
    result = {key: dict(value) for key, value in DEFAULT_OUTREACH_TEMPLATES.items()}
    path = _templates_path()
    if not path.exists():
        return result
    try:
        saved = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return result
    if not isinstance(saved, dict):
        return result
    for key, value in saved.items():
        if key in result and isinstance(value, str) and value.strip():
            result[key]["text"] = value.strip()
    return result


def save_outreach_template(key: str, text: str) -> dict[str, dict[str, str]]:
    key = str(key or "").strip()
    clean_text = str(text or "").strip()[:4000]
    if key not in DEFAULT_OUTREACH_TEMPLATES:
        raise ValueError("Неизвестный шаблон захода")
    if not clean_text:
        raise ValueError("Черновик не может быть пустым")

    path = _templates_path()
    with _LOCK:
        current = load_outreach_templates()
        saved = {template_key: value["text"] for template_key, value in current.items()}
        saved[key] = clean_text
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_suffix(".tmp")
        temp_path.write_text(json.dumps(saved, ensure_ascii=False, indent=2), encoding="utf-8")
        temp_path.replace(path)
    return load_outreach_templates()


def _templates_path() -> Path:
    configured = os.getenv("PEOPLE_OUTREACH_TEMPLATES_PATH", "data/people_outreach_templates.json")
    return Path(configured)
