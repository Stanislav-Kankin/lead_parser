from __future__ import annotations

import json
import os
from pathlib import Path
from threading import Lock


TEMPLATE_VERSION = 2

LEGACY_TEMPLATE_TEXTS = {
    "universal": (
        "{имя}, добрый день. Посмотрел компанию {компания}. Мы помогаем производителям и брендам "
        "искать дополнительные точки роста без резких изменений текущей модели продаж. Подскажите, "
        "у вас сейчас актуальна задача развивать собственный спрос или direct-канал?"
    ),
    "owner": (
        "{имя}, добрый день. Вижу, что вы развиваете {компания}. У компаний с собственным продуктом "
        "на определённом этапе возникает вопрос, как расти дальше и не усиливать зависимость от одной "
        "площадки. У вас сейчас такая задача обсуждается?"
    ),
    "marketing": (
        "{имя}, добрый день. Обратил внимание на ваш опыт в {компания}. Часто performance продолжает "
        "работать, но каждый следующий шаг по бюджету даёт всё меньший эффект. У вас сейчас больше задача "
        "оптимизации текущих каналов или уже ищете следующий источник роста?"
    ),
    "marketplaces": (
        "{имя}, добрый день. Посмотрел {компания}. У брендов, которые развивают продажи на маркетплейсах, "
        "часто появляется задача снижать зависимость от ставок и комиссий, не ломая работающий канал. "
        "Подскажите, внешний спрос или direct у вас уже в фокусе?"
    ),
}

DEFAULT_OUTREACH_TEMPLATES = {
    "universal": {
        "label": "Универсальный",
        "text": (
            "{имя}, добрый день. Посмотрел {компания} и решил написать по делу. У брендов с собственным "
            "продуктом в какой-то момент встаёт вопрос, как расти дальше и при этом не усиливать зависимость "
            "от одной площадки.\n\nПодскажите, у вас сейчас эта тема вообще актуальна?"
        ),
    },
    "owner": {
        "label": "Собственник / CEO",
        "text": (
            "{имя}, добрый день. Вижу, что вы развиваете {компания}. У собственников на этом этапе часто "
            "появляется выбор: продолжать усиливать текущие каналы или аккуратно проверять дополнительный "
            "спрос вне них.\n\nУ вас сейчас такая задача есть или пока не в приоритете?"
        ),
    },
    "marketing": {
        "label": "Маркетинг / e-commerce",
        "text": (
            "{имя}, добрый день. Обратил внимание на вашу роль в {компания}. Часто performance ещё работает, "
            "но следующий рост требует всё больше бюджета, а результат становится менее предсказуемым.\n\n"
            "У вас сейчас основной фокус на оптимизации текущих каналов или уже смотрите новые точки роста?"
        ),
    },
    "marketplaces": {
        "label": "Маркетплейсы",
        "text": (
            "{имя}, добрый день. Посмотрел {компания}. У брендов на маркетплейсах со временем растут ставки "
            "и комиссии, а масштабировать продажи становится сложнее. Обычно тогда начинают аккуратно "
            "проверять спрос за пределами площадки.\n\nУ вас эта задача уже появилась или пока маркетплейсов хватает?"
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
    saved_version = 1
    saved_templates = saved
    if isinstance(saved.get("templates"), dict):
        saved_version = int(saved.get("version") or 1)
        saved_templates = saved["templates"]
    for key, value in saved_templates.items():
        if key in result and isinstance(value, str) and value.strip():
            if saved_version < TEMPLATE_VERSION and value.strip() == LEGACY_TEMPLATE_TEXTS.get(key):
                continue
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
        payload = {"version": TEMPLATE_VERSION, "templates": saved}
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temp_path.replace(path)
    return load_outreach_templates()


def reset_outreach_templates() -> dict[str, dict[str, str]]:
    path = _templates_path()
    with _LOCK:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_suffix(".tmp")
        payload = {"version": TEMPLATE_VERSION, "templates": {}}
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temp_path.replace(path)
    return load_outreach_templates()


def _templates_path() -> Path:
    configured = os.getenv("PEOPLE_OUTREACH_TEMPLATES_PATH", "data/people_outreach_templates.json")
    return Path(configured)
