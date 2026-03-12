HYPOTHESIS_LABELS = {
    "high_cac_or_scaling_plateau": "Вероятно, упёрлись в рост CAC или потолок масштабирования.",
    "no_system_leadgen": "Похоже, нет системного direct-канала и управляемого притока спроса.",
    "marketplace_dependency": "Есть риск сильной зависимости от маркетплейсов или партнёрских каналов.",
    "need_manual_review": "Нужна ручная проверка бизнес-модели и текущего канала продаж.",
}

OPENER_LABELS = {
    "high_cac_or_scaling_plateau": "Похоже, у вас уже есть спрос, но масштабирование в performance становится всё дороже. Интересно понять, где сейчас упираетесь в экономику роста?",
    "no_system_leadgen": "Часто у производителей и B2B-компаний в такой точке нет стабильного канала прямого спроса. Есть смысл коротко сверить, насколько у вас маркетинг вообще управляет ростом?",
    "marketplace_dependency": "Если значимая часть продаж завязана на внешние площадки, обычно страдает контроль над спросом и маржой. Есть смысл обсудить, как вы сейчас снижаете эту зависимость?",
    "need_manual_review": "Есть ощущение, что у вас есть потенциал для роста в direct, но без короткого разговора это лучше не додумывать. Могу прислать гипотезу по вашему кейсу в 2–3 пунктах.",
}


def build_hypothesis(title: str | None, is_icp: bool, company_name: str | None = None, text: str | None = None) -> tuple[str | None, str | None]:
    if not is_icp:
        return None, None

    blob = f"{title or ''} {company_name or ''} {text or ''}".lower()

    if any(word in blob for word in ["маркетплейс", "wb", "ozon", "дистрибьютор"]):
        key = "marketplace_dependency"
    elif any(word in blob for word in ["оптом", "производство", "производитель", "factory", "manufacturer"]):
        key = "no_system_leadgen"
    elif any(word in blob for word in ["бренд", "официальный сайт", "для бизнеса"]):
        key = "high_cac_or_scaling_plateau"
    else:
        key = "need_manual_review"

    return HYPOTHESIS_LABELS[key], OPENER_LABELS[key]
