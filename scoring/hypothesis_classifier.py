HYPOTHESIS_LABELS = {
    "high_cac_or_scaling_plateau": "Вероятно, упёрлись в рост CAC или потолок масштабирования.",
    "no_system_leadgen": "Похоже, нет системного direct-канала и управляемого притока спроса.",
    "marketplace_dependency": "Есть риск сильной зависимости от маркетплейсов или партнёрских каналов.",
    "need_manual_review": "Нужна ручная проверка бизнес-модели и текущего канала продаж.",
}

OPENER_LABELS = {
    "high_cac_or_scaling_plateau": "Видел ваш вопрос про рост. У брендов с хорошим performance часто в этой точке возникает не вопрос «как оптимизировать», а «где следующий слой роста». Интересно понять - вы сейчас это как формулируете для себя?",
    "no_system_leadgen": "Судя по контексту - у вас есть продукт и продажи, но прямого управляемого канала спроса нет. Это нормальная точка для производителей. Если интересно - могу прислать 2-3 вопроса, которые обычно помогают понять, где именно потолок.",
    "marketplace_dependency": "Интересно читать - это ровно то место, где у производителей часто ломается экономика роста. Не агрессивно продаю: просто если интересно сверить гипотезу про вашу модель - готов коротко.",
    "need_manual_review": "Похоже, здесь важно не выбирать инструмент, а сначала понять, где именно упирается модель роста. Если интересно - могу коротко сверить гипотезу без продажи.",
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
