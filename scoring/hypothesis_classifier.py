HYPOTHESIS_LABELS = {
    "high_cac_or_scaling_plateau": "Вероятно упёрлись в стоимость привлечения и масштабирование",
    "marketplace_dependency": "Похоже есть зависимость от маркетплейсов / чужого спроса",
    "no_system_leadgen": "Похоже нет системного привлечения спроса и лидогенерации",
    "need_manual_review": "Нужна ручная проверка экономики и каналов роста",
}


OPENER_LABELS = {
    "high_cac_or_scaling_plateau": "Как сейчас у вас устроено масштабирование платного трафика и что происходит с CAC при росте бюджета?",
    "marketplace_dependency": "Какая доля спроса у вас идёт через собственные каналы, а какая через маркетплейсы или посредников?",
    "no_system_leadgen": "Как у вас сейчас устроено привлечение новых клиентов: есть системный поток спроса или всё держится на текущих каналах и повторных продажах?",
    "need_manual_review": "Где сейчас главный потолок роста: трафик, конверсия, отдел продаж или экономика канала?",
}


def build_hypothesis(title: str | None, is_icp: bool, company_name: str | None = None, meta_description: str | None = None, icp_score: int = 0, text: str | None = None) -> tuple[str | None, str | None, str | None]:
    if not is_icp:
        return None, None, None

    combined = " ".join(filter(None, [title, company_name, meta_description, text])).lower()

    if any(word in combined for word in ["маркетплейс", "ozon", "wildberries"]):
        code = "marketplace_dependency"
    elif any(word in combined for word in ["оптом", "дистрибьютор", "поставщик", "b2b"]):
        code = "no_system_leadgen"
    elif icp_score >= 5:
        code = "high_cac_or_scaling_plateau"
    else:
        code = "need_manual_review"

    return code, HYPOTHESIS_LABELS[code], OPENER_LABELS[code]
