def build_hypothesis(title: str | None, is_icp: bool, company_name: str | None = None) -> str | None:
    if not is_icp:
        return None

    text = f"{title or ''} {company_name or ''}".lower()

    if any(word in text for word in ["магазин", "каталог", "бренд", "купить", "официальный сайт"]):
        return "high_cac_or_scaling_plateau"

    if any(word in text for word in ["оптом", "производство", "поставщик", "b2b", "для бизнеса"]):
        return "no_system_leadgen"

    return "need_manual_review"
