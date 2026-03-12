def build_hypothesis(title: str | None, is_icp: bool) -> str | None:
    if not is_icp:
        return None

    text = (title or "").lower()

    if any(word in text for word in ["магазин", "каталог", "бренд", "купить"]):
        return "high_cac_or_scaling_plateau"

    if any(word in text for word in ["оптом", "производство", "поставщик", "b2b"]):
        return "no_system_leadgen"

    return "need_manual_review"
