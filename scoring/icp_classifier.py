from __future__ import annotations

from typing import Any


ICP1_POSITIVE_SIGNALS: dict[str, int] = {
    "производитель": 10,
    "производство": 9,
    "собственное производство": 14,
    "собственный бренд": 14,
    "наш бренд": 10,
    "фабрика": 9,
    "завод": 8,
    "изготавливаем": 7,
    "разрабатываем и производим": 10,
    "контрактное производство": 7,
    "private label": 7,
    "оптовым покупателям": 5,
    "дистрибьюторам": 4,
    "где купить": 4,
    "интернет-магазин": 5,
    "официальный интернет-магазин": 8,
    "каталог продукции": 4,
}

PRODUCT_CATEGORY_SIGNALS: dict[str, int] = {
    "косметика": 8,
    "уход за кожей": 7,
    "бытовая химия": 8,
    "товары для дома": 6,
    "продукты питания": 8,
    "напитки": 6,
    "одежда": 5,
    "обувь": 5,
    "детские товары": 7,
    "зоотовары": 5,
    "парфюмерия": 6,
    "fmcg": 8,
    "cpg": 8,
}

CHANNEL_SIGNALS: dict[str, int] = {
    "wildberries": 8,
    "wb": 6,
    "ozon": 8,
    "яндекс маркет": 6,
    "маркетплейс": 5,
    "маркетплейсы": 5,
    "розничные сети": 5,
    "дилеры": 3,
    "опт": 3,
    "b2b": 3,
    "b2c": 3,
    "d2c": 6,
    "direct": 4,
}

GROWTH_PAIN_PROXY_SIGNALS: dict[str, int] = {
    "запуск интернет-магазина": 7,
    "развитие интернет-магазина": 7,
    "новый сайт": 5,
    "бренд": 3,
    "реклама": 3,
    "партнерство": 3,
    "франшиза": 2,
    "экспорт": 4,
    "выставка": 4,
    "новая линейка": 4,
    "новинки": 2,
}

NEGATIVE_SIGNALS: dict[str, int] = {
    "маркетинговое агентство": -20,
    "агентство": -12,
    "seo": -10,
    "создание сайтов": -18,
    "продвижение сайтов": -18,
    "настройка рекламы": -18,
    "дизайн карточек": -16,
    "фулфилмент": -14,
    "карго": -14,
    "доставка из китая": -14,
    "поставщик из китая": -12,
    "дропшиппинг": -12,
    "маркетплейс для": -8,
    "каталог компаний": -12,
    "отзывы": -8,
    "обзор": -8,
    "вакансии": -10,
    "резюме": -10,
    "b2b портал": -8,
    "портал для производителей": -35,
    "портал": -18,
    "закупщиков": -25,
    "доска объявлений": -25,
    "каталог производителей": -20,
    "форум": -25,
    "клуб": -18,
    "сообщество": -14,
    "конструктор интернет-магазинов": -30,
    "создайте интернет-магазин": -30,
    "создать интернет-магазин": -30,
    "платформа для интернет-магазина": -30,
    "crm": -8,
    "saas": -12,
    "шаблон сайта": -18,
    "разработка интернет-магазина": -22,
    "готовый интернет-магазин": -20,
    "кабель": -12,
    "электротехничес": -12,
    "промышлен": -10,
    "оборудование": -10,
    "станки": -12,
}

TYPE_MAP = {
    "core_icp": "Ядро ICP1: бренд / производитель",
    "possible_icp": "Потенциальный ICP1",
    "seller_segment": "Селлер / ecom-сегмент",
    "low_relevance": "Низкая релевантность",
}

PRIORITY_MAP = {
    "high": "Высокий",
    "medium": "Средний",
    "low": "Низкий",
}


def _hits(full_text: str, signals: dict[str, int]) -> list[tuple[str, int]]:
    return [(signal, weight) for signal, weight in signals.items() if signal in full_text]


def _format_hits(items: list[tuple[str, int]], limit: int = 8) -> str:
    return ", ".join(signal for signal, _ in items[:limit])


def _build_hypothesis(
    *,
    score: int,
    product_hits: list[tuple[str, int]],
    channel_hits: list[tuple[str, int]],
    growth_hits: list[tuple[str, int]],
    has_contacts: bool,
    has_catalog: bool = False,
    has_cart: bool = False,
    site_type: str | None = None,
) -> tuple[str, str, str]:
    product_part = _format_hits(product_hits, 4) or "есть товарная линейка"
    channel_part = _format_hits(channel_hits, 4) or "каналы продаж неочевидны"
    site_part = ""
    if has_catalog and has_cart:
        site_part = " На сайте видны каталог и покупка/корзина, значит direct можно обсуждать предметно."
    elif has_catalog:
        site_part = " На сайте виден каталог, но корзина неочевидна: стоит проверить, как сейчас устроены прямые продажи."
    elif site_type == "leadgen_landing":
        site_part = " Сайт больше похож на лидоген-лендинг: возможная точка входа — как превратить спрос в управляемый direct."

    if channel_hits and growth_hits:
        hypothesis = (
            f"Похоже на бренд/производителя: {product_part}. Есть признаки продаж через {channel_part} "
            "и активности вокруг роста. Вероятная боль: как развивать direct без ломки текущих каналов."
        )
        angle = "Зайти через безопасный аудит модели роста: MP/retail/direct, экономика первого заказа, что можно проверить малым пилотом."
        cjm_stage = "awareness"
    elif channel_hits:
        hypothesis = (
            f"Компания похожа на производителя с несколькими каналами продаж: {product_part}; {channel_part}. "
            "Вероятная боль: зависимость от внешних каналов и слабый контроль клиентской базы."
        )
        angle = "Зайти через гипотезу снижения зависимости от маркетплейсов/retail и теста прямого канала."
        cjm_stage = "awareness"
    else:
        hypothesis = (
            f"Компания похожа на производителя: {product_part}. Нужно проверить, есть ли MP/direct и какой канал сейчас основной."
        )
        angle = "Зайти мягко: уточнить, как сейчас устроены продажи и есть ли задача развивать управляемый спрос."
        cjm_stage = "signal_only"

    if score >= 70 and has_contacts:
        cjm_stage = "consideration"
        angle = "Можно писать как к зрелому ICP: коротко обозначить гипотезу потолка текущей модели и предложить диагностический разбор."

    return hypothesis + site_part, angle, cjm_stage


def classify_icp(
    title: str | None,
    domain: str,
    company_name: str | None = None,
    description: str | None = None,
    h1: str | None = None,
    text: str | None = None,
    has_contacts: bool = False,
    has_catalog: bool = False,
    has_cart: bool = False,
    ecommerce_score: int = 0,
    site_type: str | None = None,
    site_assessment: str | None = None,
) -> dict[str, Any]:
    full_text = " ".join(
        part for part in [title, description, h1, company_name, domain, text] if part
    ).lower()

    icp_hits = _hits(full_text, ICP1_POSITIVE_SIGNALS)
    product_hits = _hits(full_text, PRODUCT_CATEGORY_SIGNALS)
    channel_hits = _hits(full_text, CHANNEL_SIGNALS)
    growth_hits = _hits(full_text, GROWTH_PAIN_PROXY_SIGNALS)
    negative_hits = _hits(full_text, NEGATIVE_SIGNALS)

    raw_score = (
        sum(weight for _, weight in icp_hits)
        + sum(weight for _, weight in product_hits)
        + min(18, sum(weight for _, weight in channel_hits))
        + min(14, sum(weight for _, weight in growth_hits))
        + sum(weight for _, weight in negative_hits)
    )

    if icp_hits and product_hits:
        raw_score += 10
    if channel_hits and product_hits:
        raw_score += 8
    if has_contacts:
        raw_score += 4
    if has_catalog:
        raw_score += 8
    if has_cart:
        raw_score += 6
    if ecommerce_score >= 60:
        raw_score += 8
    elif ecommerce_score >= 35:
        raw_score += 4
    if site_type == "leadgen_landing":
        raw_score -= 5

    score = max(0, min(100, raw_score))
    hard_negative = sum(abs(weight) for _, weight in negative_hits if weight <= -18)
    is_icp = score >= 45 and bool(icp_hits and product_hits) and hard_negative < 30

    if score >= 70 and is_icp:
        lead_type = "core_icp"
        priority = "high"
    elif score >= 45 and is_icp:
        lead_type = "possible_icp"
        priority = "medium"
    elif score >= 28:
        lead_type = "seller_segment"
        priority = "low"
    else:
        lead_type = "low_relevance"
        priority = "low"

    hypothesis, outreach_angle, cjm_stage = _build_hypothesis(
        score=score,
        product_hits=product_hits,
        channel_hits=channel_hits,
        growth_hits=growth_hits,
        has_contacts=has_contacts,
        has_catalog=has_catalog,
        has_cart=has_cart,
        site_type=site_type,
    )

    evidence_parts = []
    if icp_hits:
        evidence_parts.append("производитель/бренд: " + _format_hits(icp_hits))
    if product_hits:
        evidence_parts.append("категория: " + _format_hits(product_hits))
    if channel_hits:
        evidence_parts.append("каналы: " + _format_hits(channel_hits))
    if growth_hits:
        evidence_parts.append("сигналы роста: " + _format_hits(growth_hits))
    if negative_hits:
        evidence_parts.append("минусы: " + _format_hits(negative_hits))
    if site_assessment:
        evidence_parts.append("сайт: " + site_assessment)

    reason_parts = [f"score={score}"]
    reason_parts.extend(evidence_parts)

    opener = (
        "Добрый день. Изучил ваш сайт: похоже, у вас уже есть продуктовая линейка и несколько каналов продаж. "
        "Обычно на этой стадии вопрос не в том, чтобы заменить текущие каналы, а в том, где аккуратно проверить следующий слой роста. "
        "Могу коротко показать, как мы смотрим на связку MP/retail/direct и где обычно виден потолок модели."
    )

    return {
        "is_icp": is_icp,
        "icp_score": score,
        "icp_reason": "; ".join(reason_parts),
        "evidence": "\n".join(evidence_parts),
        "hypothesis": hypothesis,
        "outreach_angle": outreach_angle,
        "opener": opener,
        "cjm_stage": cjm_stage,
        "lead_type": lead_type,
        "lead_type_ru": TYPE_MAP[lead_type],
        "priority": priority,
        "priority_ru": PRIORITY_MAP[priority],
        "score": score,
    }
