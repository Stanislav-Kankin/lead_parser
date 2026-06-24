from __future__ import annotations

import re


BANNED_FIRST_TOUCH_PHRASES = [
    "мы агентство",
    "мы помогаем",
    "давайте созвонимся",
    "можем созвониться",
    "можем обсудить",
    "запустим кит",
    "кит + директ",
    "яндекс кит + директ",
    "brandformance",
    "система роста",
    "стратегия роста",
    "стратегия",
    "прямой канал продаж",
    "юнит-экономика",
    "коммерческое предложение",
]

FALLBACK_REPLY_DRAFT = (
    "Здравствуйте. Я бы тут сначала уточнил пару деталей, потому что причина может быть разной.\n\n"
    "Обычно смотрят площадку, категорию товара и где именно появилась просадка: доставка, выкуп, "
    "реклама или комиссия. Это WB или Ozon?"
)

DO_NOT_CONTACT_CATEGORIES = {"taxes", "certification"}
DO_NOT_CONTACT_MARKERS = [
    "налог",
    "бухгалтер",
    "упд",
    "сертифик",
    "деклараци",
    "карго",
    "тамож",
    "честный знак",
    "маркировк",
]

MEETING_CTA_MARKERS = [
    "созвон",
    "встреч",
    "назначим звонок",
    "назначить звонок",
    "встретимся",
    "назначим встречу",
    "обсудим на встрече",
]


def build_human_reply_draft(
    *,
    pain_category: str,
    bridge_to_offer: str,
    search_profile: str | None = None,
    marketplace: str | None = None,
    niche: str | None = None,
    message_text: str = "",
) -> dict:
    del search_profile
    category = str(pain_category or "").strip()
    text_l = _normalize_for_check(message_text)
    if _should_not_contact(category, bridge_to_offer, text_l):
        reason = "налоги, документы, сертификация, карго или таможня"
        return {
            "best_reply_draft": f"Не писать первым: запрос относится к теме «{reason}» и не имеет прямого моста к предложению AdBeam.",
            "next_question": "",
            "reply_tone": "do_not_contact",
        }

    tone = _reply_tone(category, bridge_to_offer)
    draft, question = _category_draft(
        category=category,
        bridge_to_offer=bridge_to_offer,
        marketplace=marketplace or "",
        niche=niche or "",
    )
    draft = _clean_draft(draft)
    if not validate_reply_draft(draft):
        draft = FALLBACK_REPLY_DRAFT
        question = "Это WB или Ozon?"
        tone = "helpful"
    return {
        "best_reply_draft": draft,
        "next_question": question,
        "reply_tone": tone,
    }


def build_human_reply_variants(
    *,
    pain_category: str,
    bridge_to_offer: str,
    search_profile: str | None = None,
    marketplace: str | None = None,
    niche: str | None = None,
    message_text: str = "",
) -> dict:
    primary = build_human_reply_draft(
        pain_category=pain_category,
        bridge_to_offer=bridge_to_offer,
        search_profile=search_profile,
        marketplace=marketplace,
        niche=niche,
        message_text=message_text,
    )
    if primary["reply_tone"] == "do_not_contact":
        warning = primary["best_reply_draft"]
        return {
            "opener_soft": warning,
            "opener_expert": warning,
            "opener_sales": warning,
        }

    question = primary["next_question"]
    telegram = _telegram_variant(pain_category, marketplace or "", niche or "", question)
    expert = primary["best_reply_draft"]
    b2b = _b2b_variant(pain_category, bridge_to_offer, marketplace or "", niche or "", question)
    return {
        "opener_soft": _validated_or_fallback(telegram),
        "opener_expert": _validated_or_fallback(expert),
        "opener_sales": _validated_or_fallback(b2b),
    }


def build_second_touch_bridge() -> str:
    return (
        "Понял. Если такая история повторяется, это уже может бить не только по операционке, но и по марже.\n\n"
        "Тут обычно смотрят связку: SKU → доставка → выкуп → реклама → прибыль.\n\n"
        "Если хотите, могу подсказать, что проверить в первую очередь."
    )


def validate_reply_draft(text: str) -> bool:
    clean = _clean_draft(text)
    if not clean or len(clean) > 500:
        return False
    if clean.count("?") > 1:
        return False
    if "?" in clean and not clean.endswith("?"):
        return False
    text_l = _normalize_for_check(clean)
    if any(phrase in text_l for phrase in BANNED_FIRST_TOUCH_PHRASES):
        return False
    if any(marker in text_l for marker in MEETING_CTA_MARKERS):
        return False
    paragraphs = [paragraph for paragraph in re.split(r"\n\s*\n", clean) if paragraph.strip()]
    return len(paragraphs) <= 3


def _category_draft(*, category: str, bridge_to_offer: str, marketplace: str, niche: str) -> tuple[str, str]:
    category_question = f"Какая у вас категория{f' — {niche}' if niche else ''}?"
    platform_question = "Это WB или Ozon?" if marketplace not in {"WB", "Ozon"} else "По каким SKU это происходит чаще всего?"

    if category == "returns_logistics":
        return (
            "Здравствуйте. По описанию похоже на авто-возврат или невыкуп: товар доехал до ПВЗ, покупатель "
            "его не забрал, и площадка вернула товар продавцу.\n\n"
            "Я бы проверил, по каким SKU это чаще всего происходит и не выросли ли сроки доставки. "
            f"{platform_question}",
            platform_question,
        )
    if category == "ads_complaint":
        return (
            "Добрый день. Я бы тут не смотрел только на ставку в рекламе. Часто проблема в связке: комиссия, "
            "логистика, скидка, возвраты и сама реклама.\n\n"
            "Сначала стоит посчитать расходы по конкретным SKU, а не общий ДРР по кабинету. Так быстрее видно, "
            "где именно появилась просадка. "
            f"{category_question}",
            category_question,
        )
    if category == "unit_economics":
        return (
            "Добрый день. Тут лучше смотреть не только комиссию, а всю связку расходов: логистика, хранение, "
            "возвраты, скидка и реклама. Иногда оборот выглядит нормально, а прибыль проседает из-за суммы мелких расходов.\n\n"
            f"Я бы начал с расчёта по 3–5 основным SKU. {category_question}",
            category_question,
        )
    if category == "direct_channel" or bridge_to_offer in {"direct_channel", "kit_store", "yandex_direct"}:
        question = "Сайт уже есть или пока только думаете?"
        return (
            "Добрый день. Сайт сам по себе обычно проблему не решает. Главный вопрос — какие товары можно вести "
            "на сайт и сколько будет стоить первый заказ.\n\n"
            "Я бы сначала проверил маржу по 3–5 SKU и спрос в Яндексе. Так можно оценить потенциал до вложений "
            "в полноценный запуск. "
            f"{question}",
            question,
        )
    if category in {"contractor_search", "marketer_search"}:
        question = "Вы ищете подрядчика под WB/Ozon или под внешний трафик?"
        return (
            "Добрый день. Перед выбором подрядчика я бы попросил у всех не просто настройку рекламы, а план "
            "теста: какие товары берём, какой бюджет, какой допустимый ДРР и когда принимаем решение — продолжаем или стоп.\n\n"
            f"Так проще сравнить подходы до старта и не опираться только на кейсы и обещания. {question}",
            question,
        )
    if category == "sales_growth":
        return (
            "Добрый день. Я бы сначала посмотрел не на скидку, а на первые сигналы: показы, клики, добавления "
            "в корзину и выкуп. Если клики есть, а корзин нет, узкое место чаще в первом экране, цене или оффере.\n\n"
            f"Если кликов мало, стоит проверить запросы и показы. {category_question}",
            category_question,
        )
    question = platform_question if marketplace not in {"WB", "Ozon"} else category_question
    return (
        "Добрый день. По сообщению пока сложно отделить разовую просадку от системной проблемы. Я бы сначала "
        "разложил ситуацию на товар, логистику, выкуп, рекламу и комиссию.\n\n"
        f"Так быстрее видно, где появилось ограничение и что проверять первым. {question}",
        question,
    )


def _telegram_variant(category: str, marketplace: str, niche: str, question: str) -> str:
    del marketplace, niche
    if category == "returns_logistics":
        body = "Похоже на невыкуп: товар дошёл до ПВЗ, его не забрали, и площадка отправила обратно. Я бы сначала посмотрел SKU и сроки доставки."
    elif category in {"ads_complaint", "unit_economics"}:
        body = "Тут ставка может быть только частью проблемы. Я бы свёл по SKU комиссию, логистику, скидки, возвраты и рекламу — обычно после этого видно, где съедается результат."
    elif category in {"contractor_search", "marketer_search"}:
        body = "Я бы у подрядчиков сразу просил короткий план теста: товары, бюджет, допустимый ДРР и условие остановки. Так проще сравнить их до старта."
    elif category == "direct_channel":
        body = "Сайт лучше проверять на нескольких товарах, а не запускать сразу на весь ассортимент. Сначала нужны маржа, спрос и допустимая стоимость первого заказа."
    else:
        body = "Я бы сначала разложил ситуацию по шагам: показы, клики, корзина, выкуп и расходы площадки. Так быстрее находится реальное узкое место."
    return f"Добрый день. {body}\n\n{question}"


def _b2b_variant(category: str, bridge_to_offer: str, marketplace: str, niche: str, question: str) -> str:
    del marketplace, niche
    if category in {"ads_complaint", "unit_economics"}:
        body = (
            "Для бизнеса здесь важна не отдельная ставка, а итоговая экономика SKU после комиссии, логистики, "
            "возвратов, скидок и рекламы. Я бы проверил основные товары отдельно и сравнил вклад каждого расхода."
        )
    elif category == "returns_logistics":
        body = (
            "Если возвраты повторяются по одним SKU или регионам, это уже влияет не только на операционку, "
            "но и на маржу. Я бы сопоставил сроки доставки, ПВЗ и долю выкупа."
        )
    elif category in {"contractor_search", "marketer_search"}:
        body = (
            "При выборе подрядчика полезно заранее зафиксировать товары для теста, бюджет, допустимый ДРР и "
            "критерии продолжения. Тогда сравнивается не обещание результата, а качество подхода."
        )
    elif category == "direct_channel" or bridge_to_offer in {"direct_channel", "kit_store", "yandex_direct"}:
        body = (
            "Сайт стоит оценивать как ограниченный тест спроса: выбрать несколько товаров, проверить маржу, "
            "стоимость первого заказа и заранее определить условие остановки."
        )
    else:
        body = (
            "Здесь полезно отделить локальную просадку от ограничения модели: посмотреть спрос, конверсию, "
            "выкуп и все расходы по ключевым SKU, а затем выбирать следующий шаг."
        )
    return f"Добрый день. {body}\n\n{question}"


def _reply_tone(category: str, bridge_to_offer: str) -> str:
    if category == "returns_logistics":
        return "helpful"
    if category in {"unit_economics", "ads_complaint", "sales_growth", "marketplace_complaint"}:
        return "expert"
    if bridge_to_offer in {"direct_channel", "kit_store", "yandex_direct"}:
        return "sales_bridge"
    return "helpful"


def _should_not_contact(category: str, bridge_to_offer: str, text_l: str) -> bool:
    if bridge_to_offer != "no_bridge":
        return False
    return category in DO_NOT_CONTACT_CATEGORIES or any(marker in text_l for marker in DO_NOT_CONTACT_MARKERS)


def _validated_or_fallback(text: str) -> str:
    clean = _clean_draft(text)
    return clean if validate_reply_draft(clean) else FALLBACK_REPLY_DRAFT


def _clean_draft(text: str) -> str:
    paragraphs = [re.sub(r"\s+", " ", paragraph).strip() for paragraph in re.split(r"\n\s*\n", str(text or ""))]
    return "\n\n".join(paragraph for paragraph in paragraphs if paragraph)


def _normalize_for_check(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").lower().replace("ё", "е")).strip()
