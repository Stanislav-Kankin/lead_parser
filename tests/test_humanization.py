from __future__ import annotations

import unittest

from telegram_signals.humanization import (
    BANNED_FIRST_TOUCH_PHRASES,
    build_human_reply_draft,
    build_human_reply_variants,
    build_second_touch_bridge,
    validate_reply_draft,
)
from telegram_signals.signal_classifier import classify_signal


class HumanizationEngineTests(unittest.TestCase):
    CASES = [
        ("returns_logistics", "no_bridge", "WB", "одежда", "У нас выросли возвраты с ПВЗ", "helpful"),
        ("ads_complaint", "yandex_direct", "Ozon", "косметика", "Реклама перестала окупаться", "expert"),
        ("unit_economics", "unit_economics_audit", "WB", "одежда", "Комиссии съели маржу", "expert"),
        ("direct_channel", "direct_channel", "WB", "косметика", "Думаем сделать свой сайт", "sales_bridge"),
        ("contractor_search", "yandex_direct", "WB", "одежда", "Ищем подрядчика по рекламе", "sales_bridge"),
        ("sales_growth", "no_bridge", "Ozon", "электроника", "Продажи остановились", "expert"),
        ("marketplace_complaint", "no_bridge", "", "товары для дома", "На маркетплейсе всё стало дороже", "expert"),
    ]

    def test_ten_first_touch_scenarios(self):
        for category, bridge, marketplace, niche, message, expected_tone in self.CASES:
            with self.subTest(category=category):
                result = build_human_reply_draft(
                    pain_category=category,
                    bridge_to_offer=bridge,
                    marketplace=marketplace,
                    niche=niche,
                    message_text=message,
                )
                draft = result["best_reply_draft"]
                self.assertEqual(result["reply_tone"], expected_tone)
                self.assertGreaterEqual(len(draft), 250)
                self.assertLessEqual(len(draft), 500)
                self.assertTrue(validate_reply_draft(draft))
                self.assertEqual(draft.count("?"), 1)
                self.assertTrue(draft.endswith("?"))

        do_not_contact_cases = [
            ("taxes", "Вопрос по УПД и налогам"),
            ("certification", "Нужна сертификация товара"),
            ("returns_logistics", "Груз застрял на таможне, везём через карго"),
        ]
        for category, message in do_not_contact_cases:
            with self.subTest(category=category, message=message):
                result = build_human_reply_draft(
                    pain_category=category,
                    bridge_to_offer="no_bridge",
                    message_text=message,
                )
                self.assertEqual(result["reply_tone"], "do_not_contact")
                self.assertIn("Не писать первым", result["best_reply_draft"])

    def test_three_variants_are_safe(self):
        for category, bridge, marketplace, niche, message, _tone in self.CASES:
            variants = build_human_reply_variants(
                pain_category=category,
                bridge_to_offer=bridge,
                marketplace=marketplace,
                niche=niche,
                message_text=message,
            )
            self.assertEqual(set(variants), {"opener_soft", "opener_expert", "opener_sales"})
            for draft in variants.values():
                self.assertTrue(validate_reply_draft(draft))
                self.assertLessEqual(draft.count("?"), 1)

    def test_banned_phrases_are_rejected(self):
        for phrase in BANNED_FIRST_TOUCH_PHRASES:
            self.assertFalse(validate_reply_draft(f"Добрый день. {phrase}. Какая у вас категория?"))

    def test_second_touch_is_separate(self):
        second_touch = build_second_touch_bridge()
        self.assertIn("SKU → доставка → выкуп → реклама → прибыль", second_touch)
        self.assertNotIn("давайте созвонимся", second_touch.lower())

    def test_classifier_uses_humanization_engine(self):
        samples = [
            ("У нас реклама на Ozon перестала окупаться, ставки растут. Что проверить?", "ads_complaint", "expert"),
            ("Ищем подрядчика для внешнего трафика на сайт бренда", "contractor_search", "sales_bridge"),
            ("Подскажите по УПД и налогам на Wildberries", "taxes", "do_not_contact"),
        ]
        for text, category, tone in samples:
            result = classify_signal(
                text,
                "ecom_marketplace_pain",
                author_username="seller_test",
                author_name="Иван",
                chat_title="Чат селлеров",
            )
            self.assertEqual(result["lead_category"], category)
            self.assertEqual(result["reply_tone"], tone)
            self.assertIn("opener_soft", result)
            self.assertIn("opener_expert", result)
            self.assertIn("opener_sales", result)


if __name__ == "__main__":
    unittest.main()
