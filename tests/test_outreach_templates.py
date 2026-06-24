from __future__ import annotations

import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from social_leads.outreach_templates import (
    DEFAULT_OUTREACH_TEMPLATES,
    LEGACY_TEMPLATE_TEXTS,
    TEMPLATE_VERSION,
    load_outreach_templates,
    reset_outreach_templates,
    save_outreach_template,
)
from telegram_signals.humanization import validate_reply_draft


class OutreachTemplateTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        self.path = Path(self.temp_dir.name) / "outreach.json"
        self.previous_path = os.environ.get("PEOPLE_OUTREACH_TEMPLATES_PATH")
        os.environ["PEOPLE_OUTREACH_TEMPLATES_PATH"] = str(self.path)

    def tearDown(self):
        if self.previous_path is None:
            os.environ.pop("PEOPLE_OUTREACH_TEMPLATES_PATH", None)
        else:
            os.environ["PEOPLE_OUTREACH_TEMPLATES_PATH"] = self.previous_path
        self.temp_dir.cleanup()

    def test_default_drafts_are_human_and_safe(self):
        templates = load_outreach_templates()
        self.assertEqual(set(templates), set(DEFAULT_OUTREACH_TEMPLATES))
        for template in templates.values():
            text = template["text"]
            self.assertGreaterEqual(len(text), 230)
            self.assertTrue(validate_reply_draft(text))
            self.assertEqual(text.count("?"), 1)

    def test_legacy_defaults_migrate_but_custom_text_survives(self):
        legacy = dict(LEGACY_TEMPLATE_TEXTS)
        legacy["marketing"] = "Мой собственный текст. Какая задача сейчас актуальна?"
        self.path.write_text(json.dumps(legacy, ensure_ascii=False), encoding="utf-8")

        templates = load_outreach_templates()
        self.assertEqual(templates["universal"]["text"], DEFAULT_OUTREACH_TEMPLATES["universal"]["text"])
        self.assertEqual(templates["owner"]["text"], DEFAULT_OUTREACH_TEMPLATES["owner"]["text"])
        self.assertEqual(templates["marketing"]["text"], legacy["marketing"])

    def test_save_and_reset(self):
        custom = "Иван, добрый день. Посмотрел компанию. Какая задача сейчас актуальна?"
        save_outreach_template("owner", custom)
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        self.assertEqual(payload["version"], TEMPLATE_VERSION)
        self.assertEqual(load_outreach_templates()["owner"]["text"], custom)

        reset_outreach_templates()
        self.assertEqual(load_outreach_templates()["owner"]["text"], DEFAULT_OUTREACH_TEMPLATES["owner"]["text"])


if __name__ == "__main__":
    unittest.main()
