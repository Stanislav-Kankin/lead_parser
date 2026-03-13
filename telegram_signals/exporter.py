from __future__ import annotations

from datetime import datetime
from pathlib import Path

from openpyxl import Workbook

from telegram_signals.repository import get_discussion_leads, get_signals


def export_signals_to_xlsx(kind: str = "actionable") -> Path:
    if kind == "discussion":
        items = get_discussion_leads()
        suffix = "discussion"
    elif kind == "raw":
        items = get_signals(limit=None)
        suffix = "raw"
    else:
        items = get_signals(limit=None, only_actionable=True)
        suffix = "actionable"

    wb = Workbook()
    ws = wb.active
    ws.title = "signals"

    headers = [
        "date",
        "segment",
        "chat_title",
        "chat_username",
        "author_username",
        "author_type_guess",
        "message_type",
        "conversation_type",
        "signal_level",
        "signal_score",
        "final_lead_score",
        "is_actionable",
        "matched_keywords",
        "company_hint",
        "website_hint",
        "contact_hint",
        "why_actionable",
        "text_excerpt",
        "recommended_opener",
        "chat_url",
    ]
    ws.append(headers)

    for item in items:
        ws.append([
            str(item.message_date)[:19] if item.message_date else "",
            item.segment or "",
            item.chat_title or "",
            item.chat_username or "",
            item.author_username or "",
            item.author_type_guess or "",
            item.message_type or "",
            item.conversation_type or "",
            item.signal_level or "",
            item.signal_score or 0,
            item.final_lead_score or 0,
            int(bool(item.is_actionable)),
            item.matched_keywords or "",
            item.company_hint or "",
            item.website_hint or "",
            item.contact_hint or "",
            item.why_actionable or "",
            item.text_excerpt or "",
            item.recommended_opener or "",
            item.chat_url or "",
        ])

    export_dir = Path("exports")
    export_dir.mkdir(exist_ok=True)
    file_path = export_dir / f"telegram_signals_{suffix}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
    wb.save(file_path)
    return file_path
