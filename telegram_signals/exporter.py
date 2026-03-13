from __future__ import annotations

from datetime import datetime
from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

from telegram_signals.repository import (
    get_discussion_leads,
    get_review_leads,
    get_signals,
    get_target_leads,
    get_market_intelligence,
)


def _autosize(ws) -> None:
    for col_idx, column_cells in enumerate(ws.columns, start=1):
        max_len = 0
        for cell in column_cells:
            try:
                max_len = max(max_len, len(str(cell.value or "")))
            except Exception:
                pass
        ws.column_dimensions[get_column_letter(col_idx)].width = min(max(max_len + 2, 12), 42)


def export_signals_to_xlsx(kind: str = "actionable") -> Path:
    if kind == "discussion":
        items = get_discussion_leads()
        suffix = "discussion"
        title = "discussion_leads"
    elif kind == "review":
        items = get_review_leads()
        suffix = "review"
        title = "review_leads"
    elif kind == "raw":
        items = get_signals(limit=None)
        suffix = "raw"
        title = "raw_signals"
    elif kind == "market":
        items = get_market_intelligence()
        suffix = "market"
        title = "market_intelligence"
    elif kind == "target":
        items = get_target_leads()
        suffix = "target"
        title = "target_leads"
    else:
        items = get_signals(limit=None, lead_fit_in=["target", "review"])
        suffix = "sales"
        title = "sales_leads"

    wb = Workbook()
    ws = wb.active
    ws.title = title

    headers = [
        "date",
        "segment",
        "lead_fit",
        "next_step",
        "author_type_guess",
        "conversation_type",
        "message_type",
        "final_lead_score",
        "why_actionable",
        "company_hint",
        "website_hint",
        "contact_hint",
        "chat_title",
        "author_username",
        "short_message",
        "recommended_opener",
        "chat_url",
    ]
    ws.append(headers)
    for cell in ws[1]:
        cell.font = Font(bold=True)

    for item in items:
        ws.append([
            str(item.message_date)[:19] if item.message_date else "",
            item.segment or "",
            item.lead_fit or "",
            item.next_step or "",
            item.author_type_guess or "",
            item.conversation_type or "",
            item.message_type or "",
            item.final_lead_score or 0,
            item.why_actionable or "",
            item.company_hint or "",
            item.website_hint or "",
            item.contact_hint or "",
            item.chat_title or "",
            item.author_username or "",
            (item.text_excerpt or "")[:240],
            item.recommended_opener or "",
            item.chat_url or "",
        ])

    _autosize(ws)

    export_dir = Path("exports")
    export_dir.mkdir(exist_ok=True)
    file_path = export_dir / f"telegram_signals_{suffix}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
    wb.save(file_path)
    return file_path
