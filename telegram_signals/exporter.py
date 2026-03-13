from __future__ import annotations

from datetime import datetime
from pathlib import Path
from tempfile import gettempdir

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font

from telegram_signals.repository import get_signals


RAW_HEADERS = [
    "Дата",
    "Сегмент",
    "Тип сигнала",
    "Actionable",
    "Уровень",
    "Score",
    "ICP score",
    "Pain score",
    "Intent score",
    "Contactability score",
    "Чат",
    "Chat username",
    "Автор",
    "Contact hint",
    "Company hint",
    "Website hint",
    "Совпадения",
    "Текст",
    "Заход",
    "Ссылка на чат",
]

SALES_HEADERS = [
    "Дата",
    "Сегмент",
    "Тип сигнала",
    "Почему это лид",
    "Компания / бренд",
    "Контакт",
    "Website",
    "Чат",
    "Автор",
    "Фрагмент",
    "Заход",
    "Ссылка на чат",
    "Статус",
]


def _autosize(ws):
    for column_cells in ws.columns:
        length = max(len(str(cell.value or "")) for cell in column_cells)
        ws.column_dimensions[column_cells[0].column_letter].width = min(max(length + 2, 14), 42)
    for row in ws.iter_rows():
        for cell in row:
            cell.alignment = Alignment(wrap_text=True, vertical="top")
    for cell in ws[1]:
        cell.font = Font(bold=True)


def build_signals_export(mode: str = "actionable") -> str:
    only_actionable = mode == "actionable"
    items = get_signals(limit=None, only_actionable=only_actionable)

    wb = Workbook()
    ws = wb.active
    ws.title = "signals"

    headers = SALES_HEADERS if only_actionable else RAW_HEADERS
    ws.append(headers)

    for item in items:
        if only_actionable:
            why_parts = []
            if item.message_type:
                why_parts.append(f"type={item.message_type}")
            if item.pain_score:
                why_parts.append(f"pain={item.pain_score}")
            if item.intent_score:
                why_parts.append(f"intent={item.intent_score}")
            if item.contactability_score:
                why_parts.append(f"contact={item.contactability_score}")
            ws.append([
                item.message_date.strftime("%Y-%m-%d %H:%M") if item.message_date else "",
                item.segment,
                item.message_type,
                "; ".join(why_parts),
                item.company_hint,
                item.contact_hint,
                item.website_hint,
                item.chat_title,
                item.author_username,
                item.text_excerpt,
                item.recommended_opener,
                item.chat_url,
                item.status,
            ])
        else:
            ws.append([
                item.message_date.strftime("%Y-%m-%d %H:%M") if item.message_date else "",
                item.segment,
                item.message_type,
                "yes" if item.is_actionable else "no",
                item.signal_level,
                item.signal_score,
                item.icp_score,
                item.pain_score,
                item.intent_score,
                item.contactability_score,
                item.chat_title,
                item.chat_username,
                item.author_username,
                item.contact_hint,
                item.company_hint,
                item.website_hint,
                item.matched_keywords,
                item.message_text,
                item.recommended_opener,
                item.chat_url,
            ])

    _autosize(ws)

    filename = f"telegram_signals_{mode}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    path = Path(gettempdir()) / filename
    wb.save(path)
    return str(path)
