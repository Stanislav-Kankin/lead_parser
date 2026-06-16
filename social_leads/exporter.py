from __future__ import annotations

from datetime import datetime
from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

from storage.social_lead_repository import get_social_leads


def export_social_leads_to_xlsx() -> Path:
    items = get_social_leads(limit=10000)
    wb = Workbook()
    ws = wb.active
    ws.title = "people_icp"
    headers = [
        "score",
        "статус",
        "источник",
        "человек",
        "роль",
        "компания",
        "профиль",
        "пост/страница",
        "поисковый запрос",
        "почему подходит",
        "ICP",
        "боль",
        "CJM",
        "заход",
        "черновик сообщения",
        "комментарий",
    ]
    ws.append(headers)
    for cell in ws[1]:
        cell.font = Font(bold=True)
    for item in items:
        ws.append(
            [
                int(item.lead_score or 0),
                item.status or "",
                item.source or "",
                item.person_name or "",
                item.role_title or "",
                item.company_name or "",
                item.profile_url or "",
                item.post_url or item.source_url or "",
                item.source_query or "",
                item.why_relevant or "",
                item.likely_icp or "",
                item.pain_detected or "",
                item.cjm_stage or "",
                item.outreach_angle or "",
                item.opener or "",
                item.comment or "",
            ]
        )
    _autosize(ws)
    export_dir = Path("exports")
    export_dir.mkdir(exist_ok=True)
    file_path = export_dir / f"people_icp_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
    wb.save(file_path)
    return file_path


def _autosize(ws) -> None:
    for col_idx, column_cells in enumerate(ws.columns, start=1):
        max_len = 0
        for cell in column_cells:
            max_len = max(max_len, len(str(cell.value or "")))
        ws.column_dimensions[get_column_letter(col_idx)].width = min(max(max_len + 2, 12), 58)
