from __future__ import annotations

from datetime import datetime
from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

from storage.lead_repository import get_web_leads


def _autosize(ws) -> None:
    for col_idx, column_cells in enumerate(ws.columns, start=1):
        max_len = 0
        for cell in column_cells:
            try:
                max_len = max(max_len, len(str(cell.value or "")))
            except Exception:
                pass
        ws.column_dimensions[get_column_letter(col_idx)].width = min(max(max_len + 2, 12), 46)


def _site_url(item) -> str:
    domain = item.domain_normalized or item.domain or ""
    if not domain:
        return ""
    return f"https://{domain}"


def export_web_leads_to_xlsx() -> Path:
    items = get_web_leads(limit=10000)

    wb = Workbook()
    ws = wb.active
    ws.title = "web_icp"

    headers = [
        "категория",
        "название сайта",
        "сайт",
        "контакты(телефон)",
        "почта",
        "инн",
        "score",
        "тип ICP",
        "приоритет",
        "статус",
        "есть каталог",
        "есть корзина",
        "оценка ecom",
        "тип сайта",
        "оценка сайта",
        "юр. название",
        "гипотеза",
        "почему подходит",
        "заход",
        "поисковый запрос",
        "источник",
        "обновлено",
    ]
    ws.append(headers)
    for cell in ws[1]:
        cell.font = Font(bold=True)

    for item in items:
        ws.append(
            [
                item.search_category or item.lead_type or item.priority or "",
                item.title or item.company_name or item.domain or "",
                _site_url(item),
                item.company_phone or "",
                item.company_email or "",
                item.company_inn or "",
                int(item.icp_score or 0),
                item.lead_type or "",
                item.priority or "",
                item.status or "",
                "да" if item.has_catalog else "нет",
                "да" if item.has_cart else "нет",
                int(item.ecommerce_score or 0),
                item.site_type or "",
                item.site_assessment or "",
                item.company_legal_name or "",
                item.hypothesis or "",
                item.evidence or item.icp_reason or "",
                item.outreach_angle or "",
                item.query or "",
                item.source_url or "",
                item.updated_at.strftime("%Y-%m-%d %H:%M:%S") if item.updated_at else "",
            ]
        )

    _autosize(ws)

    export_dir = Path("exports")
    export_dir.mkdir(exist_ok=True)
    file_path = export_dir / f"web_icp_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
    wb.save(file_path)
    return file_path
