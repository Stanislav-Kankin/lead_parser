from __future__ import annotations

import csv
import re
from datetime import datetime
from pathlib import Path

from openpyxl import load_workbook
from sqlalchemy import select

from models.lead import Lead
from storage.db import SessionLocal


def import_focus_file(path: str | Path) -> dict:
    rows = _read_rows(Path(path))
    matched = 0
    unmatched = 0
    skipped = 0

    with SessionLocal() as session:
        for row in rows:
            data = _map_focus_row(row)
            inn = data.get("company_inn")
            if not inn:
                skipped += 1
                continue

            lead = session.execute(select(Lead).where(Lead.company_inn == inn)).scalar_one_or_none()
            if not lead:
                unmatched += 1
                continue

            if data.get("company_ogrn") and not lead.company_ogrn:
                lead.company_ogrn = data["company_ogrn"]
            if data.get("company_legal_name"):
                lead.company_legal_name = data["company_legal_name"]
            lead.focus_status = data.get("focus_status") or lead.focus_status
            lead.focus_region = data.get("focus_region") or lead.focus_region
            lead.focus_revenue = data.get("focus_revenue") or lead.focus_revenue
            lead.focus_employees = data.get("focus_employees") or lead.focus_employees
            lead.focus_okved = data.get("focus_okved") or lead.focus_okved
            lead.focus_director = data.get("focus_director") or lead.focus_director
            lead.focus_loaded_at = datetime.utcnow()
            lead.updated_at = datetime.utcnow()
            matched += 1
        session.commit()

    return {"rows": len(rows), "matched": matched, "unmatched": unmatched, "skipped": skipped}


def _read_rows(path: Path) -> list[dict]:
    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xlsm"}:
        return _read_xlsx(path)
    if suffix == ".csv":
        return _read_csv(path)
    raise ValueError("Поддерживаются только .xlsx, .xlsm и .csv")


def _read_xlsx(path: Path) -> list[dict]:
    wb = load_workbook(path, read_only=True, data_only=True)
    try:
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            return []
        headers = [_normalize_header(value) for value in rows[0]]
        result = []
        for raw in rows[1:]:
            item = {headers[index]: raw[index] for index in range(min(len(headers), len(raw))) if headers[index]}
            if any(value not in (None, "") for value in item.values()):
                result.append(item)
        return result
    finally:
        wb.close()


def _read_csv(path: Path) -> list[dict]:
    text = path.read_text(encoding="utf-8-sig")
    sample = text[:2048]
    dialect = csv.Sniffer().sniff(sample, delimiters=";,	,")
    reader = csv.DictReader(text.splitlines(), dialect=dialect)
    return [{_normalize_header(key): value for key, value in row.items()} for row in reader]


def _map_focus_row(row: dict) -> dict:
    return {
        "company_inn": _digits(_pick(row, "инн", "inn"), {10, 12}),
        "company_ogrn": _digits(_pick(row, "огрн", "ogrn"), {13, 15}),
        "company_legal_name": _pick(row, "наименование", "название", "компания", "организация", "юрназвание", "company", "name"),
        "focus_status": _pick(row, "статус", "состояние", "status"),
        "focus_region": _pick(row, "регион", "субъект", "адрес", "region"),
        "focus_revenue": _pick(row, "выручка", "доход", "revenue"),
        "focus_employees": _pick(row, "сотрудники", "численность", "персонал", "employees", "staff"),
        "focus_okved": _pick(row, "оквэд", "виддеятельности", "okved"),
        "focus_director": _pick(row, "руководитель", "директор", "генеральныйдиректор", "director", "ceo"),
    }


def _pick(row: dict, *needles: str) -> str | None:
    for key, value in row.items():
        normalized_key = _normalize_header(key)
        if any(needle in normalized_key for needle in needles):
            clean = str(value or "").strip()
            if clean:
                return clean
    return None


def _digits(value: str | None, allowed_lengths: set[int]) -> str | None:
    digits = re.sub(r"\D+", "", str(value or ""))
    if len(digits) in allowed_lengths:
        return digits
    return None


def _normalize_header(value) -> str:
    return re.sub(r"[^a-zа-я0-9]+", "", str(value or "").lower())
