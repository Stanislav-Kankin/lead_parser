from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

from sqlalchemy import select

from focus_importer import map_focus_row, read_focus_rows
from models.lead import SocialLead, SocialLeadProject
from storage.db import SessionLocal


def import_social_focus_file(path: str | Path, *, project_id: int) -> dict:
    if not project_id:
        raise ValueError("Для импорта Компаса нужно выбрать проект TenChat")

    rows = read_focus_rows(path)
    matched_inns: set[str] = set()
    updated_lead_ids: set[int] = set()
    active_inns: set[str] = set()
    phone_inns: set[str] = set()
    email_inns: set[str] = set()
    website_inns: set[str] = set()
    revenue_inns: set[str] = set()
    unmatched = 0
    skipped = 0

    with SessionLocal() as session:
        social_ids = select(SocialLeadProject.social_lead_id).where(
            SocialLeadProject.project_id == int(project_id)
        )
        leads = list(
            session.execute(
                select(SocialLead).where(
                    SocialLead.id.in_(social_ids),
                    SocialLead.company_inn.is_not(None),
                    SocialLead.company_inn != "",
                )
            ).scalars()
        )
        leads_by_inn: dict[str, list[SocialLead]] = {}
        for lead in leads:
            inn = _normalize_inn(lead.company_inn)
            if inn:
                leads_by_inn.setdefault(inn, []).append(lead)

        for row in rows:
            data = map_focus_row(row)
            inn = _normalize_inn(data.get("company_inn"))
            if not inn:
                skipped += 1
                continue
            matched_leads = leads_by_inn.get(inn, [])
            if not matched_leads:
                unmatched += 1
                continue

            matched_inns.add(inn)
            if _is_active(data.get("focus_status")):
                active_inns.add(inn)
            if data.get("focus_phone"):
                phone_inns.add(inn)
            if data.get("focus_email"):
                email_inns.add(inn)
            if data.get("focus_website"):
                website_inns.add(inn)
            if data.get("focus_revenue"):
                revenue_inns.add(inn)

            for lead in matched_leads:
                _apply_focus_data(lead, data)
                updated_lead_ids.add(int(lead.id))

        session.commit()

    return {
        "rows": len(rows),
        "matched_companies": len(matched_inns),
        "updated_people": len(updated_lead_ids),
        "active_companies": len(active_inns),
        "with_phone": len(phone_inns),
        "with_email": len(email_inns),
        "with_website": len(website_inns),
        "with_revenue": len(revenue_inns),
        "unmatched": unmatched,
        "skipped": skipped,
    }


def _apply_focus_data(lead: SocialLead, data: dict) -> None:
    if data.get("company_ogrn") and not lead.company_ogrn:
        lead.company_ogrn = data["company_ogrn"]
    if data.get("company_legal_name") and not lead.company_legal_name:
        lead.company_legal_name = data["company_legal_name"]
    lead.focus_legal_name = data.get("company_legal_name") or lead.focus_legal_name
    lead.focus_status = data.get("focus_status") or lead.focus_status
    lead.focus_region = data.get("focus_region") or lead.focus_region
    lead.focus_address = data.get("focus_address") or lead.focus_address
    lead.focus_revenue = data.get("focus_revenue") or lead.focus_revenue
    lead.focus_balance = data.get("focus_balance") or lead.focus_balance
    lead.focus_profit = data.get("focus_profit") or lead.focus_profit
    lead.focus_arbitration = data.get("focus_arbitration") or lead.focus_arbitration
    lead.focus_employees = data.get("focus_employees") or lead.focus_employees
    lead.focus_okved = data.get("focus_okved") or lead.focus_okved
    lead.focus_other_okved = data.get("focus_other_okved") or lead.focus_other_okved
    lead.focus_director = data.get("focus_director") or lead.focus_director
    lead.focus_msp = data.get("focus_msp") or lead.focus_msp
    lead.focus_phone = data.get("focus_phone") or lead.focus_phone
    lead.focus_email = data.get("focus_email") or lead.focus_email
    lead.focus_website = data.get("focus_website") or lead.focus_website
    lead.focus_registration_date = data.get("focus_registration_date") or lead.focus_registration_date
    lead.focus_loaded_at = datetime.utcnow()
    lead.updated_at = datetime.utcnow()


def _normalize_inn(value: str | None) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    return digits if len(digits) in {10, 12} else ""


def _is_active(value: str | None) -> bool:
    return "действующее предприятие" in str(value or "").strip().lower()
