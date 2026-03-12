from datetime import datetime
from typing import Iterable

from sqlalchemy import case, desc, or_, select

from models.lead import Lead
from storage.db import SessionLocal
from utils.domain_normalizer import get_root_domain, normalize_domain


def save_leads(leads: Iterable[dict]) -> dict:
    created = 0
    updated = 0

    with SessionLocal() as session:
        for item in leads:
            raw_domain = item.get("domain")
            domain_normalized = normalize_domain(raw_domain)
            root_domain = get_root_domain(raw_domain)

            if not raw_domain or not domain_normalized:
                continue

            exists = session.execute(
                select(Lead).where(
                    or_(
                        Lead.domain_normalized == domain_normalized,
                        Lead.domain == raw_domain,
                    )
                )
            ).scalar_one_or_none()

            payload = {
                "query": item["query"],
                "company_name": item.get("company_name"),
                "domain": raw_domain,
                "domain_normalized": domain_normalized,
                "root_domain": root_domain,
                "source": item.get("source", "ddgs"),
                "is_icp": item.get("is_icp", False),
                "icp_reason": item.get("icp_reason"),
                "hypothesis": item.get("hypothesis"),
                "opener": item.get("opener"),
                "lead_type": item.get("lead_type"),
                "priority": item.get("priority"),
                "title": item.get("title"),
                "company_inn": item.get("company_inn"),
                "company_legal_name": item.get("company_legal_name"),
                "company_email": item.get("company_email"),
                "company_phone": item.get("company_phone"),
                "employees": item.get("employees"),
                "contacts_source": item.get("contacts_source"),
                "contact_confidence": item.get("contact_confidence", _contact_confidence(item)),
                "has_contacts": item.get("has_contacts", False),
                "sales_ready": item.get("sales_ready", False),
                "status": item.get("status", "new"),
                "updated_at": datetime.utcnow(),
                "last_enriched_at": item.get("last_enriched_at"),
            }

            if exists:
                for key, value in payload.items():
                    setattr(exists, key, value)
                updated += 1
            else:
                lead = Lead(**payload)
                session.add(lead)
                created += 1

        session.commit()

    return {"created": created, "updated": updated}


def get_last_leads(limit: int = 10, only_with_contacts: bool = True) -> list[Lead]:
    with SessionLocal() as session:
        stmt = select(Lead)
        if only_with_contacts:
            stmt = stmt.where(Lead.has_contacts.is_(True))

        sales_ready_order = case((Lead.sales_ready.is_(True), 1), else_=0)
        icp_order = case((Lead.is_icp.is_(True), 1), else_=0)
        confidence_order = case(
            (Lead.contact_confidence == "high", 3),
            (Lead.contact_confidence == "medium", 2),
            (Lead.contact_confidence == "low", 1),
            else_=0,
        )

        stmt = stmt.order_by(
            desc(sales_ready_order),
            desc(icp_order),
            desc(confidence_order),
            desc(Lead.updated_at),
            desc(Lead.created_at),
        ).limit(limit)
        return list(session.execute(stmt).scalars().all())


def _contact_confidence(item: dict) -> str:
    if item.get("company_inn") or item.get("company_legal_name"):
        return "high"
    if item.get("company_email") or item.get("company_phone"):
        return "medium"
    return "low"
