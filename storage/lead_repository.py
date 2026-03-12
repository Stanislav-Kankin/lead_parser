from collections.abc import Iterable
from datetime import datetime

from sqlalchemy import desc, select

from models.lead import Lead
from storage.db import SessionLocal
from utils.domain_normalizer import normalize_domain


def save_leads(leads: Iterable[dict]) -> tuple[int, int]:
    created = 0
    updated = 0

    with SessionLocal() as session:
        for item in leads:
            domain_normalized = normalize_domain(item.get("domain_normalized") or item.get("domain"))
            if not domain_normalized:
                continue

            exists = session.execute(
                select(Lead).where(Lead.domain_normalized == domain_normalized)
            ).scalar_one_or_none()

            payload = {
                "query": item["query"],
                "company_name": item.get("company_name"),
                "domain": item.get("domain") or domain_normalized,
                "domain_normalized": domain_normalized,
                "source": item.get("source", "ddgs"),
                "is_icp": item.get("is_icp", False),
                "icp_reason": item.get("icp_reason"),
                "hypothesis": item.get("hypothesis"),
                "title": item.get("title"),
                "lead_type": item.get("lead_type"),
                "priority": item.get("priority"),
                "company_email": item.get("company_email"),
                "company_phone": item.get("company_phone"),
                "status": item.get("status", "new"),
                "updated_at": datetime.utcnow(),
            }

            if exists:
                for key, value in payload.items():
                    if value is not None:
                        setattr(exists, key, value)
                updated += 1
                continue

            lead = Lead(**payload)
            session.add(lead)
            created += 1

        session.commit()

    return created, updated


def get_last_leads(limit: int = 10) -> list[Lead]:
    with SessionLocal() as session:
        stmt = select(Lead).order_by(desc(Lead.updated_at), desc(Lead.created_at)).limit(limit)
        return list(session.execute(stmt).scalars().all())
