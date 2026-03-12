from typing import Iterable

from sqlalchemy import desc, select

from models.lead import Lead
from storage.db import SessionLocal


def save_leads(leads: Iterable[dict]) -> int:
    saved = 0
    with SessionLocal() as session:
        for item in leads:
            domain = item["domain"]
            exists = session.execute(
                select(Lead).where(Lead.domain == domain)
            ).scalar_one_or_none()
            if exists:
                continue

            lead = Lead(
                query=item["query"],
                company_name=item.get("company_name"),
                domain=domain,
                source=item.get("source", "ddg"),
                is_icp=item.get("is_icp", False),
                icp_reason=item.get("icp_reason"),
                hypothesis=item.get("hypothesis"),
            )
            session.add(lead)
            saved += 1

        session.commit()
    return saved


def get_last_leads(limit: int = 10) -> list[Lead]:
    with SessionLocal() as session:
        stmt = select(Lead).order_by(desc(Lead.created_at)).limit(limit)
        return list(session.execute(stmt).scalars().all())
