from datetime import datetime
from typing import Iterable

from sqlalchemy import desc, or_, select

from models.lead import Lead
from storage.db import SessionLocal
from utils.domain_normalizer import normalize_domain


STATUSES = ["new", "in_progress", "contacted", "skip", "done"]



def save_leads(leads: Iterable[dict]) -> tuple[int, int]:
    created = 0
    updated = 0
    now = datetime.utcnow()

    with SessionLocal() as session:
        for item in leads:
            domain = item["domain"]
            domain_normalized = normalize_domain(domain) or domain.lower()

            exists = session.execute(
                select(Lead).where(
                    or_(Lead.domain == domain, Lead.domain_normalized == domain_normalized)
                )
            ).scalar_one_or_none()

            if exists:
                exists.query = item["query"]
                exists.company_name = item.get("company_name") or exists.company_name
                exists.domain = domain
                exists.domain_normalized = domain_normalized
                exists.source = item.get("source", exists.source)
                exists.is_icp = item.get("is_icp", exists.is_icp)
                exists.icp_reason = item.get("icp_reason") or exists.icp_reason
                exists.icp_score = item.get("icp_score", exists.icp_score)
                exists.hypothesis = item.get("hypothesis") or exists.hypothesis
                exists.opener = item.get("opener") or exists.opener
                exists.title = item.get("title") or exists.title
                exists.meta_description = item.get("meta_description") or exists.meta_description
                exists.company_email = item.get("company_email") or exists.company_email
                exists.company_phone = item.get("company_phone") or exists.company_phone
                exists.lead_type = item.get("lead_type") or exists.lead_type
                exists.priority = item.get("priority") or exists.priority
                exists.updated_at = now
                updated += 1
                continue

            lead = Lead(
                query=item["query"],
                company_name=item.get("company_name"),
                domain=domain,
                domain_normalized=domain_normalized,
                source=item.get("source", "ddgs"),
                is_icp=item.get("is_icp", False),
                icp_reason=item.get("icp_reason"),
                icp_score=item.get("icp_score", 0),
                hypothesis=item.get("hypothesis"),
                opener=item.get("opener"),
                title=item.get("title"),
                meta_description=item.get("meta_description"),
                company_email=item.get("company_email"),
                company_phone=item.get("company_phone"),
                lead_type=item.get("lead_type"),
                priority=item.get("priority"),
                status=item.get("status", "new"),
                created_at=now,
                updated_at=now,
            )
            session.add(lead)
            created += 1

        session.commit()
    return created, updated



def get_last_leads(limit: int = 10, offset: int = 0) -> list[Lead]:
    with SessionLocal() as session:
        stmt = select(Lead).order_by(desc(Lead.updated_at), desc(Lead.created_at)).offset(offset).limit(limit)
        return list(session.execute(stmt).scalars().all())



def count_leads() -> int:
    with SessionLocal() as session:
        return len(list(session.execute(select(Lead.id)).scalars().all()))
