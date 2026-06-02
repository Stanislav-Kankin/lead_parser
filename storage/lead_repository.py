import logging
from datetime import datetime, timedelta
from typing import Iterable

from sqlalchemy import case, delete, desc, func, or_, select

from models.lead import Lead
from storage.db import SeenAuthor, SessionLocal
from utils.domain_normalizer import get_root_domain, normalize_domain

logger = logging.getLogger(__name__)


def save_leads(leads: Iterable[dict]) -> dict:
    created = 0
    updated = 0
    skipped = 0

    with SessionLocal() as session:
        for item in leads:
            raw_domain = item.get("domain")
            domain_normalized = normalize_domain(raw_domain)
            root_domain = get_root_domain(raw_domain)

            if not raw_domain or not domain_normalized:
                skipped += 1
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
                "source_url": item.get("source_url") or item.get("url"),
                "is_icp": item.get("is_icp", False),
                "icp_score": int(item.get("icp_score") or item.get("score") or 0),
                "icp_reason": item.get("icp_reason"),
                "evidence": item.get("evidence"),
                "outreach_angle": item.get("outreach_angle"),
                "hypothesis": item.get("hypothesis"),
                "opener": item.get("opener"),
                "cjm_stage": item.get("cjm_stage"),
                "lead_type": item.get("lead_type"),
                "priority": item.get("priority"),
                "title": item.get("title"),
                "company_inn": item.get("company_inn"),
                "company_ogrn": item.get("company_ogrn"),
                "company_legal_name": item.get("company_legal_name"),
                "legal_form": item.get("legal_form"),
                "inn_source": item.get("inn_source"),
                "company_email": item.get("company_email"),
                "company_phone": item.get("company_phone"),
                "employees": item.get("employees"),
                "contacts_source": item.get("contacts_source"),
                "contact_confidence": item.get("contact_confidence", _contact_confidence(item)),
                "has_contacts": item.get("has_contacts", False),
                "has_catalog": item.get("has_catalog", False),
                "has_cart": item.get("has_cart", False),
                "ecommerce_score": int(item.get("ecommerce_score") or 0),
                "site_type": item.get("site_type"),
                "site_assessment": item.get("site_assessment"),
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

    logger.info("[lead_repository] save created=%s updated=%s skipped=%s", created, updated, skipped)
    return {"created": created, "updated": updated, "skipped": skipped}



def get_last_leads(limit: int = 10, only_with_contacts: bool = True) -> list[Lead]:
    with SessionLocal() as session:
        stmt = select(Lead)
        if only_with_contacts:
            stmt = stmt.where(Lead.has_contacts.is_(True))

        sales_ready_order = case((Lead.sales_ready.is_(True), 1), else_=0)
        icp_order = case((Lead.is_icp.is_(True), 1), else_=0)
        inn_order = case((Lead.company_inn.is_not(None), 1), else_=0)
        confidence_order = case(
            (Lead.contact_confidence == "high", 3),
            (Lead.contact_confidence == "medium", 2),
            (Lead.contact_confidence == "low", 1),
            else_=0,
        )

        stmt = stmt.order_by(
            desc(sales_ready_order),
            desc(icp_order),
            desc(inn_order),
            desc(confidence_order),
            desc(Lead.updated_at),
            desc(Lead.created_at),
        ).limit(limit)
        return list(session.execute(stmt).scalars().all())


def get_web_leads(
    *,
    limit: int = 50,
    offset: int = 0,
    only_icp: bool = False,
    status: str | None = None,
    min_score: int | None = None,
    query: str | None = None,
) -> list[Lead]:
    with SessionLocal() as session:
        stmt = select(Lead)
        if only_icp:
            stmt = stmt.where(Lead.is_icp.is_(True))
        if status:
            stmt = stmt.where(Lead.status == status)
        if min_score is not None:
            stmt = stmt.where(Lead.icp_score >= min_score)
        if query:
            needle = f"%{query.strip()}%"
            stmt = stmt.where(
                or_(
                    Lead.company_name.ilike(needle),
                    Lead.domain.ilike(needle),
                    Lead.title.ilike(needle),
                    Lead.icp_reason.ilike(needle),
                    Lead.evidence.ilike(needle),
                )
            )

        stmt = stmt.order_by(
            desc(Lead.is_icp),
            desc(Lead.icp_score),
            desc(Lead.has_contacts),
            desc(Lead.updated_at),
            desc(Lead.created_at),
        ).offset(offset).limit(limit)
        return list(session.execute(stmt).scalars().all())


def count_web_leads(
    *,
    only_icp: bool = False,
    status: str | None = None,
    min_score: int | None = None,
) -> int:
    with SessionLocal() as session:
        stmt = select(func.count(Lead.id))
        if only_icp:
            stmt = stmt.where(Lead.is_icp.is_(True))
        if status:
            stmt = stmt.where(Lead.status == status)
        if min_score is not None:
            stmt = stmt.where(Lead.icp_score >= min_score)
        return int(session.execute(stmt).scalar_one() or 0)


def clear_web_leads() -> int:
    with SessionLocal() as session:
        result = session.execute(delete(Lead))
        session.commit()
        return int(result.rowcount or 0)


def update_web_lead(
    lead_id: int,
    *,
    status: str | None = None,
    owner: str | None = None,
    comment: str | None = None,
) -> bool:
    with SessionLocal() as session:
        item = session.get(Lead, lead_id)
        if item is None:
            return False
        if status is not None:
            item.status = status
        if owner is not None:
            item.owner = owner or None
        if comment is not None:
            item.comment = comment or None
        item.updated_at = datetime.utcnow()
        session.commit()
        return True


def get_seen_author(author_id: str | None) -> SeenAuthor | None:
    if not author_id:
        return None
    with SessionLocal() as session:
        return session.execute(select(SeenAuthor).where(SeenAuthor.author_id == str(author_id))).scalar_one_or_none()


def upsert_seen_author(author_id: str | None) -> None:
    if not author_id:
        return
    now = datetime.utcnow()
    with SessionLocal() as session:
        item = session.execute(select(SeenAuthor).where(SeenAuthor.author_id == str(author_id))).scalar_one_or_none()
        if item is None:
            session.add(SeenAuthor(author_id=str(author_id), last_signal_at=now, signal_count_7d=1))
        else:
            if item.last_signal_at and item.last_signal_at >= now - timedelta(days=7):
                item.signal_count_7d = int(item.signal_count_7d or 0) + 1
            else:
                item.signal_count_7d = 1
            item.last_signal_at = now
        session.commit()



def _contact_confidence(item: dict) -> str:
    if item.get("company_inn") or item.get("company_legal_name"):
        return "high"
    if item.get("company_email") and item.get("company_phone"):
        return "medium"
    if item.get("company_email") or item.get("company_phone"):
        return "low"
    return "low"
