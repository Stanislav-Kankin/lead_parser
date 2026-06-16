from __future__ import annotations

from datetime import datetime
from typing import Iterable

from sqlalchemy import delete, desc, func, or_, select

from models.lead import SocialLead
from storage.db import SessionLocal


def save_social_leads(items: Iterable[dict]) -> dict:
    created = 0
    updated = 0
    skipped = 0
    with SessionLocal() as session:
        for item in items:
            source_url = item.get("source_url")
            if not source_url:
                skipped += 1
                continue
            existing = session.execute(select(SocialLead).where(SocialLead.source_url == source_url)).scalar_one_or_none()
            payload = {
                "source": item.get("source") or "tenchat",
                "source_url": source_url,
                "source_query": item.get("source_query"),
                "profile_url": item.get("profile_url"),
                "post_url": item.get("post_url"),
                "person_name": item.get("person_name"),
                "role_title": item.get("role_title"),
                "company_name": item.get("company_name"),
                "title": item.get("title"),
                "snippet": item.get("snippet"),
                "text": item.get("text"),
                "lead_score": int(item.get("lead_score") or 0),
                "lead_fit": item.get("lead_fit"),
                "likely_icp": item.get("likely_icp"),
                "pain_detected": item.get("pain_detected"),
                "cjm_stage": item.get("cjm_stage"),
                "why_relevant": item.get("why_relevant"),
                "outreach_angle": item.get("outreach_angle"),
                "opener": item.get("opener"),
                "updated_at": datetime.utcnow(),
            }
            if existing:
                for key, value in payload.items():
                    setattr(existing, key, value)
                updated += 1
            else:
                session.add(SocialLead(**payload))
                created += 1
        session.commit()
    return {"created": created, "updated": updated, "skipped": skipped}


def get_social_leads(
    *,
    limit: int = 50,
    offset: int = 0,
    min_score: int | None = None,
    status: str | None = None,
    source: str | None = None,
    query: str | None = None,
) -> list[SocialLead]:
    with SessionLocal() as session:
        stmt = select(SocialLead)
        if min_score is not None:
            stmt = stmt.where(SocialLead.lead_score >= min_score)
        if status:
            stmt = stmt.where(SocialLead.status == status)
        if source:
            stmt = stmt.where(SocialLead.source == source)
        if query:
            needle = f"%{query.strip()}%"
            stmt = stmt.where(
                or_(
                    SocialLead.person_name.ilike(needle),
                    SocialLead.role_title.ilike(needle),
                    SocialLead.company_name.ilike(needle),
                    SocialLead.title.ilike(needle),
                    SocialLead.why_relevant.ilike(needle),
                    SocialLead.pain_detected.ilike(needle),
                )
            )
        stmt = stmt.order_by(desc(SocialLead.lead_score), desc(SocialLead.updated_at)).offset(offset).limit(limit)
        return list(session.execute(stmt).scalars().all())


def count_social_leads(
    *,
    min_score: int | None = None,
    status: str | None = None,
    source: str | None = None,
) -> int:
    with SessionLocal() as session:
        stmt = select(func.count(SocialLead.id))
        if min_score is not None:
            stmt = stmt.where(SocialLead.lead_score >= min_score)
        if status:
            stmt = stmt.where(SocialLead.status == status)
        if source:
            stmt = stmt.where(SocialLead.source == source)
        return int(session.execute(stmt).scalar_one() or 0)


def clear_social_leads() -> int:
    with SessionLocal() as session:
        result = session.execute(delete(SocialLead))
        session.commit()
        return int(result.rowcount or 0)


def update_social_lead(
    lead_id: int,
    *,
    status: str | None = None,
    owner: str | None = None,
    comment: str | None = None,
) -> bool:
    with SessionLocal() as session:
        item = session.get(SocialLead, lead_id)
        if item is None:
            return False
        if status is not None:
            item.status = status or "new"
        if owner is not None:
            item.owner = owner or None
        if comment is not None:
            item.comment = comment or None
        item.updated_at = datetime.utcnow()
        session.commit()
        return True
