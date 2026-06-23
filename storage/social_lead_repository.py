from __future__ import annotations

from datetime import datetime
from typing import Iterable

from sqlalchemy import and_, case, delete, desc, func, or_, select

from models.lead import SearchProject, SocialLead, SocialLeadProject
from storage.db import SessionLocal

ARTICLE_TITLE_MARKERS = [
    "как ",
    "способ",
    "способы",
    "по шагам",
    "квиз",
    "забытый канал",
    "самый экономичный",
    "белое продвижение",
    "оптимизировать рекламу",
    "реклама в telegram",
    "реклама в телеграм",
]


def save_social_leads(items: Iterable[dict], project_id: int | None = None, project_name: str | None = None) -> dict:
    created = 0
    updated = 0
    skipped = 0
    resolved_project_id = int(project_id or 0) or None
    if not resolved_project_id and project_name:
        resolved_project_id = _get_or_create_project_id(project_name)
    with SessionLocal() as session:
        for item in items:
            source_url = item.get("source_url")
            if not source_url:
                skipped += 1
                continue
            item_project_id = int(item.get("project_id") or resolved_project_id or 0) or None
            if not item_project_id and item.get("project_name"):
                item_project_id = _get_or_create_project_id(str(item.get("project_name")))
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
                "company_inn": item.get("company_inn"),
                "company_ogrn": item.get("company_ogrn"),
                "company_legal_name": item.get("company_legal_name"),
                "company_url": item.get("company_url"),
                "matched_web_lead_id": item.get("matched_web_lead_id"),
                "matched_web_domain": item.get("matched_web_domain"),
                "matched_web_title": item.get("matched_web_title"),
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
                lead = existing
            else:
                lead = SocialLead(**payload)
                session.add(lead)
                session.flush()
                created += 1
            if item_project_id:
                _attach_project(session, lead.id, item_project_id)
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
    people_only: bool = True,
    project_id: int | None = None,
) -> list[SocialLead]:
    with SessionLocal() as session:
        stmt = select(SocialLead)
        if project_id:
            social_ids = select(SocialLeadProject.social_lead_id).where(SocialLeadProject.project_id == int(project_id))
            stmt = stmt.where(SocialLead.id.in_(social_ids))
        if people_only:
            stmt = stmt.where(
                or_(
                    SocialLead.person_name.is_not(None),
                    SocialLead.role_title.is_not(None),
                    SocialLead.profile_url.is_not(None),
                )
            )
            for marker in ARTICLE_TITLE_MARKERS:
                stmt = stmt.where(or_(SocialLead.title.is_(None), ~SocialLead.title.ilike(f"%{marker}%")))
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
                    SocialLead.company_inn.ilike(needle),
                    SocialLead.company_ogrn.ilike(needle),
                    SocialLead.company_legal_name.ilike(needle),
                    SocialLead.title.ilike(needle),
                    SocialLead.why_relevant.ilike(needle),
                    SocialLead.pain_detected.ilike(needle),
                )
            )
        has_inn = case(
            (and_(SocialLead.company_inn.is_not(None), SocialLead.company_inn != ""), 1),
            else_=0,
        )
        stmt = stmt.order_by(desc(has_inn), desc(SocialLead.lead_score), desc(SocialLead.updated_at)).offset(offset).limit(limit)
        return list(session.execute(stmt).scalars().all())


def count_social_leads(
    *,
    min_score: int | None = None,
    status: str | None = None,
    source: str | None = None,
    people_only: bool = True,
    project_id: int | None = None,
) -> int:
    with SessionLocal() as session:
        stmt = select(func.count(SocialLead.id))
        if project_id:
            social_ids = select(SocialLeadProject.social_lead_id).where(SocialLeadProject.project_id == int(project_id))
            stmt = stmt.where(SocialLead.id.in_(social_ids))
        if people_only:
            stmt = stmt.where(
                or_(
                    SocialLead.person_name.is_not(None),
                    SocialLead.role_title.is_not(None),
                    SocialLead.profile_url.is_not(None),
                )
            )
            for marker in ARTICLE_TITLE_MARKERS:
                stmt = stmt.where(or_(SocialLead.title.is_(None), ~SocialLead.title.ilike(f"%{marker}%")))
        if min_score is not None:
            stmt = stmt.where(SocialLead.lead_score >= min_score)
        if status:
            stmt = stmt.where(SocialLead.status == status)
        if source:
            stmt = stmt.where(SocialLead.source == source)
        return int(session.execute(stmt).scalar_one() or 0)


def clear_social_leads(project_id: int | None = None) -> int:
    with SessionLocal() as session:
        if project_id:
            result = session.execute(delete(SocialLeadProject).where(SocialLeadProject.project_id == int(project_id)))
            session.commit()
            return int(result.rowcount or 0)
        session.execute(delete(SocialLeadProject))
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


def get_project_names_for_social_leads(lead_ids: Iterable[int]) -> dict[int, str]:
    ids = [int(value) for value in lead_ids if value]
    if not ids:
        return {}
    with SessionLocal() as session:
        rows = session.execute(
            select(SocialLeadProject.social_lead_id, SearchProject.name)
            .join(SearchProject, SearchProject.id == SocialLeadProject.project_id)
            .where(SocialLeadProject.social_lead_id.in_(ids))
            .order_by(SearchProject.created_at)
        ).all()
    result: dict[int, list[str]] = {}
    for lead_id, name in rows:
        result.setdefault(int(lead_id), []).append(str(name))
    return {lead_id: ", ".join(names) for lead_id, names in result.items()}


def count_social_leads_with_inn(*, project_id: int | None = None) -> int:
    with SessionLocal() as session:
        stmt = select(func.count(SocialLead.id)).where(SocialLead.company_inn.is_not(None), SocialLead.company_inn != "")
        if project_id:
            social_ids = select(SocialLeadProject.social_lead_id).where(SocialLeadProject.project_id == int(project_id))
            stmt = stmt.where(SocialLead.id.in_(social_ids))
        return int(session.execute(stmt).scalar_one() or 0)


def _attach_project(session, social_lead_id: int, project_id: int) -> None:
    exists = session.execute(
        select(SocialLeadProject.id).where(
            SocialLeadProject.social_lead_id == int(social_lead_id),
            SocialLeadProject.project_id == int(project_id),
        )
    ).scalar_one_or_none()
    if exists:
        return
    project = session.get(SearchProject, int(project_id))
    if project:
        project.updated_at = datetime.utcnow()
    session.add(SocialLeadProject(social_lead_id=int(social_lead_id), project_id=int(project_id)))


def _get_or_create_project_id(name: str) -> int | None:
    clean = " ".join(str(name or "").split()).strip()
    if not clean:
        return None
    with SessionLocal() as session:
        existing = session.execute(select(SearchProject).where(SearchProject.name == clean)).scalar_one_or_none()
        if existing:
            return int(existing.id)
        project = SearchProject(name=clean, updated_at=datetime.utcnow())
        session.add(project)
        session.commit()
        session.refresh(project)
        return int(project.id)
