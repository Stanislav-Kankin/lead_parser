from __future__ import annotations

from typing import Iterable

from datetime import datetime

from sqlalchemy import case, desc, or_, select

from storage.db import SessionLocal
from .models import TelegramSignal


AD_TEXT_EXCLUDE_PATTERNS = (
    "наклейки-замки",
    "на ваши зип-пакеты",
    "ваш идеальный помощник",
    "нужен товар с рынка",
    "мы выкупим",
    "упакуем",
    "доставим в любую точку",
    "этапы сотрудничества",
    "поиск одежды по фото",
    "показ товаров по видеосвязи",
)


def _exclude_obvious_ads(stmt):
    for pattern in AD_TEXT_EXCLUDE_PATTERNS:
        stmt = stmt.where(~TelegramSignal.message_text.ilike(f"%{pattern}%"))
    return stmt


def _author_identity_filter(item: dict):
    author_id = str(item.get("author_id") or "").strip()
    author_username = str(item.get("author_username") or "").strip()
    clauses = []
    if author_id:
        clauses.append(TelegramSignal.author_id == author_id)
    if author_username:
        clauses.append(TelegramSignal.author_username.ilike(author_username))
    if not clauses:
        return None
    return or_(*clauses)


def _apply_author_history(session, item: dict) -> None:
    identity_filter = _author_identity_filter(item)
    if identity_filter is None:
        return

    contacted = session.execute(
        select(TelegramSignal.id).where(
            identity_filter,
            TelegramSignal.status == "contacted",
        ).limit(1)
    ).first()
    if contacted:
        item["status"] = "contacted"
        item["review_status"] = "ok"
        return

    not_ok = session.execute(
        select(TelegramSignal.id).where(
            identity_filter,
            TelegramSignal.review_status == "not_ok",
        ).limit(1)
    ).first()
    if not_ok:
        item["review_status"] = "not_ok"


def save_signals(items: Iterable[dict]) -> dict:
    created = 0
    updated = 0

    with SessionLocal() as session:
        for item in items:
            exists = session.execute(
                select(TelegramSignal).where(
                    TelegramSignal.chat_id == item.get("chat_id"),
                    TelegramSignal.message_id == item.get("message_id"),
                )
            ).scalar_one_or_none()

            if exists:
                for k, v in item.items():
                    if k in {"review_status", "reviewed_at", "status"}:
                        continue
                    setattr(exists, k, v)
                updated += 1
            else:
                _apply_author_history(session, item)
                session.add(TelegramSignal(**item))
                created += 1
        session.commit()

    return {"created": created, "updated": updated}


def get_signals(
    segment: str | None = None,
    limit: int | None = None,
    *,
    only_actionable: bool = False,
    conversation_type: str | None = None,
    business_only: bool = False,
    lead_fit: str | None = None,
    lead_fit_in: list[str] | None = None,
    review_status: str | None = None,
    review_status_in: list[str] | None = None,
    status: str | None = None,
    status_not: str | None = None,
    crm_tag: str | None = None,
    min_score: int | None = None,
    marketplace: str | None = None,
    niche: str | None = None,
    lead_category: str | None = None,
) -> list[TelegramSignal]:
    with SessionLocal() as session:
        stmt = select(TelegramSignal)
        if segment:
            stmt = stmt.where(TelegramSignal.segment == segment)
        if only_actionable:
            stmt = stmt.where(TelegramSignal.is_actionable == True)  # noqa: E712
        if conversation_type:
            stmt = stmt.where(TelegramSignal.conversation_type == conversation_type)
        if business_only:
            stmt = stmt.where(TelegramSignal.author_type_guess == "business")
        if lead_fit:
            stmt = stmt.where(TelegramSignal.lead_fit == lead_fit)
        if lead_fit_in:
            stmt = stmt.where(TelegramSignal.lead_fit.in_(lead_fit_in))
        if review_status:
            stmt = stmt.where(TelegramSignal.review_status == review_status)
        if review_status_in:
            stmt = stmt.where(TelegramSignal.review_status.in_(review_status_in))
        if status:
            stmt = stmt.where(TelegramSignal.status == status)
        if status_not:
            stmt = stmt.where(TelegramSignal.status != status_not)
        if crm_tag:
            stmt = stmt.where(TelegramSignal.crm_tag == crm_tag)
        if min_score is not None:
            stmt = stmt.where(TelegramSignal.lead_score_100 >= min_score)
        if marketplace:
            stmt = stmt.where(TelegramSignal.marketplace == marketplace)
        if niche:
            stmt = stmt.where(TelegramSignal.niche == niche)
        if lead_category:
            stmt = stmt.where(TelegramSignal.lead_category == lead_category)
        if only_actionable or lead_fit or lead_fit_in or review_status or review_status_in:
            stmt = _exclude_obvious_ads(stmt)

        level_order = case(
            (TelegramSignal.signal_level == "high", 3),
            (TelegramSignal.signal_level == "medium", 2),
            else_=1,
        )
        stmt = stmt.order_by(
            desc(TelegramSignal.is_person_reachable),
            desc(TelegramSignal.lead_score_100),
            desc(TelegramSignal.contact_entity_score),
            desc(TelegramSignal.final_lead_score),
            desc(TelegramSignal.conversation_score),
            desc(TelegramSignal.reply_depth),
            desc(TelegramSignal.contactability_score),
            desc(level_order),
            desc(TelegramSignal.signal_score),
            desc(TelegramSignal.message_date),
            desc(TelegramSignal.created_at),
        )
        if limit:
            stmt = stmt.limit(limit)
        return list(session.execute(stmt).scalars().all())


def get_target_leads(segment: str | None = None, limit: int | None = None, *, include_reviewed: bool = False) -> list[TelegramSignal]:
    kwargs = {} if include_reviewed else {"review_status": "unchecked"}
    return get_signals(segment=segment, limit=limit, lead_fit="target", **kwargs)


def get_review_leads(segment: str | None = None, limit: int | None = None, *, include_reviewed: bool = False) -> list[TelegramSignal]:
    kwargs = {} if include_reviewed else {"review_status": "unchecked"}
    return get_signals(segment=segment, limit=limit, lead_fit="review", **kwargs)


def get_discussion_leads(segment: str | None = None, limit: int | None = None) -> list[TelegramSignal]:
    with SessionLocal() as session:
        stmt = select(TelegramSignal)
        if segment:
            stmt = stmt.where(TelegramSignal.segment == segment)
        stmt = stmt.where(
            TelegramSignal.lead_fit.in_(["target", "review"]),
            TelegramSignal.author_type_guess != "contractor",
            or_(
                TelegramSignal.context_score >= 2,
                TelegramSignal.conversation_score >= 2,
                TelegramSignal.conversation_type.in_(["discussion", "complaint", "help_request", "question"]),
                TelegramSignal.pain_score >= 3,
                TelegramSignal.intent_score >= 4,
            ),
        )
        stmt = stmt.order_by(
            desc(TelegramSignal.conversation_score),
            desc(TelegramSignal.context_score),
            desc(TelegramSignal.final_lead_score),
            desc(TelegramSignal.message_date),
            desc(TelegramSignal.created_at),
        )
        if limit:
            stmt = stmt.limit(limit)
        return list(session.execute(stmt).scalars().all())


def get_business_like_messages(segment: str | None = None, limit: int | None = None) -> list[TelegramSignal]:
    return get_signals(segment=segment, limit=limit, business_only=True, lead_fit_in=["target", "review"])


def get_market_intelligence(segment: str | None = None, limit: int | None = None) -> list[TelegramSignal]:
    with SessionLocal() as session:
        stmt = select(TelegramSignal)
        if segment:
            stmt = stmt.where(TelegramSignal.segment == segment)
        stmt = stmt.where(TelegramSignal.message_type.in_(["expert_content", "market_intelligence"]))
        stmt = stmt.order_by(
            desc(TelegramSignal.final_lead_score),
            desc(TelegramSignal.message_date),
            desc(TelegramSignal.created_at),
        )
        if limit:
            stmt = stmt.limit(limit)
        return list(session.execute(stmt).scalars().all())


def get_reviewed_leads(review_status: str, segment: str | None = None, limit: int | None = None) -> list[TelegramSignal]:
    return get_signals(segment=segment, limit=limit, lead_fit_in=["target", "review"], review_status=review_status, status_not="contacted")


def get_contacted_leads(segment: str | None = None, limit: int | None = None) -> list[TelegramSignal]:
    return get_signals(segment=segment, limit=limit, lead_fit_in=["target", "review"], review_status="ok", status="contacted")


def get_hot_leads(limit: int | None = 10) -> list[TelegramSignal]:
    return get_signals(limit=limit, lead_fit_in=["target", "review"], review_status="unchecked", status_not="contacted", min_score=80)


def get_signal_by_id(signal_id: int) -> TelegramSignal | None:
    with SessionLocal() as session:
        return session.get(TelegramSignal, signal_id)


def set_signal_review_status(signal_id: int, review_status: str) -> bool:
    with SessionLocal() as session:
        item = session.get(TelegramSignal, signal_id)
        if item is None:
            return False
        item.review_status = review_status
        item.reviewed_at = datetime.utcnow()
        session.commit()
        return True


def set_signal_status(signal_id: int, status: str, *, review_status: str | None = None) -> bool:
    with SessionLocal() as session:
        item = session.get(TelegramSignal, signal_id)
        if item is None:
            return False
        item.status = status
        if review_status:
            item.review_status = review_status
            item.reviewed_at = datetime.utcnow()
        session.commit()
        return True


def update_signal_crm(signal_id: int, *, status: str | None = None, crm_tag: str | None = None, comment: str | None = None, review_status: str | None = None) -> bool:
    with SessionLocal() as session:
        item = session.get(TelegramSignal, signal_id)
        if item is None:
            return False
        if status is not None:
            item.status = status
        if crm_tag is not None:
            item.crm_tag = crm_tag or None
        if comment is not None:
            item.comment = comment.strip() or None
        if review_status:
            item.review_status = review_status
            item.reviewed_at = datetime.utcnow()
        session.commit()
        return True
