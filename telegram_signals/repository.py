from __future__ import annotations

from typing import Iterable

from datetime import datetime

from sqlalchemy import case, desc, or_, select

from storage.db import SessionLocal
from .models import TelegramSignal


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
                    setattr(exists, k, v)
                updated += 1
            else:
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

        level_order = case(
            (TelegramSignal.signal_level == "high", 3),
            (TelegramSignal.signal_level == "medium", 2),
            else_=1,
        )
        stmt = stmt.order_by(
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
    return get_signals(segment=segment, limit=limit, lead_fit_in=["target", "review"], review_status=review_status)


def set_signal_review_status(signal_id: int, review_status: str) -> bool:
    with SessionLocal() as session:
        item = session.get(TelegramSignal, signal_id)
        if item is None:
            return False
        item.review_status = review_status
        item.reviewed_at = datetime.utcnow()
        session.commit()
        return True
