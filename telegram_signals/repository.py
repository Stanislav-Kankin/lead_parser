from __future__ import annotations

from typing import Iterable

from sqlalchemy import case, desc, select

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

        level_order = case(
            (TelegramSignal.signal_level == "high", 3),
            (TelegramSignal.signal_level == "medium", 2),
            else_=1,
        )
        stmt = stmt.order_by(
            desc(TelegramSignal.final_lead_score),
            desc(level_order),
            desc(TelegramSignal.signal_score),
            desc(TelegramSignal.message_date),
            desc(TelegramSignal.created_at),
        )
        if limit:
            stmt = stmt.limit(limit)
        return list(session.execute(stmt).scalars().all())


def get_discussion_leads(segment: str | None = None, limit: int | None = None) -> list[TelegramSignal]:
    return get_signals(segment=segment, limit=limit, conversation_type="discussion", business_only=True)


def get_business_like_messages(segment: str | None = None, limit: int | None = None) -> list[TelegramSignal]:
    return get_signals(segment=segment, limit=limit, business_only=True)
