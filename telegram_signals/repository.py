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


def get_signals(segment: str | None = None, limit: int = 30) -> list[TelegramSignal]:
    with SessionLocal() as session:
        stmt = select(TelegramSignal)
        if segment:
            stmt = stmt.where(TelegramSignal.segment == segment)
        level_order = case(
            (TelegramSignal.signal_level == "high", 3),
            (TelegramSignal.signal_level == "medium", 2),
            else_=1,
        )
        stmt = stmt.order_by(desc(level_order), desc(TelegramSignal.signal_score), desc(TelegramSignal.message_date), desc(TelegramSignal.created_at)).limit(limit)
        return list(session.execute(stmt).scalars().all())
