from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, Integer, String, Text

from models.lead import Base


class TelegramSignal(Base):
    __tablename__ = "telegram_signals"

    id = Column(Integer, primary_key=True)
    source_query = Column(String, nullable=True)
    segment = Column(String, nullable=True)

    chat_id = Column(String, nullable=True)
    chat_title = Column(String, nullable=True)
    chat_username = Column(String, nullable=True)
    chat_url = Column(String, nullable=True)

    message_id = Column(Integer, nullable=False)
    message_date = Column(DateTime, nullable=True)

    author_id = Column(String, nullable=True)
    author_name = Column(String, nullable=True)
    author_username = Column(String, nullable=True)

    message_text = Column(Text, nullable=True)
    text_excerpt = Column(Text, nullable=True)
    matched_keywords = Column(Text, nullable=True)

    message_type = Column(String, nullable=True)
    icp_score = Column(Integer, nullable=False, default=0)
    pain_score = Column(Integer, nullable=False, default=0)
    intent_score = Column(Integer, nullable=False, default=0)
    contactability_score = Column(Integer, nullable=False, default=0)
    is_actionable = Column(Boolean, nullable=False, default=False)
    contact_hint = Column(String, nullable=True)
    company_hint = Column(String, nullable=True)
    website_hint = Column(String, nullable=True)

    signal_score = Column(Integer, nullable=False, default=0)
    signal_level = Column(String, nullable=True)
    recommended_opener = Column(Text, nullable=True)

    status = Column(String, nullable=False, default="new")
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
