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

    signal_score = Column(Integer, nullable=False, default=0)
    signal_level = Column(String, nullable=True)
    recommended_opener = Column(Text, nullable=True)

    source_type = Column(String, nullable=True, default="chat_message")
    is_comment = Column(Boolean, nullable=False, default=False)
    parent_message_id = Column(Integer, nullable=True)
    root_message_id = Column(Integer, nullable=True)

    reply_depth = Column(Integer, nullable=False, default=0)
    conversation_key = Column(String, nullable=True)
    conversation_score = Column(Integer, nullable=False, default=0)
    pain_detected = Column(Text, nullable=True)
    icp_detected = Column(Text, nullable=True)

    message_type = Column(String, nullable=True)
    conversation_type = Column(String, nullable=True)
    author_type_guess = Column(String, nullable=True)

    icp_score = Column(Integer, nullable=False, default=0)
    pain_score = Column(Integer, nullable=False, default=0)
    intent_score = Column(Integer, nullable=False, default=0)
    context_score = Column(Integer, nullable=False, default=0)
    owner_likelihood_score = Column(Integer, nullable=False, default=0)
    promo_penalty = Column(Integer, nullable=False, default=0)
    contractor_penalty = Column(Integer, nullable=False, default=0)
    final_lead_score = Column(Integer, nullable=False, default=0)
    contactability_score = Column(Integer, nullable=False, default=0)
    contact_entity_type = Column(String, nullable=True)
    contact_entity_score = Column(Integer, nullable=False, default=0)
    is_person_reachable = Column(Boolean, nullable=False, default=False)

    lead_fit = Column(String, nullable=True, default="noise")
    next_step = Column(String, nullable=True)
    why_actionable = Column(Text, nullable=True)
    company_hint = Column(String, nullable=True)
    website_hint = Column(String, nullable=True)
    contact_hint = Column(String, nullable=True)
    outreach_segment = Column(String, nullable=True)
    outreach_stage = Column(String, nullable=True)
    outreach_angle = Column(Text, nullable=True)

    status = Column(String, nullable=False, default="new")
    review_status = Column(String, nullable=False, default="unchecked")
    reviewed_at = Column(DateTime, nullable=True)
    is_actionable = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
