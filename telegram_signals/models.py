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
    lead_category = Column(String, nullable=True)
    lead_score_100 = Column(Integer, nullable=False, default=0)
    likely_icp = Column(String, nullable=True)
    marketplace = Column(String, nullable=True)
    niche = Column(String, nullable=True)
    budget_hint = Column(String, nullable=True)
    urgency = Column(String, nullable=True)
    opener_soft = Column(Text, nullable=True)
    opener_expert = Column(Text, nullable=True)
    opener_sales = Column(Text, nullable=True)

    status = Column(String, nullable=False, default="new")
    crm_tag = Column(String, nullable=True)
    comment = Column(Text, nullable=True)
    review_status = Column(String, nullable=False, default="unchecked")
    reviewed_at = Column(DateTime, nullable=True)
    is_actionable = Column(Boolean, nullable=False, default=False)
    is_duplicate = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class TelegramSignalComment(Base):
    __tablename__ = "telegram_signal_comments"

    id = Column(Integer, primary_key=True)
    signal_id = Column(Integer, nullable=False, index=True)
    comment = Column(Text, nullable=False)
    author = Column(String, nullable=True, default="dashboard")
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class SearchProfile(Base):
    __tablename__ = "search_profiles"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    segment = Column(String, nullable=False, default="ecom_marketplace_pain")
    queries_text = Column(Text, nullable=False, default="")
    stop_words_text = Column(Text, nullable=True)
    good_chat_hints_text = Column(Text, nullable=True)
    bad_chat_hints_text = Column(Text, nullable=True)
    max_age_hours = Column(Integer, nullable=False, default=96)
    limit_chats = Column(Integer, nullable=False, default=12)
    limit_messages_per_chat = Column(Integer, nullable=False, default=80)
    min_score = Column(Integer, nullable=False, default=0)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)
