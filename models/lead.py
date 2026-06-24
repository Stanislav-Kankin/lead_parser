from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Lead(Base):
    __tablename__ = "leads"

    id = Column(Integer, primary_key=True)
    query = Column(String, nullable=False)
    company_name = Column(String, nullable=True)
    domain = Column(String, nullable=False, unique=True)
    domain_normalized = Column(String, nullable=False, unique=True)
    root_domain = Column(String, nullable=True)
    source = Column(String, nullable=False, default="ddgs")
    source_url = Column(String, nullable=True)
    search_category = Column(String, nullable=True)

    is_icp = Column(Boolean, nullable=False, default=False)
    icp_score = Column(Integer, nullable=False, default=0)
    icp_reason = Column(String, nullable=True)
    evidence = Column(Text, nullable=True)
    outreach_angle = Column(Text, nullable=True)
    hypothesis = Column(String, nullable=True)
    opener = Column(String, nullable=True)
    cjm_stage = Column(String, nullable=True)
    lead_type = Column(String, nullable=True)
    priority = Column(String, nullable=True)
    title = Column(String, nullable=True)

    company_inn = Column(String, nullable=True)
    company_ogrn = Column(String, nullable=True)
    company_legal_name = Column(String, nullable=True)
    legal_form = Column(String, nullable=True)
    inn_source = Column(String, nullable=True)

    focus_loaded_at = Column(DateTime, nullable=True)
    focus_legal_name = Column(String, nullable=True)
    focus_status = Column(String, nullable=True)
    focus_region = Column(String, nullable=True)
    focus_address = Column(String, nullable=True)
    focus_revenue = Column(String, nullable=True)
    focus_balance = Column(String, nullable=True)
    focus_profit = Column(String, nullable=True)
    focus_arbitration = Column(String, nullable=True)
    focus_employees = Column(String, nullable=True)
    focus_okved = Column(String, nullable=True)
    focus_other_okved = Column(Text, nullable=True)
    focus_director = Column(String, nullable=True)
    focus_msp = Column(String, nullable=True)
    focus_phone = Column(Text, nullable=True)
    focus_email = Column(Text, nullable=True)
    focus_website = Column(String, nullable=True)
    focus_registration_date = Column(String, nullable=True)

    company_email = Column(String, nullable=True)
    company_phone = Column(String, nullable=True)
    employees = Column(String, nullable=True)
    contacts_source = Column(String, nullable=True)
    contact_confidence = Column(String, nullable=True)
    has_contacts = Column(Boolean, nullable=False, default=False)
    has_catalog = Column(Boolean, nullable=False, default=False)
    has_cart = Column(Boolean, nullable=False, default=False)
    ecommerce_score = Column(Integer, nullable=False, default=0)
    site_type = Column(String, nullable=True)
    site_assessment = Column(Text, nullable=True)
    sales_ready = Column(Boolean, nullable=False, default=False)

    status = Column(String, nullable=False, default="new")
    owner = Column(String, nullable=True)
    comment = Column(String, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    last_enriched_at = Column(DateTime, nullable=True)


class SearchProject(Base):
    __tablename__ = "search_projects"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class LeadProject(Base):
    __tablename__ = "lead_projects"
    __table_args__ = (UniqueConstraint("lead_id", "project_id", name="uq_lead_project"),)

    id = Column(Integer, primary_key=True)
    lead_id = Column(Integer, nullable=False, index=True)
    project_id = Column(Integer, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class SocialLead(Base):
    __tablename__ = "social_leads"

    id = Column(Integer, primary_key=True)
    source = Column(String, nullable=False, default="tenchat")
    source_url = Column(String, nullable=False, unique=True)
    source_query = Column(Text, nullable=True)
    profile_url = Column(String, nullable=True)
    post_url = Column(String, nullable=True)

    person_name = Column(String, nullable=True)
    role_title = Column(String, nullable=True)
    company_name = Column(String, nullable=True)
    company_inn = Column(String, nullable=True)
    company_ogrn = Column(String, nullable=True)
    company_legal_name = Column(String, nullable=True)
    company_url = Column(String, nullable=True)
    focus_loaded_at = Column(DateTime, nullable=True)
    focus_legal_name = Column(String, nullable=True)
    focus_status = Column(String, nullable=True)
    focus_region = Column(String, nullable=True)
    focus_address = Column(String, nullable=True)
    focus_revenue = Column(String, nullable=True)
    focus_balance = Column(String, nullable=True)
    focus_profit = Column(String, nullable=True)
    focus_arbitration = Column(String, nullable=True)
    focus_employees = Column(String, nullable=True)
    focus_okved = Column(String, nullable=True)
    focus_other_okved = Column(Text, nullable=True)
    focus_director = Column(String, nullable=True)
    focus_msp = Column(String, nullable=True)
    focus_phone = Column(Text, nullable=True)
    focus_email = Column(Text, nullable=True)
    focus_website = Column(String, nullable=True)
    focus_registration_date = Column(String, nullable=True)
    matched_web_lead_id = Column(Integer, nullable=True, index=True)
    matched_web_domain = Column(String, nullable=True)
    matched_web_title = Column(String, nullable=True)
    title = Column(String, nullable=True)
    snippet = Column(Text, nullable=True)
    text = Column(Text, nullable=True)

    lead_score = Column(Integer, nullable=False, default=0)
    lead_fit = Column(String, nullable=True)
    likely_icp = Column(String, nullable=True)
    pain_detected = Column(Text, nullable=True)
    cjm_stage = Column(String, nullable=True)
    why_relevant = Column(Text, nullable=True)
    outreach_angle = Column(Text, nullable=True)
    opener = Column(Text, nullable=True)

    status = Column(String, nullable=False, default="new")
    owner = Column(String, nullable=True)
    comment = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class SocialLeadProject(Base):
    __tablename__ = "social_lead_projects"
    __table_args__ = (UniqueConstraint("social_lead_id", "project_id", name="uq_social_lead_project"),)

    id = Column(Integer, primary_key=True)
    social_lead_id = Column(Integer, nullable=False, index=True)
    project_id = Column(Integer, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
