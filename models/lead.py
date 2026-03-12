from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, Integer, String
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

    is_icp = Column(Boolean, nullable=False, default=False)
    icp_reason = Column(String, nullable=True)
    hypothesis = Column(String, nullable=True)
    opener = Column(String, nullable=True)
    lead_type = Column(String, nullable=True)
    priority = Column(String, nullable=True)
    title = Column(String, nullable=True)

    company_inn = Column(String, nullable=True)
    company_legal_name = Column(String, nullable=True)
    company_email = Column(String, nullable=True)
    company_phone = Column(String, nullable=True)
    employees = Column(String, nullable=True)
    contacts_source = Column(String, nullable=True)
    contact_confidence = Column(String, nullable=True)
    has_contacts = Column(Boolean, nullable=False, default=False)
    sales_ready = Column(Boolean, nullable=False, default=False)

    status = Column(String, nullable=False, default="new")
    owner = Column(String, nullable=True)
    comment = Column(String, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    last_enriched_at = Column(DateTime, nullable=True)
