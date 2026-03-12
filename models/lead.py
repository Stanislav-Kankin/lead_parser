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
    domain_normalized = Column(String, nullable=True, unique=True)
    source = Column(String, nullable=False, default="ddgs")

    is_icp = Column(Boolean, nullable=False, default=False)
    icp_reason = Column(String, nullable=True)
    icp_score = Column(Integer, nullable=False, default=0)
    lead_type = Column(String, nullable=True)
    priority = Column(String, nullable=True)
    hypothesis = Column(String, nullable=True)
    opener = Column(String, nullable=True)

    title = Column(String, nullable=True)
    meta_description = Column(String, nullable=True)

    company_email = Column(String, nullable=True)
    company_phone = Column(String, nullable=True)

    status = Column(String, nullable=False, default="new")
    owner = Column(String, nullable=True)
    comment = Column(String, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    last_enriched_at = Column(DateTime, nullable=True)
