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
    source = Column(String, nullable=False, default="ddg")
    is_icp = Column(Boolean, nullable=False, default=False)
    icp_reason = Column(String, nullable=True)
    hypothesis = Column(String, nullable=True)
    title = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
