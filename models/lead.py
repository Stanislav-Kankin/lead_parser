from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import declarative_base
from datetime import datetime

Base = declarative_base()


class Lead(Base):
    __tablename__ = "leads"

    id = Column(Integer, primary_key=True)
    brand = Column(String)
    marketplace = Column(String)
    product_url = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
