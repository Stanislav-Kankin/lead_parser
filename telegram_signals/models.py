from sqlalchemy import Column, Integer, String, Text, DateTime
from datetime import datetime

from storage.db import Base


class TelegramSignal(Base):
    __tablename__ = "telegram_signals"

    id = Column(Integer, primary_key=True)

    chat_title = Column(String)
    chat_username = Column(String)
    chat_url = Column(String)

    message_id = Column(Integer)
    message_date = Column(DateTime)

    author_username = Column(String)

    message_text = Column(Text)
    text_excerpt = Column(Text)

    matched_keywords = Column(Text)

    signal_score = Column(Integer)
    signal_level = Column(String)

    status = Column(String, default="new")

    created_at = Column(DateTime, default=datetime.utcnow)