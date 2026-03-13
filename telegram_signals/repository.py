from storage.db import SessionLocal
from .models import TelegramSignal


def save_signal(data: dict):
    session = SessionLocal()

    signal = TelegramSignal(**data)

    session.add(signal)
    session.commit()

    session.close()