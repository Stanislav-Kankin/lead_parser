from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker

from models.lead import Base

DATABASE_URL = "sqlite:///./leads.db"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def _ensure_columns():
    inspector = inspect(engine)
    if "leads" not in inspector.get_table_names():
        return

    columns = {col["name"] for col in inspector.get_columns("leads")}
    with engine.begin() as conn:
        if "title" not in columns:
            conn.execute(text("ALTER TABLE leads ADD COLUMN title VARCHAR"))


def init_db():
    Base.metadata.create_all(bind=engine)
    _ensure_columns()
