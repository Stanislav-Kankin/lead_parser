from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker

from models.lead import Base

DATABASE_URL = "sqlite:///./leads.db"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


REQUIRED_COLUMNS = {
    "title": "ALTER TABLE leads ADD COLUMN title VARCHAR",
    "domain_normalized": "ALTER TABLE leads ADD COLUMN domain_normalized VARCHAR",
    "icp_score": "ALTER TABLE leads ADD COLUMN icp_score INTEGER DEFAULT 0",
    "lead_type": "ALTER TABLE leads ADD COLUMN lead_type VARCHAR",
    "priority": "ALTER TABLE leads ADD COLUMN priority VARCHAR",
    "opener": "ALTER TABLE leads ADD COLUMN opener VARCHAR",
    "meta_description": "ALTER TABLE leads ADD COLUMN meta_description VARCHAR",
    "company_email": "ALTER TABLE leads ADD COLUMN company_email VARCHAR",
    "company_phone": "ALTER TABLE leads ADD COLUMN company_phone VARCHAR",
    "status": "ALTER TABLE leads ADD COLUMN status VARCHAR DEFAULT 'new'",
    "owner": "ALTER TABLE leads ADD COLUMN owner VARCHAR",
    "comment": "ALTER TABLE leads ADD COLUMN comment VARCHAR",
    "updated_at": "ALTER TABLE leads ADD COLUMN updated_at DATETIME",
    "last_enriched_at": "ALTER TABLE leads ADD COLUMN last_enriched_at DATETIME",
}


def _ensure_columns():
    inspector = inspect(engine)
    if "leads" not in inspector.get_table_names():
        return

    columns = {col["name"] for col in inspector.get_columns("leads")}
    with engine.begin() as conn:
        for name, sql in REQUIRED_COLUMNS.items():
            if name not in columns:
                conn.execute(text(sql))

        conn.execute(text("UPDATE leads SET domain_normalized = lower(domain) WHERE domain_normalized IS NULL AND domain IS NOT NULL"))
        conn.execute(text("UPDATE leads SET updated_at = created_at WHERE updated_at IS NULL"))
        conn.execute(text("UPDATE leads SET status = 'new' WHERE status IS NULL OR status = ''"))


def init_db():
    Base.metadata.create_all(bind=engine)
    _ensure_columns()
