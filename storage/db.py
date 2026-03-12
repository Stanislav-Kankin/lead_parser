from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker

from models.lead import Base

DATABASE_URL = "sqlite:///./leads.db"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


REQUIRED_COLUMNS: dict[str, str] = {
    "title": "ALTER TABLE leads ADD COLUMN title VARCHAR",
    "domain_normalized": "ALTER TABLE leads ADD COLUMN domain_normalized VARCHAR",
    "lead_type": "ALTER TABLE leads ADD COLUMN lead_type VARCHAR",
    "priority": "ALTER TABLE leads ADD COLUMN priority VARCHAR",
    "company_email": "ALTER TABLE leads ADD COLUMN company_email VARCHAR",
    "company_phone": "ALTER TABLE leads ADD COLUMN company_phone VARCHAR",
    "status": "ALTER TABLE leads ADD COLUMN status VARCHAR DEFAULT 'new'",
    "updated_at": "ALTER TABLE leads ADD COLUMN updated_at DATETIME",
    "last_enriched_at": "ALTER TABLE leads ADD COLUMN last_enriched_at DATETIME",
}


def _ensure_columns():
    inspector = inspect(engine)
    if "leads" not in inspector.get_table_names():
        return

    columns = {col["name"] for col in inspector.get_columns("leads")}
    with engine.begin() as conn:
        for name, ddl in REQUIRED_COLUMNS.items():
            if name not in columns:
                conn.execute(text(ddl))

        conn.execute(text("UPDATE leads SET domain_normalized = COALESCE(domain_normalized, domain) WHERE domain_normalized IS NULL OR domain_normalized = ''"))
        conn.execute(text("UPDATE leads SET updated_at = COALESCE(updated_at, created_at) WHERE updated_at IS NULL"))
        conn.execute(text("UPDATE leads SET status = COALESCE(status, 'new') WHERE status IS NULL OR status = ''"))

        indexes = {idx["name"] for idx in inspector.get_indexes("leads")}
        if "ix_leads_domain_normalized" not in indexes:
            conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ix_leads_domain_normalized ON leads(domain_normalized)"))


def init_db():
    Base.metadata.create_all(bind=engine)
    _ensure_columns()
