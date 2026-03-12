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
    "opener": "ALTER TABLE leads ADD COLUMN opener VARCHAR",
    "lead_type": "ALTER TABLE leads ADD COLUMN lead_type VARCHAR",
    "priority": "ALTER TABLE leads ADD COLUMN priority VARCHAR",
    "company_inn": "ALTER TABLE leads ADD COLUMN company_inn VARCHAR",
    "company_legal_name": "ALTER TABLE leads ADD COLUMN company_legal_name VARCHAR",
    "company_email": "ALTER TABLE leads ADD COLUMN company_email VARCHAR",
    "company_phone": "ALTER TABLE leads ADD COLUMN company_phone VARCHAR",
    "employees": "ALTER TABLE leads ADD COLUMN employees VARCHAR",
    "contacts_source": "ALTER TABLE leads ADD COLUMN contacts_source VARCHAR",
    "contact_confidence": "ALTER TABLE leads ADD COLUMN contact_confidence VARCHAR",
    "has_contacts": "ALTER TABLE leads ADD COLUMN has_contacts BOOLEAN DEFAULT 0",
    "sales_ready": "ALTER TABLE leads ADD COLUMN sales_ready BOOLEAN DEFAULT 0",
    "status": "ALTER TABLE leads ADD COLUMN status VARCHAR DEFAULT 'new'",
    "owner": "ALTER TABLE leads ADD COLUMN owner VARCHAR",
    "comment": "ALTER TABLE leads ADD COLUMN comment VARCHAR",
    "updated_at": "ALTER TABLE leads ADD COLUMN updated_at DATETIME",
    "last_enriched_at": "ALTER TABLE leads ADD COLUMN last_enriched_at DATETIME",
    "domain_normalized": "ALTER TABLE leads ADD COLUMN domain_normalized VARCHAR",
    "root_domain": "ALTER TABLE leads ADD COLUMN root_domain VARCHAR",
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

        conn.execute(text("UPDATE leads SET updated_at = COALESCE(updated_at, created_at)"))
        conn.execute(text("UPDATE leads SET status = COALESCE(status, 'new')"))
        conn.execute(text("UPDATE leads SET domain_normalized = COALESCE(domain_normalized, domain)"))
        conn.execute(text("UPDATE leads SET root_domain = COALESCE(root_domain, domain)"))
        conn.execute(text("UPDATE leads SET has_contacts = CASE WHEN company_email IS NOT NULL OR company_phone IS NOT NULL THEN 1 ELSE COALESCE(has_contacts, 0) END"))
        conn.execute(text("UPDATE leads SET sales_ready = CASE WHEN COALESCE(is_icp, 0) = 1 AND COALESCE(has_contacts, 0) = 1 THEN 1 ELSE 0 END"))
        conn.execute(text("UPDATE leads SET contact_confidence = COALESCE(contact_confidence, CASE WHEN company_inn IS NOT NULL THEN 'high' WHEN company_email IS NOT NULL OR company_phone IS NOT NULL THEN 'medium' ELSE 'low' END)"))


def init_db():
    Base.metadata.create_all(bind=engine)
    _ensure_columns()
