from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker

from models.lead import Base
from telegram_signals.models import TelegramSignal  # noqa: F401

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
    "company_ogrn": "ALTER TABLE leads ADD COLUMN company_ogrn VARCHAR",
    "company_legal_name": "ALTER TABLE leads ADD COLUMN company_legal_name VARCHAR",
    "legal_form": "ALTER TABLE leads ADD COLUMN legal_form VARCHAR",
    "inn_source": "ALTER TABLE leads ADD COLUMN inn_source VARCHAR",
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

TELEGRAM_SIGNAL_REQUIRED_COLUMNS = {
    "message_type": "ALTER TABLE telegram_signals ADD COLUMN message_type VARCHAR",
    "icp_score": "ALTER TABLE telegram_signals ADD COLUMN icp_score INTEGER DEFAULT 0",
    "pain_score": "ALTER TABLE telegram_signals ADD COLUMN pain_score INTEGER DEFAULT 0",
    "intent_score": "ALTER TABLE telegram_signals ADD COLUMN intent_score INTEGER DEFAULT 0",
    "contactability_score": "ALTER TABLE telegram_signals ADD COLUMN contactability_score INTEGER DEFAULT 0",
    "is_actionable": "ALTER TABLE telegram_signals ADD COLUMN is_actionable BOOLEAN DEFAULT 0",
    "contact_hint": "ALTER TABLE telegram_signals ADD COLUMN contact_hint VARCHAR",
    "company_hint": "ALTER TABLE telegram_signals ADD COLUMN company_hint VARCHAR",
    "website_hint": "ALTER TABLE telegram_signals ADD COLUMN website_hint VARCHAR",
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
        conn.execute(text("UPDATE leads SET contact_confidence = COALESCE(contact_confidence, CASE WHEN company_inn IS NOT NULL OR company_legal_name IS NOT NULL THEN 'high' WHEN company_email IS NOT NULL AND company_phone IS NOT NULL THEN 'medium' WHEN company_email IS NOT NULL OR company_phone IS NOT NULL THEN 'low' ELSE 'low' END)"))


def _ensure_telegram_signal_columns():
    inspector = inspect(engine)
    if "telegram_signals" not in inspector.get_table_names():
        return

    columns = {col["name"] for col in inspector.get_columns("telegram_signals")}
    with engine.begin() as conn:
        for name, ddl in TELEGRAM_SIGNAL_REQUIRED_COLUMNS.items():
            if name not in columns:
                conn.execute(text(ddl))

        conn.execute(text("UPDATE telegram_signals SET icp_score = COALESCE(icp_score, 0)"))
        conn.execute(text("UPDATE telegram_signals SET pain_score = COALESCE(pain_score, 0)"))
        conn.execute(text("UPDATE telegram_signals SET intent_score = COALESCE(intent_score, 0)"))
        conn.execute(text("UPDATE telegram_signals SET contactability_score = COALESCE(contactability_score, 0)"))
        conn.execute(text("UPDATE telegram_signals SET is_actionable = COALESCE(is_actionable, 0)"))


def init_db():
    Base.metadata.create_all(bind=engine)
    _ensure_columns()
    _ensure_telegram_signal_columns()
