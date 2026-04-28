import os

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker

from models.lead import Base
from telegram_signals.models import TelegramSignal  # noqa: F401

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./leads.db")

if DATABASE_URL.startswith("sqlite:///"):
    db_path = DATABASE_URL.replace("sqlite:///", "", 1)
    if db_path and db_path != ":memory:":
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)



TELEGRAM_SIGNAL_REQUIRED_COLUMNS = {
    "source_type": "ALTER TABLE telegram_signals ADD COLUMN source_type VARCHAR DEFAULT 'chat_message'",
    "is_comment": "ALTER TABLE telegram_signals ADD COLUMN is_comment BOOLEAN DEFAULT 0",
    "parent_message_id": "ALTER TABLE telegram_signals ADD COLUMN parent_message_id INTEGER",
    "root_message_id": "ALTER TABLE telegram_signals ADD COLUMN root_message_id INTEGER",
    "reply_depth": "ALTER TABLE telegram_signals ADD COLUMN reply_depth INTEGER DEFAULT 0",
    "conversation_key": "ALTER TABLE telegram_signals ADD COLUMN conversation_key VARCHAR",
    "conversation_score": "ALTER TABLE telegram_signals ADD COLUMN conversation_score INTEGER DEFAULT 0",
    "pain_detected": "ALTER TABLE telegram_signals ADD COLUMN pain_detected TEXT",
    "icp_detected": "ALTER TABLE telegram_signals ADD COLUMN icp_detected TEXT",
    "message_type": "ALTER TABLE telegram_signals ADD COLUMN message_type VARCHAR",
    "conversation_type": "ALTER TABLE telegram_signals ADD COLUMN conversation_type VARCHAR",
    "author_type_guess": "ALTER TABLE telegram_signals ADD COLUMN author_type_guess VARCHAR",
    "icp_score": "ALTER TABLE telegram_signals ADD COLUMN icp_score INTEGER DEFAULT 0",
    "pain_score": "ALTER TABLE telegram_signals ADD COLUMN pain_score INTEGER DEFAULT 0",
    "intent_score": "ALTER TABLE telegram_signals ADD COLUMN intent_score INTEGER DEFAULT 0",
    "context_score": "ALTER TABLE telegram_signals ADD COLUMN context_score INTEGER DEFAULT 0",
    "owner_likelihood_score": "ALTER TABLE telegram_signals ADD COLUMN owner_likelihood_score INTEGER DEFAULT 0",
    "promo_penalty": "ALTER TABLE telegram_signals ADD COLUMN promo_penalty INTEGER DEFAULT 0",
    "contractor_penalty": "ALTER TABLE telegram_signals ADD COLUMN contractor_penalty INTEGER DEFAULT 0",
    "final_lead_score": "ALTER TABLE telegram_signals ADD COLUMN final_lead_score INTEGER DEFAULT 0",
    "contactability_score": "ALTER TABLE telegram_signals ADD COLUMN contactability_score INTEGER DEFAULT 0",
    "contact_entity_type": "ALTER TABLE telegram_signals ADD COLUMN contact_entity_type VARCHAR",
    "contact_entity_score": "ALTER TABLE telegram_signals ADD COLUMN contact_entity_score INTEGER DEFAULT 0",
    "is_person_reachable": "ALTER TABLE telegram_signals ADD COLUMN is_person_reachable BOOLEAN DEFAULT 0",
    "why_actionable": "ALTER TABLE telegram_signals ADD COLUMN why_actionable TEXT",
    "company_hint": "ALTER TABLE telegram_signals ADD COLUMN company_hint VARCHAR",
    "website_hint": "ALTER TABLE telegram_signals ADD COLUMN website_hint VARCHAR",
    "contact_hint": "ALTER TABLE telegram_signals ADD COLUMN contact_hint VARCHAR",
    "outreach_segment": "ALTER TABLE telegram_signals ADD COLUMN outreach_segment VARCHAR",
    "outreach_stage": "ALTER TABLE telegram_signals ADD COLUMN outreach_stage VARCHAR",
    "outreach_angle": "ALTER TABLE telegram_signals ADD COLUMN outreach_angle TEXT",
    "lead_category": "ALTER TABLE telegram_signals ADD COLUMN lead_category VARCHAR",
    "lead_score_100": "ALTER TABLE telegram_signals ADD COLUMN lead_score_100 INTEGER DEFAULT 0",
    "likely_icp": "ALTER TABLE telegram_signals ADD COLUMN likely_icp VARCHAR",
    "marketplace": "ALTER TABLE telegram_signals ADD COLUMN marketplace VARCHAR",
    "niche": "ALTER TABLE telegram_signals ADD COLUMN niche VARCHAR",
    "budget_hint": "ALTER TABLE telegram_signals ADD COLUMN budget_hint VARCHAR",
    "urgency": "ALTER TABLE telegram_signals ADD COLUMN urgency VARCHAR",
    "opener_soft": "ALTER TABLE telegram_signals ADD COLUMN opener_soft TEXT",
    "opener_expert": "ALTER TABLE telegram_signals ADD COLUMN opener_expert TEXT",
    "opener_sales": "ALTER TABLE telegram_signals ADD COLUMN opener_sales TEXT",
    "lead_fit": "ALTER TABLE telegram_signals ADD COLUMN lead_fit VARCHAR DEFAULT 'noise'",
    "next_step": "ALTER TABLE telegram_signals ADD COLUMN next_step VARCHAR",
    "status": "ALTER TABLE telegram_signals ADD COLUMN status VARCHAR DEFAULT 'new'",
    "crm_tag": "ALTER TABLE telegram_signals ADD COLUMN crm_tag VARCHAR",
    "comment": "ALTER TABLE telegram_signals ADD COLUMN comment TEXT",
    "review_status": "ALTER TABLE telegram_signals ADD COLUMN review_status VARCHAR DEFAULT 'unchecked'",
    "reviewed_at": "ALTER TABLE telegram_signals ADD COLUMN reviewed_at DATETIME",
    "is_actionable": "ALTER TABLE telegram_signals ADD COLUMN is_actionable BOOLEAN DEFAULT 0"
}


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

        conn.execute(text("UPDATE telegram_signals SET source_type = COALESCE(source_type, 'chat_message')"))
        conn.execute(text("UPDATE telegram_signals SET is_comment = COALESCE(is_comment, 0)"))
        conn.execute(text("UPDATE telegram_signals SET is_actionable = COALESCE(is_actionable, 0)"))
        conn.execute(text("UPDATE telegram_signals SET status = COALESCE(status, 'new')"))
        conn.execute(text("UPDATE telegram_signals SET review_status = COALESCE(review_status, 'unchecked')"))
        conn.execute(text("UPDATE telegram_signals SET final_lead_score = COALESCE(final_lead_score, signal_score, 0)"))
        conn.execute(text("UPDATE telegram_signals SET icp_score = COALESCE(icp_score, 0)"))
        conn.execute(text("UPDATE telegram_signals SET pain_score = COALESCE(pain_score, 0)"))
        conn.execute(text("UPDATE telegram_signals SET intent_score = COALESCE(intent_score, 0)"))
        conn.execute(text("UPDATE telegram_signals SET context_score = COALESCE(context_score, 0)"))
        conn.execute(text("UPDATE telegram_signals SET owner_likelihood_score = COALESCE(owner_likelihood_score, 0)"))
        conn.execute(text("UPDATE telegram_signals SET promo_penalty = COALESCE(promo_penalty, 0)"))
        conn.execute(text("UPDATE telegram_signals SET contractor_penalty = COALESCE(contractor_penalty, 0)"))
        conn.execute(text("UPDATE telegram_signals SET contactability_score = COALESCE(contactability_score, 0)"))
        conn.execute(text("UPDATE telegram_signals SET contact_entity_score = COALESCE(contact_entity_score, 0)"))
        conn.execute(text("UPDATE telegram_signals SET is_person_reachable = COALESCE(is_person_reachable, 0)"))
        conn.execute(text("UPDATE telegram_signals SET reply_depth = COALESCE(reply_depth, 0)"))
        conn.execute(text("UPDATE telegram_signals SET conversation_score = COALESCE(conversation_score, 0)"))
        conn.execute(text("UPDATE telegram_signals SET lead_score_100 = COALESCE(lead_score_100, final_lead_score, signal_score, 0)"))


def init_db():
    Base.metadata.create_all(bind=engine)
    _ensure_columns()
    _ensure_telegram_signal_columns()
