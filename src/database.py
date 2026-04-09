import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# SECTION 1: Declarative Base
# ─────────────────────────────────────────────

class Base(DeclarativeBase):
    """
    WHY a shared Base class?
    All SQLAlchemy ORM models (table definitions) must inherit from the same
    Base instance. When we call Base.metadata.create_all(engine), SQLAlchemy
    walks every class that inherited from this Base and creates their tables.
    Defining it here means load.py and any future model files all share
    one single source of truth for the schema registry.
    """
    pass


# ─────────────────────────────────────────────
# SECTION 2: Engine Factory
# ─────────────────────────────────────────────

def get_engine():
    """
    Builds and returns a SQLAlchemy engine from environment variables.

    WHY a factory function instead of a module-level engine?
    A module-level engine is created the moment the file is imported — even
    during testing or when running transform.py in isolation. A factory
    function only opens the connection when explicitly called, giving you
    full control over when the database is touched.
    """
    user     = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host     = os.getenv("DB_HOST", "localhost")
    port     = os.getenv("DB_PORT", "5432")
    db_name  = os.getenv("DB_NAME")

    if not all([user, password, db_name]):
        raise EnvironmentError(
            "Missing required DB environment variables. "
            "Check DB_USER, DB_PASSWORD, and DB_NAME in your .env file."
        )

    connection_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"

    engine = create_engine(
        connection_url,
        pool_pre_ping=True,
        echo=True,
    )

    logger.info(f"Database engine created for {host}:{port}/{db_name}")
    return engine


def get_session_factory(engine):
    """Returns a session factory bound to the given engine."""
    return sessionmaker(bind=engine)


def verify_connection(engine) -> bool:
    """
    Sends a lightweight query to confirm the DB is reachable.
    Call this at pipeline startup to fail fast if credentials are wrong.
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connection verified successfully.")
        return True
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False
