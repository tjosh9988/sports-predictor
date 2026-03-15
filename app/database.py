"""
database.py — Supabase client with graceful retry logic.

Provides:
  - `get_supabase_client()`  : returns the public (anon) Supabase client
  - `get_supabase_admin()`   : returns the service-role client (backend writes)
  - `get_db()`               : SQLAlchemy session dependency for FastAPI
  - SQLAlchemy engine + SessionLocal for ORM usage
"""

import logging
import time
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from supabase import create_client, Client
from supabase.lib.client_options import ClientOptions

from app.config import settings

logger = logging.getLogger(__name__)

# ─────────────────────────── SQLAlchemy ────────────────────────────────────

# Build a connection URL from the Supabase project URL
# Supabase Postgres is exposed at: postgresql://postgres:<password>@db.<ref>.supabase.co:5432/postgres
# We derive it from DATABASE_URL if set, otherwise fall back to a computed value.
try:
    DATABASE_URL: str = settings.model_config.get("DATABASE_URL") or (
        settings.SUPABASE_URL
        .replace("https://", "postgresql://postgres:$(DB_PASSWORD)@db.")
        .replace(".supabase.co", ".supabase.co:5432/postgres")
    )
except AttributeError:
    DATABASE_URL = ""

_engine = None
_SessionLocal = None


def _get_engine():
    global _engine
    if _engine is None and DATABASE_URL:
        _engine = create_engine(
            DATABASE_URL,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,       # detect stale connections
            pool_recycle=1800,        # recycle every 30 minutes
            connect_args={"connect_timeout": 10},
        )
    return _engine


def _get_session_local():
    global _SessionLocal
    if _SessionLocal is None:
        engine = _get_engine()
        if engine:
            _SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return _SessionLocal


def get_db() -> Generator[Session, None, None]:
    """FastAPI dependency: yields a SQLAlchemy session."""
    SessionLocal = _get_session_local()
    if SessionLocal is None:
        raise RuntimeError("Database engine not initialised — check DATABASE_URL.")
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ─────────────────────────── Supabase Clients ──────────────────────────────

def _create_client_with_retry(url: str, key: str, retries: int = 3, delay: float = 2.0) -> Client:
    """
    Creates a Supabase client with exponential-backoff retry.
    Logs a warning on each failure and raises after exhausting retries.
    """
    options = ClientOptions(
        postgrest_client_timeout=15,
        storage_client_timeout=15,
    )
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            client = create_client(url, key, options=options)
            logger.info("Supabase client connected (attempt %d).", attempt)
            return client
        except Exception as exc:
            last_exc = exc
            wait = delay * (2 ** (attempt - 1))
            logger.warning(
                "Supabase connection attempt %d/%d failed: %s — retrying in %.1fs",
                attempt, retries, exc, wait,
            )
            if attempt < retries:
                time.sleep(wait)
    raise ConnectionError(
        f"Could not connect to Supabase after {retries} attempts."
    ) from last_exc


# Public (anon) client — safe to use for reads; respects RLS
_anon_client: Client | None = None

def get_supabase_client() -> Client:
    """Returns the cached anon Supabase client (public reads, RLS enforced)."""
    global _anon_client
    if _anon_client is None:
        _anon_client = _create_client_with_retry(
            settings.SUPABASE_URL,
            settings.SUPABASE_ANON_KEY,
        )
    return _anon_client


# Service-role client — bypasses RLS; ONLY used in backend services
_admin_client: Client | None = None

def get_supabase_admin() -> Client:
    """Returns the cached service-role Supabase client (full DB access, bypasses RLS)."""
    global _admin_client
    if _admin_client is None:
        _admin_client = _create_client_with_retry(
            settings.SUPABASE_URL,
            settings.SUPABASE_SERVICE_ROLE_KEY,
        )
    return _admin_client


def check_supabase_health() -> bool:
    """Ping the DB via the service client. Returns True if healthy."""
    try:
        client = get_supabase_admin()
        client.table("sports").select("id").limit(1).execute()
        return True
    except Exception as exc:
        logger.error("Supabase health check failed: %s", exc)
        return False
