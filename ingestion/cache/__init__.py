"""Cache package.

``build_cache_database`` selects the durable PostgreSQL-backed cache when a
Postgres ``DATABASE_URL`` is configured (CI and Cloud Run prod), and falls
back to the local SQLite cache for local dev / unit tests. This is the
KAN-230 fix seam: prod no longer relies on an ephemeral SQLite file.
"""

from .database import CacheDatabase
from .postgres_database import PostgresCacheDatabase

__all__ = ["CacheDatabase", "PostgresCacheDatabase", "build_cache_database"]


def _is_postgres_url(url: str) -> bool:
    scheme = (url or "").strip().split("://", 1)[0].lower()
    return scheme.startswith("postgres")  # postgres:// or postgresql[+driver]://


def build_cache_database(settings):
    """Return the right cache backend for the current environment.

    Postgres when ``settings.database_url`` is a postgres URL (persists across
    Cloud Run Job executions — fixes KAN-230); SQLite otherwise.
    """
    db_url = (getattr(settings, "database_url", "") or "").strip()
    if db_url and _is_postgres_url(db_url):
        return PostgresCacheDatabase(db_url)
    return CacheDatabase(settings.cache_db_path)
