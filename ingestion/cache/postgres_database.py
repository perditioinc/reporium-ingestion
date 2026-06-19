"""PostgreSQL-backed cache — durable replacement for the SQLite CacheDatabase.

KAN-230: the SQLite cache lives at ``./data/cache.db`` on the container's
ephemeral filesystem. Cloud Run Jobs get a fresh filesystem every execution,
so the dedup cache was empty on every run and the nightly enrichment job
re-fetched/re-enriched the full ~1900-repo backlog from scratch each time —
never finishing inside the 2h timeout.

This class persists the same data in PostgreSQL (``reporium-db``), which
survives across job executions, so ``needs_*_fetch`` correctly skips work
already done and the job completes in minutes again.

It is a drop-in replacement: identical async method surface to
``CacheDatabase``. psycopg2 is synchronous, so each operation runs in a
worker thread via ``asyncio.to_thread`` to preserve the async interface.
Connection-per-operation matches the convention used elsewhere in this
codebase (graph/atomic_swap, enrichers/ai_enricher, extractors/dependencies).
"""

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Any

import psycopg2
import psycopg2.extras

from .models import RepoCacheRow, IngestionRun


# Postgres DDL — additive, idempotent. These three tables are new and do not
# collide with the reporium-api schema (which uses ``ingest_runs`` — note the
# different name — plus ``repos``/edges/embeddings). No alembic migration is
# required precisely because every statement is CREATE TABLE IF NOT EXISTS.
_PG_DDL = """
CREATE TABLE IF NOT EXISTS repo_cache (
  name                 TEXT PRIMARY KEY,
  github_updated_at    TEXT,
  upstream_created_at  TEXT,
  original_owner       TEXT,
  forked_from          TEXT,
  permanent_fetched_at TEXT,
  parent_stars         INTEGER,
  parent_forks         INTEGER,
  parent_archived      BOOLEAN,
  language_breakdown   TEXT,
  weekly_fetched_at    TEXT,
  readme_content       TEXT,
  recent_commits       TEXT,
  latest_release       TEXT,
  daily_fetched_at     TEXT,
  fork_sync_state      TEXT,
  behind_by            INTEGER,
  ahead_by             INTEGER,
  sync_fetched_at      TEXT,
  completed_at                TEXT,
  completed_github_updated_at TEXT
);

-- Additive, idempotent: existing deployments already have a repo_cache table
-- (CREATE TABLE IF NOT EXISTS is a no-op there), so the COMPLETED-checkpoint
-- columns must be ALTERed in explicitly. Postgres supports IF NOT EXISTS here.
ALTER TABLE repo_cache ADD COLUMN IF NOT EXISTS completed_at TEXT;
ALTER TABLE repo_cache ADD COLUMN IF NOT EXISTS completed_github_updated_at TEXT;

CREATE TABLE IF NOT EXISTS ingestion_runs (
  id              BIGSERIAL PRIMARY KEY,
  started_at      TEXT NOT NULL,
  completed_at    TEXT,
  mode            TEXT NOT NULL,
  repos_processed INTEGER DEFAULT 0,
  repos_updated   INTEGER DEFAULT 0,
  api_calls_made  INTEGER DEFAULT 0,
  rate_limit_hits INTEGER DEFAULT 0,
  status          TEXT DEFAULT 'running'
);

CREATE TABLE IF NOT EXISTS api_call_log (
  id          BIGSERIAL PRIMARY KEY,
  timestamp   TEXT NOT NULL,
  endpoint    TEXT NOT NULL,
  status_code INTEGER,
  rate_limit_remaining INTEGER
);
"""


def normalize_db_url(url: str) -> str:
    """Strip the async driver suffix so psycopg2 accepts the shared secret.

    The same ``DATABASE_URL`` / ``reporium-db-url`` secret is used by async
    API code (``postgresql+asyncpg://``) and by sync ingestion code. psycopg2
    only understands the plain ``postgresql://`` scheme.
    """
    url = (url or "").strip().replace("+asyncpg", "")
    if url.startswith("postgresql+"):
        url = "postgresql" + url[url.index("://"):]
    return url


class PostgresCacheDatabase:
    def __init__(self, db_url: str):
        self.db_url = normalize_db_url(db_url)

    def _connect(self):
        conn = psycopg2.connect(self.db_url)
        conn.autocommit = True
        return conn

    # ── schema ───────────────────────────────────────────────────────────────

    def _init_sync(self) -> None:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(_PG_DDL)
        finally:
            conn.close()

    async def init(self) -> None:
        await asyncio.to_thread(self._init_sync)

    # ── repo_cache ───────────────────────────────────────────────────────────

    def _get_repo_sync(self, name: str) -> RepoCacheRow | None:
        conn = self._connect()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM repo_cache WHERE name = %s", (name,))
                row = cur.fetchone()
                return RepoCacheRow(**dict(row)) if row else None
        finally:
            conn.close()

    async def get_repo(self, name: str) -> RepoCacheRow | None:
        return await asyncio.to_thread(self._get_repo_sync, name)

    def _get_all_repos_sync(self) -> list[RepoCacheRow]:
        conn = self._connect()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM repo_cache")
                return [RepoCacheRow(**dict(r)) for r in cur.fetchall()]
        finally:
            conn.close()

    async def get_all_repos(self) -> list[RepoCacheRow]:
        return await asyncio.to_thread(self._get_all_repos_sync)

    def _upsert_repo_sync(self, row: RepoCacheRow) -> None:
        data = row.model_dump()
        cols = list(data.keys())
        col_list = ", ".join(cols)
        placeholders = ", ".join(["%s"] * len(cols))
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != "name")
        sql = (
            f"INSERT INTO repo_cache ({col_list}) VALUES ({placeholders}) "
            f"ON CONFLICT (name) DO UPDATE SET {updates}"
        )
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, [data[c] for c in cols])
        finally:
            conn.close()

    async def upsert_repo(self, row: RepoCacheRow) -> None:
        await asyncio.to_thread(self._upsert_repo_sync, row)

    def _mark_completed_sync(self, name: str, github_updated_at: str | None) -> None:
        now = datetime.now(timezone.utc).isoformat()
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE repo_cache "
                    "SET completed_at=%s, completed_github_updated_at=%s "
                    "WHERE name=%s",
                    (now, github_updated_at, name),
                )
        finally:
            conn.close()

    async def mark_completed(self, name: str, github_updated_at: str | None) -> None:
        """Set the COMPLETED checkpoint for a repo AFTER it is fully posted to
        the API (lost-work fix). Idempotent UPDATE keyed on name."""
        await asyncio.to_thread(self._mark_completed_sync, name, github_updated_at)

    async def needs_permanent_fetch(self, name: str) -> bool:
        row = await self.get_repo(name)
        return row is None or row.permanent_fetched_at is None

    async def needs_weekly_fetch(self, name: str) -> bool:
        row = await self.get_repo(name)
        if row is None or row.weekly_fetched_at is None:
            return True
        fetched = datetime.fromisoformat(row.weekly_fetched_at)
        return (datetime.now(timezone.utc) - fetched).days >= 7

    async def needs_daily_fetch(self, name: str, current_github_updated_at: str) -> bool:
        row = await self.get_repo(name)
        if row is None or row.daily_fetched_at is None:
            return True
        return row.github_updated_at != current_github_updated_at

    async def needs_sync_fetch(self, name: str) -> bool:
        row = await self.get_repo(name)
        if row is None or row.sync_fetched_at is None:
            return True
        fetched = datetime.fromisoformat(row.sync_fetched_at)
        return (datetime.now(timezone.utc) - fetched).total_seconds() < 3600

    # ── ingestion_runs ───────────────────────────────────────────────────────

    def _start_run_sync(self, mode: str) -> int:
        now = datetime.now(timezone.utc).isoformat()
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO ingestion_runs (started_at, mode, status) "
                    "VALUES (%s, %s, %s) RETURNING id",
                    (now, mode, "running"),
                )
                return cur.fetchone()[0]
        finally:
            conn.close()

    async def start_run(self, mode: str) -> int:
        return await asyncio.to_thread(self._start_run_sync, mode)

    def _finish_run_sync(self, run_id: int, repos_processed: int, repos_updated: int,
                         api_calls_made: int, rate_limit_hits: int, status: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE ingestion_runs
                       SET completed_at=%s, repos_processed=%s, repos_updated=%s,
                           api_calls_made=%s, rate_limit_hits=%s, status=%s
                       WHERE id=%s""",
                    (now, repos_processed, repos_updated,
                     api_calls_made, rate_limit_hits, status, run_id),
                )
        finally:
            conn.close()

    async def finish_run(self, run_id: int, repos_processed: int, repos_updated: int,
                         api_calls_made: int, rate_limit_hits: int,
                         status: str = "completed") -> None:
        await asyncio.to_thread(
            self._finish_run_sync, run_id, repos_processed, repos_updated,
            api_calls_made, rate_limit_hits, status,
        )

    def _get_last_run_sync(self, mode: str | None) -> IngestionRun | None:
        conn = self._connect()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                if mode:
                    cur.execute(
                        "SELECT * FROM ingestion_runs WHERE mode=%s "
                        "ORDER BY id DESC LIMIT 1",
                        (mode,),
                    )
                else:
                    cur.execute(
                        "SELECT * FROM ingestion_runs ORDER BY id DESC LIMIT 1"
                    )
                row = cur.fetchone()
                return IngestionRun(**dict(row)) if row else None
        finally:
            conn.close()

    async def get_last_run(self, mode: str | None = None) -> IngestionRun | None:
        return await asyncio.to_thread(self._get_last_run_sync, mode)

    # ── api_call_log ─────────────────────────────────────────────────────────

    def _log_api_call_sync(self, endpoint: str, status_code: int,
                           rate_limit_remaining: int | None) -> None:
        now = datetime.now(timezone.utc).isoformat()
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO api_call_log "
                    "(timestamp, endpoint, status_code, rate_limit_remaining) "
                    "VALUES (%s, %s, %s, %s)",
                    (now, endpoint, status_code, rate_limit_remaining),
                )
        finally:
            conn.close()

    async def log_api_call(self, endpoint: str, status_code: int,
                           rate_limit_remaining: int | None) -> None:
        await asyncio.to_thread(
            self._log_api_call_sync, endpoint, status_code, rate_limit_remaining
        )

    def _get_cache_stats_sync(self) -> dict[str, Any]:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM repo_cache")
                total = cur.fetchone()[0]
                cur.execute(
                    "SELECT COUNT(*) FROM repo_cache "
                    "WHERE permanent_fetched_at IS NOT NULL"
                )
                permanent = cur.fetchone()[0]
                cur.execute(
                    "SELECT COUNT(*) FROM repo_cache "
                    "WHERE daily_fetched_at IS NOT NULL"
                )
                daily = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM ingestion_runs")
                runs = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM api_call_log")
                calls = cur.fetchone()[0]
            return {
                "total_repos": total,
                "permanent_cached": permanent,
                "daily_cached": daily,
                "total_runs": runs,
                "total_api_calls_logged": calls,
            }
        finally:
            conn.close()

    async def get_cache_stats(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._get_cache_stats_sync)

    def _clean_stale_sync(self, days: int) -> int:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM repo_cache "
                    "WHERE daily_fetched_at < %s AND daily_fetched_at IS NOT NULL",
                    (cutoff,),
                )
                return cur.rowcount
        finally:
            conn.close()

    async def clean_stale(self, days: int = 90) -> int:
        return await asyncio.to_thread(self._clean_stale_sync, days)
