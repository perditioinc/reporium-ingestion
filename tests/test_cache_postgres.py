"""Tests for the PostgreSQL-backed cache (KAN-230 durable fix).

The headline regression test is ``test_persists_across_instances``: it proves
the cache survives when the process/container is replaced — the exact failure
mode that made the nightly Cloud Run enrichment job redo the full backlog
every run and time out. SQLite-on-ephemeral-disk fails this; Postgres passes.

Postgres tests use the ``db_url`` fixture and run against the CI Postgres
service container. They skip locally when DATABASE_URL is unset (same pattern
as the rest of this suite). The factory test needs no DB.
"""

from datetime import datetime, timezone, timedelta

import psycopg2
import pytest

from ingestion.cache import (
    CacheDatabase,
    PostgresCacheDatabase,
    build_cache_database,
)
from ingestion.cache.models import RepoCacheRow


# ── factory selection (no DB) ────────────────────────────────────────────────


@pytest.mark.no_db
class _StubSettings:
    def __init__(self, database_url: str, cache_db_path: str = "./data/cache.db"):
        self.database_url = database_url
        self.cache_db_path = cache_db_path


@pytest.mark.no_db
def test_factory_picks_postgres_for_pg_url():
    db = build_cache_database(_StubSettings("postgresql+asyncpg://u:p@h/db"))
    assert isinstance(db, PostgresCacheDatabase)
    # async driver suffix stripped for psycopg2
    assert "+asyncpg" not in db.db_url
    assert db.db_url.startswith("postgresql://")


@pytest.mark.no_db
def test_factory_falls_back_to_sqlite_without_pg_url():
    assert isinstance(build_cache_database(_StubSettings("")), CacheDatabase)
    assert isinstance(
        build_cache_database(_StubSettings("sqlite:///x.db")), CacheDatabase
    )


# ── Postgres integration (CI Postgres service container) ─────────────────────


@pytest.fixture
async def pg_cache(db_url):
    """A clean PostgresCacheDatabase backed by the test Postgres."""
    db = PostgresCacheDatabase(db_url)
    await db.init()
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("TRUNCATE repo_cache, ingestion_runs, api_call_log")
    conn.close()
    return db


@pytest.mark.asyncio
async def test_init_is_idempotent_and_starts_empty(pg_cache):
    await pg_cache.init()  # second call must not raise
    stats = await pg_cache.get_cache_stats()
    assert stats["total_repos"] == 0


@pytest.mark.asyncio
async def test_upsert_and_retrieve(pg_cache):
    row = RepoCacheRow(
        name="test-repo",
        github_updated_at="2024-06-01T00:00:00Z",
        readme_content="# Test README",
        original_owner="testuser",
    )
    await pg_cache.upsert_repo(row)
    got = await pg_cache.get_repo("test-repo")
    assert got is not None
    assert got.name == "test-repo"
    assert got.readme_content == "# Test README"
    assert got.original_owner == "testuser"


@pytest.mark.asyncio
async def test_upsert_is_idempotent_on_conflict(pg_cache):
    await pg_cache.upsert_repo(RepoCacheRow(name="r", readme_content="v1"))
    await pg_cache.upsert_repo(RepoCacheRow(name="r", readme_content="v2"))
    got = await pg_cache.get_repo("r")
    assert got.readme_content == "v2"
    assert (await pg_cache.get_cache_stats())["total_repos"] == 1


@pytest.mark.asyncio
async def test_parent_archived_boolean_roundtrips(pg_cache):
    await pg_cache.upsert_repo(RepoCacheRow(name="a", parent_archived=True))
    await pg_cache.upsert_repo(RepoCacheRow(name="b", parent_archived=False))
    assert (await pg_cache.get_repo("a")).parent_archived is True
    assert (await pg_cache.get_repo("b")).parent_archived is False


@pytest.mark.asyncio
async def test_needs_daily_fetch_logic(pg_cache):
    now = datetime.now(timezone.utc).isoformat()
    await pg_cache.upsert_repo(
        RepoCacheRow(
            name="r", github_updated_at="2024-01-15T10:00:00Z", daily_fetched_at=now
        )
    )
    assert await pg_cache.needs_daily_fetch("r", "2024-01-15T10:00:00Z") is False
    assert await pg_cache.needs_daily_fetch("r", "2024-02-01T10:00:00Z") is True
    assert await pg_cache.needs_daily_fetch("unknown", "x") is True


@pytest.mark.asyncio
async def test_needs_permanent_and_weekly(pg_cache):
    await pg_cache.upsert_repo(
        RepoCacheRow(
            name="perm", permanent_fetched_at=datetime.now(timezone.utc).isoformat()
        )
    )
    assert await pg_cache.needs_permanent_fetch("perm") is False

    old = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
    recent = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
    await pg_cache.upsert_repo(RepoCacheRow(name="stale", weekly_fetched_at=old))
    await pg_cache.upsert_repo(RepoCacheRow(name="fresh", weekly_fetched_at=recent))
    assert await pg_cache.needs_weekly_fetch("stale") is True
    assert await pg_cache.needs_weekly_fetch("fresh") is False


@pytest.mark.asyncio
async def test_run_tracking(pg_cache):
    run_id = await pg_cache.start_run("quick")
    assert run_id > 0
    await pg_cache.finish_run(
        run_id, repos_processed=100, repos_updated=10,
        api_calls_made=50, rate_limit_hits=0,
    )
    last = await pg_cache.get_last_run("quick")
    assert last is not None
    assert last.repos_processed == 100
    assert last.status == "completed"


@pytest.mark.asyncio
async def test_persists_across_instances(db_url):
    """KAN-230 regression: a brand-new instance (≈ a new Cloud Run container
    on the next scheduled execution) must still see prior cached state.

    With the old SQLite-on-ephemeral-disk cache this is exactly what failed:
    every run started cold, so needs_*_fetch always returned True and the job
    re-enriched ~1900 repos and timed out. Persistence here is the fix.
    """
    writer = PostgresCacheDatabase(db_url)
    await writer.init()
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("TRUNCATE repo_cache, ingestion_runs, api_call_log")
    conn.close()

    await writer.upsert_repo(
        RepoCacheRow(
            name="persisted-repo",
            github_updated_at="2024-09-01T00:00:00Z",
            daily_fetched_at=datetime.now(timezone.utc).isoformat(),
        )
    )
    del writer

    # Fresh object, fresh connections — simulates the next job execution.
    reader = PostgresCacheDatabase(db_url)
    got = await reader.get_repo("persisted-repo")
    assert got is not None, "cache did not survive a new instance — KAN-230 not fixed"
    # The dedup decision the nightly job actually makes:
    assert await reader.needs_daily_fetch("persisted-repo", "2024-09-01T00:00:00Z") is False
