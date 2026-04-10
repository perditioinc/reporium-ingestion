"""
Test configuration for reporium-ingestion.

Uses a real PostgreSQL + pgvector database in CI via GitHub Actions service
container.  Falls back to skipping DB-dependent tests when no DATABASE_URL
is set (local dev without postgres).
"""

import os
import uuid

import psycopg2
import pytest


def _db_url() -> str | None:
    """Return DATABASE_URL if set and usable, else None."""
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        return None
    # Quick connectivity check
    try:
        conn = psycopg2.connect(url)
        conn.close()
        return url
    except Exception:
        return None


@pytest.fixture(scope="session")
def db_url():
    """Session-scoped DB URL.  Skips entire test if unavailable."""
    url = _db_url()
    if url is None:
        pytest.skip("DATABASE_URL not set or unreachable")
    return url


@pytest.fixture(scope="session")
def db_setup(db_url):
    """One-time schema setup: create all tables needed by the graph builder.

    Uses raw SQL matching the Alembic migrations so tests run against the
    real schema — not an ORM approximation.
    """
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    cur = conn.cursor()

    # repos (minimal columns needed by graph builder)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS repos (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name TEXT NOT NULL,
            owner TEXT NOT NULL DEFAULT '',
            description TEXT,
            is_fork BOOLEAN NOT NULL DEFAULT false,
            is_private BOOLEAN NOT NULL DEFAULT false,
            forked_from TEXT,
            primary_language TEXT,
            github_url TEXT NOT NULL DEFAULT '',
            primary_category TEXT,
            problem_solved TEXT,
            integration_tags JSONB,
            open_issues_count INTEGER NOT NULL DEFAULT 0,
            stargazers_count INTEGER,
            parent_stars INTEGER,
            parent_is_archived BOOLEAN NOT NULL DEFAULT false,
            activity_score INTEGER NOT NULL DEFAULT 0,
            ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    # repo_categories junction
    cur.execute("""
        CREATE TABLE IF NOT EXISTS repo_categories (
            repo_id UUID NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
            category_id TEXT NOT NULL,
            category_name TEXT NOT NULL,
            is_primary BOOLEAN NOT NULL DEFAULT false,
            PRIMARY KEY (repo_id, category_id)
        )
    """)

    # repo_dependencies (migration 029)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS repo_dependencies (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            repo_id UUID NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
            package_name TEXT NOT NULL,
            package_ecosystem TEXT,
            version_constraint TEXT,
            is_direct BOOLEAN NOT NULL DEFAULT true,
            fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (repo_id, package_name, package_ecosystem)
        )
    """)

    # repo_edges (migration 031 + 034: ingest_run_id column added in Wave 3)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS repo_edges (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            source_repo_id UUID NOT NULL REFERENCES repos(id),
            target_repo_id UUID NOT NULL REFERENCES repos(id),
            edge_type TEXT NOT NULL,
            weight FLOAT DEFAULT 1.0,
            confidence FLOAT DEFAULT 0.5,
            metadata JSONB DEFAULT '{}',
            ingest_run_id INTEGER,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (source_repo_id, target_repo_id, edge_type)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_repo_edges_source ON repo_edges(source_repo_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_repo_edges_target ON repo_edges(target_repo_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_repo_edges_type ON repo_edges(edge_type)")

    # ingest_runs (migration 017 + 032 extensions)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ingest_runs (
            id SERIAL PRIMARY KEY,
            run_mode TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'running',
            repos_upserted INTEGER NOT NULL DEFAULT 0,
            repos_processed INTEGER NOT NULL DEFAULT 0,
            errors JSONB,
            started_at TIMESTAMPTZ DEFAULT NOW(),
            finished_at TIMESTAMPTZ,
            checkpoint_data JSONB,
            prev_edge_counts JSONB,
            git_sha TEXT,
            triggered_by TEXT
        )
    """)

    # repo_edges_history (migration 033 — full edge archive)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS repo_edges_history (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            source_repo_id UUID NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
            target_repo_id UUID NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
            edge_type VARCHAR(32) NOT NULL,
            weight FLOAT NOT NULL DEFAULT 1.0,
            confidence FLOAT NOT NULL DEFAULT 0.5,
            metadata JSONB DEFAULT '{}',
            ingest_run_id INTEGER,
            valid_from TIMESTAMPTZ DEFAULT NOW(),
            valid_until TIMESTAMPTZ,
            run_id INTEGER,
            edge_count INTEGER NOT NULL DEFAULT 0,
            sample_edges JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    # repo_embeddings (migrations 001 + 034)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS repo_embeddings (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            repo_id UUID NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
            embedding TEXT,
            model TEXT NOT NULL DEFAULT 'all-MiniLM-L6-v2',
            generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            is_current BOOLEAN NOT NULL DEFAULT true,
            ingest_run_id INTEGER
        )
    """)
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_repo_embeddings_current
        ON repo_embeddings(repo_id) WHERE is_current = true
    """)

    # Migration 034 backfill: ensure ingest_run_id exists on repo_edges even if
    # the Alembic migration in reporium-api hasn't been promoted yet.  This is
    # idempotent — ALTER TABLE ... ADD COLUMN IF NOT EXISTS is a no-op when the
    # column already exists.
    cur.execute("""
        ALTER TABLE repo_edges
        ADD COLUMN IF NOT EXISTS ingest_run_id INTEGER
    """)

    cur.close()
    conn.close()

    yield db_url

    # Teardown: drop all test tables
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    cur = conn.cursor()
    for table in [
        "repo_embeddings", "repo_edges_history", "repo_edges",
        "ingest_runs", "repo_dependencies", "repo_categories", "repos",
    ]:
        cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
    cur.close()
    conn.close()


@pytest.fixture(autouse=True)
def clean_tables(db_setup):
    """Truncate all data between tests for isolation."""
    conn = psycopg2.connect(db_setup)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("TRUNCATE repos, repo_categories, repo_dependencies, repo_edges, repo_edges_history, ingest_runs, repo_embeddings CASCADE")
    cur.close()
    conn.close()


@pytest.fixture
def db_conn(db_setup):
    """Provide a psycopg2 connection + cursor for tests."""
    conn = psycopg2.connect(db_setup)
    yield conn
    conn.rollback()
    conn.close()


def make_repo(cur, name="test-repo", owner="testowner", **kwargs):
    """Helper: insert a repo and return its UUID."""
    repo_id = str(uuid.uuid4())
    defaults = {
        "forked_from": None,
        "primary_category": None,
        "problem_solved": None,
        "integration_tags": None,
    }
    defaults.update(kwargs)
    cur.execute(
        """INSERT INTO repos (id, name, owner, forked_from,
                              problem_solved, integration_tags, github_url)
           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        (
            repo_id, name, owner,
            defaults["forked_from"],
            defaults["problem_solved"],
            json.dumps(defaults["integration_tags"]) if defaults["integration_tags"] else None,
            f"https://github.com/{owner}/{name}",
        ),
    )
    return repo_id


# Needed for make_repo helper
import json
