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
    """One-time schema setup.

    In CI, Alembic migrations run before pytest (see test.yml), so all tables
    already exist when this fixture runs and the CREATE TABLE IF NOT EXISTS
    statements below are no-ops.

    For local dev without migrations these statements create a compatible
    minimal schema so integration tests can still run.
    """
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    cur = conn.cursor()

    # pgvector extension (needed by alembic migrations; safe to re-run)
    cur.execute("CREATE EXTENSION IF NOT EXISTS vector")

    # repos (minimal columns needed by graph builder — full schema from migrations)
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

    # repo_edges (migration 033) — metadata column, no updated_at
    cur.execute("""
        CREATE TABLE IF NOT EXISTS repo_edges (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            source_repo_id UUID NOT NULL REFERENCES repos(id),
            target_repo_id UUID NOT NULL REFERENCES repos(id),
            edge_type TEXT NOT NULL,
            weight FLOAT DEFAULT 1.0,
            confidence FLOAT DEFAULT 0.5,
            metadata JSONB DEFAULT '{}',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (source_repo_id, target_repo_id, edge_type)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_repo_edges_source ON repo_edges(source_repo_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_repo_edges_target ON repo_edges(target_repo_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_repo_edges_type ON repo_edges(edge_type)")

    # repo_edges_history (migration 033) — count-log schema
    cur.execute("""
        CREATE TABLE IF NOT EXISTS repo_edges_history (
            id SERIAL PRIMARY KEY,
            run_id INTEGER,
            edge_type TEXT NOT NULL,
            edge_count INTEGER NOT NULL DEFAULT 0,
            sample_edges JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    cur.close()
    conn.close()

    yield db_url

    # Teardown: drop all test tables
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    cur = conn.cursor()
    for table in [
        "repo_edges_history", "repo_edges", "repo_dependencies",
        "repo_categories", "repos",
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
    cur.execute("TRUNCATE repos, repo_categories, repo_dependencies, repo_edges, repo_edges_history CASCADE")
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
        """INSERT INTO repos (id, name, owner, forked_from, primary_category,
                              problem_solved, integration_tags, github_url)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            repo_id, name, owner,
            defaults["forked_from"],
            defaults["primary_category"],
            defaults["problem_solved"],
            json.dumps(defaults["integration_tags"]) if defaults["integration_tags"] else None,
            f"https://github.com/{owner}/{name}",
        ),
    )
    return repo_id


# Needed for make_repo helper
import json
