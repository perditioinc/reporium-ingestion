"""
pytest fixtures for tests that require a real PostgreSQL database.

Creates the minimum schema via raw SQL (no Alembic dependency) once per
session. Each test runs inside a transaction that is rolled back on teardown,
so no test data persists between tests.

Set DATABASE_URL (or TEST_DATABASE_URL) to a PostgreSQL DSN before running.
In CI the pgvector/pgvector:pg16 service container is used.
"""

from __future__ import annotations

import os
import uuid

import psycopg2
import pytest


def _db_url() -> str | None:
    url = (
        os.environ.get("TEST_DATABASE_URL", "").strip()
        or os.environ.get("DATABASE_URL", "").strip()
    )
    if not url:
        return None
    try:
        conn = psycopg2.connect(url)
        conn.close()
        return url
    except Exception:
        return None


@pytest.fixture(scope="session")
def db_url():
    url = _db_url()
    if url is None:
        pytest.skip("DATABASE_URL / TEST_DATABASE_URL not set or unreachable")
    return url


@pytest.fixture(scope="session")
def db_setup(db_url):
    """Create all tables needed by the knowledge graph builder once per session."""
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    cur = conn.cursor()

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

    cur.execute("""
        CREATE TABLE IF NOT EXISTS repo_categories (
            repo_id UUID NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
            category_id TEXT NOT NULL,
            category_name TEXT NOT NULL,
            is_primary BOOLEAN NOT NULL DEFAULT false,
            PRIMARY KEY (repo_id, category_id)
        )
    """)

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
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    cur.close()
    conn.close()

    yield db_url

    # Teardown
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    cur = conn.cursor()
    for table in [
        "repo_edges_history", "repo_edges", "repo_dependencies",
        "repo_categories", "ingest_runs", "repos",
    ]:
        cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
    cur.close()
    conn.close()


@pytest.fixture(autouse=True)
def clean_tables(db_setup):
    """Truncate data between tests for isolation."""
    conn = psycopg2.connect(db_setup)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        "TRUNCATE repos, repo_categories, repo_dependencies, "
        "repo_edges, repo_edges_history, ingest_runs CASCADE"
    )
    cur.close()
    conn.close()


@pytest.fixture
def db_conn(db_setup):
    """Provide a psycopg2 connection + cursor for tests."""
    conn = psycopg2.connect(db_setup)
    yield conn
    conn.rollback()
    conn.close()
