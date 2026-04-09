"""
pytest fixtures for tests that require a real PostgreSQL database.

Schema is provided by running `alembic upgrade head` from the reporium-api
repository once per test session. Each test runs inside a transaction that is
rolled back after the test completes, so no test data persists.

Required environment variables (set automatically in CI via test.yml):
  TEST_DATABASE_URL  — PostgreSQL DSN for the test database
  REPORIUM_API_PATH  — Path to the reporium-api checkout (contains alembic.ini)

Local usage:
  1. Start a local PostgreSQL instance (or use Docker):
       docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres pgvector/pgvector:pg16
  2. Clone reporium-api alongside reporium-ingestion (or set REPORIUM_API_PATH).
  3. Run:
       TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres \\
       REPORIUM_API_PATH=../../reporium-api \\
       pytest tests/test_knowledge_graph.py -v
"""

from __future__ import annotations

import os
import subprocess
import sys

import pytest

TEST_DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/postgres",
)


def _find_reporium_api_path() -> str | None:
    """Locate reporium-api directory from env var or common relative paths."""
    explicit = os.getenv("REPORIUM_API_PATH", "").strip()
    if explicit and os.path.isfile(os.path.join(explicit, "alembic.ini")):
        return explicit

    # Relative paths tried in order: CI checkout location, local sibling
    here = os.path.dirname(__file__)
    candidates = [
        os.path.join(here, "..", "reporium-api"),       # CI: checked out alongside
        os.path.join(here, "..", "..", "reporium-api"),  # local sibling repo
    ]
    for path in candidates:
        path = os.path.normpath(path)
        if os.path.isfile(os.path.join(path, "alembic.ini")):
            return path

    return None


@pytest.fixture(scope="session")
def db_schema():
    """
    Run `alembic upgrade head` once per test session against TEST_DATABASE_URL.

    Uses the migration history in reporium-api — the canonical schema source.
    Skips if reporium-api is not available (see REPORIUM_API_PATH above).
    """
    import psycopg2  # noqa: PLC0415 — imported here so non-DB tests don't need it

    api_path = _find_reporium_api_path()
    if api_path is None:
        pytest.skip(
            "reporium-api not found. Set REPORIUM_API_PATH or clone it alongside "
            "reporium-ingestion. Skipping DB tests."
        )

    env = {**os.environ, "DATABASE_URL": TEST_DATABASE_URL}

    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=api_path,
        env=env,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.fail(
            f"alembic upgrade head failed (cwd={api_path}):\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )

    yield

    # Do not downgrade — leave schema in place for fast re-runs.


@pytest.fixture()
def db_conn(db_schema):
    """
    Yield a psycopg2 connection in an open transaction.

    The transaction is rolled back after the test, so inserts made during the
    test leave no persistent state. The connection uses autocommit=False
    (psycopg2 default) so all DML is in the same transaction unless the test
    explicitly commits.

    Note: functions under test (build_depends_on, insert_edges, etc.) accept a
    cursor and do not commit — commits are the caller's responsibility. This
    allows clean rollback-based isolation.
    """
    import psycopg2  # noqa: PLC0415

    conn = psycopg2.connect(TEST_DATABASE_URL)
    conn.autocommit = False
    try:
        yield conn
    finally:
        conn.rollback()
        conn.close()
