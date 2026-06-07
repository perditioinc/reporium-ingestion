"""
Local stub of reporium-api for the $0 ingestion substrate.

Implements the ingest contract that reporium-ingestion writes to and persists
repos / trends / gaps / runs into the local Postgres. It is deliberately
permissive (no auth enforcement) because it exists only to exercise the
ingestion path locally — it is never exposed to the internet and never used in
production.

Endpoints (the subset the pipeline calls):
  GET  /health
  POST /ingest/repos                 -> upsert repos into the `repos` table
  POST /ingest/trends/snapshot       -> store a trend snapshot
  POST /ingest/gaps                  -> store gap analysis
  POST /ingest/log                   -> accept run log (no-op store)
  POST /admin/runs                   -> record a run in ingest_runs
  GET  /repos/count                  -> convenience for the smoke script
"""
import json
import os
from typing import Any

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, Request

DATABASE_URL = os.environ["DATABASE_URL"]

app = FastAPI(title="reporium-api (local stub)")


def _conn():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    return conn


# Columns on the local `repos` table the stub is willing to write. Anything the
# pipeline sends that is not here (junction-table data: tags, categories,
# builders, languages, commits, taxonomy lists) is accepted and ignored — in
# prod those route to separate tables; the local substrate only needs the
# scalar repo row so the embeddings + graph paths have data to work with.
_REPO_COLUMNS = {
    "name", "owner", "description", "is_fork", "is_private",
    "forked_from", "primary_language", "github_url", "open_issues_count",
    "forks_count", "commits_last_7_days", "commits_last_30_days",
    "commits_last_90_days", "activity_score", "activity_score_breakdown",
    "readme_summary", "problem_solved", "maturity_level", "quality_assessment",
    "integration_tags", "dependencies", "license_spdx",
    "github_created_at", "github_updated_at", "your_last_push_at",
    "upstream_last_push_at", "forked_at",
}
_JSON_COLUMNS = {
    "activity_score_breakdown", "integration_tags", "dependencies",
}


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/repos/count")
def repos_count() -> dict[str, int]:
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM repos")
            return {"count": cur.fetchone()[0]}
    finally:
        conn.close()


def _upsert_one(cur, repo: dict[str, Any]) -> None:
    cols = [c for c in repo.keys() if c in _REPO_COLUMNS]
    if "name" not in cols:
        return
    values = []
    for c in cols:
        v = repo[c]
        if c in _JSON_COLUMNS and v is not None and not isinstance(v, str):
            v = json.dumps(v)
        values.append(v)

    col_list = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    # skip-empty / null-preserve guard: never overwrite a stored value with NULL
    updates = ", ".join(
        f"{c} = COALESCE(EXCLUDED.{c}, repos.{c})" for c in cols if c != "name"
    )
    sql = (
        f"INSERT INTO repos ({col_list}) VALUES ({placeholders}) "
        f"ON CONFLICT (name) DO UPDATE SET {updates}, updated_at = now()"
    )
    cur.execute(sql, values)


@app.post("/ingest/repos")
async def ingest_repos(request: Request) -> dict[str, Any]:
    repos = await request.json()
    if isinstance(repos, dict):
        repos = [repos]
    upserted = 0
    errors: list[str] = []
    conn = _conn()
    try:
        with conn.cursor() as cur:
            for repo in repos:
                try:
                    _upsert_one(cur, repo)
                    upserted += 1
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"{repo.get('name')}: {exc}")
    finally:
        conn.close()
    return {"upserted": upserted, "errors": errors}


@app.post("/ingest/trends/snapshot")
async def ingest_trend_snapshot(request: Request) -> dict[str, str]:
    snapshot = await request.json()
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO trend_snapshots (captured_at, payload) VALUES (%s, %s)",
                (snapshot.get("captured_at"), json.dumps(snapshot)),
            )
    finally:
        conn.close()
    return {"status": "ok"}


@app.post("/ingest/gaps")
async def ingest_gaps(request: Request) -> dict[str, str]:
    gaps = await request.json()
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO gaps (payload) VALUES (%s)", (json.dumps(gaps),)
            )
    finally:
        conn.close()
    return {"status": "ok"}


@app.post("/ingest/log")
async def ingest_log(request: Request) -> dict[str, str]:
    await request.json()  # accept and discard — run history not modeled locally
    return {"status": "ok"}


@app.post("/admin/runs")
async def admin_runs(request: Request) -> dict[str, str]:
    run = await request.json()
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO ingest_runs (run_mode, status, repos_upserted, "
                "repos_processed, errors, finished_at) "
                "VALUES (%s, %s, %s, %s, %s, now())",
                (
                    run.get("run_mode"),
                    run.get("status", "success"),
                    run.get("repos_upserted", 0),
                    run.get("repos_processed", 0),
                    json.dumps(run.get("errors", [])),
                ),
            )
    finally:
        conn.close()
    return {"status": "ok"}
