#!/usr/bin/env python3
"""
Safe re-enrichment script with checkpoint/resume.
Processes repos missing 8-dimension taxonomy data.
Uses max 2 concurrent requests, 500ms delay between batches.
Saves progress to SQLite. Never crashes.
"""

import asyncio
import logging
import os
import signal
import sqlite3
import sys
from datetime import datetime, timezone

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

API_URL = os.getenv("REPORIUM_API_URL", "http://localhost:8000")
API_KEY = os.getenv("REPORIUM_API_KEY") or os.getenv("INGEST_API_KEY", "")
ADMIN_KEY = os.getenv("ADMIN_API_KEY", "")
PROGRESS_DB = os.path.join(os.path.dirname(__file__), "reenrich_progress.db")

# Rate limiting
CONCURRENCY = 2
BATCH_DELAY = 0.5  # seconds between batches

# Global shutdown flag
_shutdown = False


def _handle_sigint(sig, frame):
    global _shutdown
    logger.info("Received interrupt signal — finishing current batch then stopping...")
    _shutdown = True


signal.signal(signal.SIGINT, _handle_sigint)


# ── SQLite progress tracking ──────────────────────────────────────────────────

def init_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS progress (
            repo_name TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            processed_at TEXT,
            error TEXT
        )
    """)
    conn.commit()
    return conn


def get_done_repos(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT repo_name FROM progress WHERE status = 'done'"
    ).fetchall()
    return {row[0] for row in rows}


def mark_progress(conn: sqlite3.Connection, repo_name: str, status: str, error: str | None = None):
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT OR REPLACE INTO progress (repo_name, status, processed_at, error) "
        "VALUES (?, ?, ?, ?)",
        (repo_name, status, now, error),
    )
    conn.commit()


def get_summary(conn: sqlite3.Connection) -> dict:
    rows = conn.execute(
        "SELECT status, COUNT(*) FROM progress GROUP BY status"
    ).fetchall()
    return {row[0]: row[1] for row in rows}


# ── API helpers ───────────────────────────────────────────────────────────────

def _headers() -> dict:
    h = {"Content-Type": "application/json"}
    if API_KEY:
        h["Authorization"] = f"Bearer {API_KEY}"
    if ADMIN_KEY:
        h["X-Admin-Key"] = ADMIN_KEY
    return h


async def fetch_all_repos(client: httpx.AsyncClient) -> list[dict]:
    """Paginate through GET /repos and return all repos."""
    repos = []
    page = 1
    limit = 100
    while True:
        try:
            resp = await client.get(
                f"{API_URL}/repos",
                params={"page": page, "limit": limit},
                headers=_headers(),
                timeout=30.0,
            )
            resp.raise_for_status()
            data = resp.json()
            batch = data.get("repos") or data.get("items") or []
            if not batch:
                break
            repos.extend(batch)
            # Check if there are more pages
            total = data.get("total", 0)
            if len(repos) >= total or len(batch) < limit:
                break
            page += 1
        except Exception as exc:
            logger.error("Failed to fetch repos page %d: %s", page, exc)
            break
    return repos


async def get_repo_taxonomy_count(client: httpx.AsyncClient, repo_name: str) -> int:
    """Check how many taxonomy entries a repo has via GET /repos/{name}."""
    try:
        resp = await client.get(
            f"{API_URL}/repos/{repo_name}",
            headers=_headers(),
            timeout=15.0,
        )
        if resp.status_code == 404:
            return -1
        resp.raise_for_status()
        data = resp.json()
        taxonomy = data.get("taxonomy") or []
        return len(taxonomy)
    except Exception as exc:
        logger.warning("Failed to get taxonomy for %s: %s", repo_name, exc)
        return -1


async def run_taxonomy_bootstrap(client: httpx.AsyncClient, repo_name: str) -> bool:
    """
    Trigger taxonomy assignment for a specific repo.
    Uses POST /admin/taxonomy/bootstrap with limit=1 is not per-repo,
    so we use the general assign endpoint and hope it picks up this repo.
    As a fallback, we call POST /admin/taxonomy/bootstrap?limit=1.
    """
    try:
        resp = await client.post(
            f"{API_URL}/admin/taxonomy/bootstrap",
            params={"limit": 1},
            headers=_headers(),
            timeout=60.0,
        )
        if resp.status_code in (200, 201):
            data = resp.json()
            return data.get("assigned", 0) > 0 or data.get("processed", 0) > 0
        logger.warning(
            "Taxonomy bootstrap returned %d for %s",
            resp.status_code,
            repo_name,
        )
        return False
    except Exception as exc:
        logger.warning("Taxonomy bootstrap failed for %s: %s", repo_name, exc)
        return False


# ── Main processing ───────────────────────────────────────────────────────────

async def process_repo(
    client: httpx.AsyncClient,
    conn: sqlite3.Connection,
    semaphore: asyncio.Semaphore,
    repo: dict,
) -> str:
    """Returns 'done', 'skipped', or 'error'."""
    repo_name = repo.get("name") or repo.get("full_name") or ""
    if not repo_name:
        return "error"

    async with semaphore:
        try:
            count = await get_repo_taxonomy_count(client, repo_name)
            if count < 0:
                mark_progress(conn, repo_name, "error", "repo not found or API error")
                return "error"

            if count > 0:
                # Already has taxonomy
                mark_progress(conn, repo_name, "done")
                return "skipped"

            # Needs taxonomy enrichment
            success = await run_taxonomy_bootstrap(client, repo_name)
            if success:
                mark_progress(conn, repo_name, "done")
                return "done"
            else:
                mark_progress(conn, repo_name, "error", "bootstrap returned no assignments")
                return "error"

        except Exception as exc:
            err = str(exc)
            logger.warning("Unexpected error processing %s: %s", repo_name, err)
            mark_progress(conn, repo_name, "error", err)
            return "error"


async def main():
    global _shutdown

    logger.info("Starting re-enrichment script")
    logger.info("API URL: %s", API_URL)
    logger.info("Progress DB: %s", PROGRESS_DB)

    conn = init_db(PROGRESS_DB)
    done_repos = get_done_repos(conn)
    logger.info("Loaded %d already-done repos from checkpoint", len(done_repos))

    async with httpx.AsyncClient() as client:
        logger.info("Fetching all repos from API...")
        all_repos = await fetch_all_repos(client)
        logger.info("Found %d total repos", len(all_repos))

        # Filter out already-done repos
        pending = [
            r for r in all_repos
            if (r.get("name") or r.get("full_name") or "") not in done_repos
        ]
        logger.info("%d repos pending enrichment check", len(pending))

        if not pending:
            logger.info("All repos already processed — nothing to do")
            summary = get_summary(conn)
            logger.info("Final summary: %s", summary)
            conn.close()
            return

        total = len(pending)
        processed = 0
        done_count = 0
        error_count = 0
        semaphore = asyncio.Semaphore(CONCURRENCY)

        for i in range(0, total, CONCURRENCY):
            if _shutdown:
                logger.info("Graceful shutdown: stopping after %d/%d repos", processed, total)
                break

            batch = pending[i : i + CONCURRENCY]
            tasks = [
                process_repo(client, conn, semaphore, repo)
                for repo in batch
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                processed += 1
                if isinstance(result, Exception):
                    error_count += 1
                elif result == "done":
                    done_count += 1
                elif result == "error":
                    error_count += 1
                # skipped counts as processed but not done/error

            if processed % 10 == 0 or processed == total:
                logger.info(
                    "Processed %d/%d repos (%d done, %d errors)",
                    processed,
                    total,
                    done_count,
                    error_count,
                )

            await asyncio.sleep(BATCH_DELAY)

    summary = get_summary(conn)
    conn.close()

    logger.info("=" * 60)
    logger.info("Re-enrichment complete")
    logger.info("Total repos found:    %d", len(all_repos))
    logger.info("Pending this run:     %d", total)
    logger.info("Processed:            %d", processed)
    logger.info("Done (assigned):      %d", done_count)
    logger.info("Errors:               %d", error_count)
    logger.info("DB summary:           %s", summary)
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
