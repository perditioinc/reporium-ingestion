"""
Backfill forked_from for repos where is_fork = true but forked_from IS NULL.

Queries GitHub API for each affected repo to get parent.full_name, then
updates the DB. Run whenever the ingestion pipeline leaves forked_from blank
(happens when gh repo list returns null parent for some fork repos).

Usage:
    python scripts/backfill_forked_from.py           # update DB
    python scripts/backfill_forked_from.py --dry-run  # show what would change
"""

import json
import logging
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DRY_RUN = "--dry-run" in sys.argv
GCP_PROJECT = os.getenv("GCP_PROJECT", "perditio-platform")


def get_db_url() -> str:
    url = os.getenv("DATABASE_URL", "").strip()
    if url:
        return url.replace("+asyncpg", "").replace("+psycopg2", "")
    try:
        from google.cloud import secretmanager
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{GCP_PROJECT}/secrets/reporium-db-url-async/versions/latest"
        response = client.access_secret_version(request={"name": name})
        raw = response.payload.data.decode("UTF-8").strip()
        return raw.replace("+asyncpg", "").replace("+psycopg2", "")
    except Exception as e:
        raise RuntimeError(f"No DATABASE_URL: {e}")


def get_gh_token() -> str:
    result = subprocess.run(["gh", "auth", "token"], capture_output=True, text=True)
    if result.returncode == 0:
        return result.stdout.strip()
    return os.getenv("GH_TOKEN", os.getenv("GITHUB_TOKEN", ""))


def fetch_github_repo(token: str, owner: str, repo: str) -> dict | None:
    """GET /repos/{owner}/{repo} — returns full repo object or None."""
    url = f"https://api.github.com/repos/{owner}/{repo}"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        if e.code == 404:
            logger.warning(f"  {owner}/{repo}: 404 not found")
        else:
            logger.warning(f"  {owner}/{repo}: HTTP {e.code}")
        return None
    except Exception as e:
        logger.warning(f"  {owner}/{repo}: {e}")
        return None


def main():
    logger.info("=" * 60)
    logger.info("Backfill forked_from for is_fork=true repos with NULL forked_from")
    logger.info(f"Dry run: {DRY_RUN}")
    logger.info("=" * 60)

    db_url = get_db_url()
    token = get_gh_token()
    if not token:
        raise RuntimeError("No GitHub token — run: gh auth login")

    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    # Find all fork repos missing forked_from
    cur.execute("""
        SELECT id, owner, name
        FROM repos
        WHERE is_fork = true AND (forked_from IS NULL OR forked_from = '')
        ORDER BY ingested_at DESC;
    """)
    rows = cur.fetchall()

    if not rows:
        logger.info("No repos need backfilling — all fork repos have forked_from set.")
        conn.close()
        return

    logger.info(f"Found {len(rows)} fork repos missing forked_from:")
    for _, owner, name in rows:
        logger.info(f"  - {owner}/{name}")

    if DRY_RUN:
        logger.info("Dry run — no changes made.")
        conn.close()
        return

    updated = 0
    not_found = 0
    not_fork = 0

    for repo_id, owner, name in rows:
        data = fetch_github_repo(token, owner, name)
        time.sleep(0.5)  # be gentle with the API

        if data is None:
            not_found += 1
            continue

        if not data.get("fork"):
            # gh says not a fork — fix is_fork too
            logger.info(f"  {owner}/{name}: not actually a fork — setting is_fork=false")
            cur.execute(
                "UPDATE repos SET is_fork = false, updated_at = NOW() WHERE id = %s;",
                (str(repo_id),),
            )
            conn.commit()
            not_fork += 1
            continue

        parent = data.get("parent") or {}
        forked_from = parent.get("full_name")
        if not forked_from:
            logger.warning(f"  {owner}/{name}: fork=true but parent.full_name missing")
            not_found += 1
            continue

        logger.info(f"  {owner}/{name}: forked_from = {forked_from}")
        cur.execute(
            "UPDATE repos SET forked_from = %s, updated_at = NOW() WHERE id = %s;",
            (forked_from, str(repo_id)),
        )
        conn.commit()
        updated += 1

    cur.execute("""
        SELECT COUNT(*) FROM repos
        WHERE is_fork = true AND (forked_from IS NULL OR forked_from = '');
    """)
    still_missing = cur.fetchone()[0]

    print()
    print("=" * 60)
    print("BACKFILL COMPLETE")
    print("=" * 60)
    print(f"  Targeted:      {len(rows)}")
    print(f"  Updated:       {updated}")
    print(f"  Not a fork:    {not_fork}")
    print(f"  Not found:     {not_found}")
    print(f"  Still missing: {still_missing}")
    print()

    conn.close()


if __name__ == "__main__":
    main()
