#!/usr/bin/env python3
"""
KAN-42 — Backfill stargazers_count for built (non-fork) repos.

Queries GitHub API for current star count of each non-fork repo and
updates the DB. Safe to re-run (idempotent).

Usage:
    DATABASE_URL=<psycopg2-url> python scripts/backfill_stargazers.py
    DATABASE_URL=... python scripts/backfill_stargazers.py --dry-run
"""

import argparse
import logging
import os
import subprocess
import sys

import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def get_db_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL not set.")
    url = url.replace("+asyncpg", "").replace("+psycopg2", "")
    url = url.replace("?ssl=require", "").replace("?sslmode=require", "")
    return url


def fetch_stars(owner: str, name: str) -> int | None:
    result = subprocess.run(
        ["gh", "api", f"repos/{owner}/{name}", "--jq", ".stargazers_count"],
        capture_output=True,
        text=True,
    )
    val = result.stdout.strip()
    return int(val) if val.isdigit() else None


def run(dry_run: bool = False) -> None:
    conn = psycopg2.connect(get_db_url())
    cur = conn.cursor()

    cur.execute(
        "SELECT name, owner, stargazers_count FROM repos WHERE is_fork = false ORDER BY name"
    )
    repos = cur.fetchall()
    logger.info("Found %d non-fork repos", len(repos))

    updated = 0
    for name, owner, current in repos:
        stars = fetch_stars(owner, name)
        if stars is None:
            logger.warning("%s/%s: GitHub API returned no star count — skipping", owner, name)
            continue
        if stars == current:
            logger.debug("%s/%s: already %d stars, no change", owner, name, stars)
            continue
        if dry_run:
            logger.info("[dry-run] %s/%s: would set stargazers_count = %d (was %s)", owner, name, stars, current)
        else:
            cur.execute(
                "UPDATE repos SET stargazers_count = %s WHERE name = %s AND owner = %s",
                (stars, name, owner),
            )
            conn.commit()
            logger.info("%s/%s: %s -> %d", owner, name, current, stars)
            updated += 1

    conn.close()
    logger.info("Done — %d repos updated", updated)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
