"""
Backfill fork date fields for all fork repos in the DB.

Fetches three date fields from the GitHub API that were never captured:
  - forked_at          : when perditioinc forked the repo (fork's own created_at)
  - your_last_push_at  : perditioinc's last push to the fork (fork's pushed_at)
  - upstream_created_at: when the original upstream repo was first created

Uses a single GraphQL batch query — roughly 1 API call per 50 repos, staying
well within GitHub's 5,000 req/hr authenticated rate limit.

Usage:
    GH_TOKEN=ghp_... DATABASE_URL=postgresql://... python scripts/backfill_fork_dates.py [--dry-run]

Cost: $0. No AI calls.
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone

import httpx
import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

GITHUB_GRAPHQL = "https://api.github.com/graphql"
BATCH_SIZE = 50  # repos per GraphQL query


def get_db_url() -> str:
    url = os.getenv("DATABASE_URL", "").strip()
    if url:
        return url
    try:
        from google.cloud import secretmanager
        client = secretmanager.SecretManagerServiceClient()
        project = os.getenv("GCP_PROJECT", "perditio-platform")
        name = f"projects/{project}/secrets/reporium-db-url/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8").strip()
    except Exception:
        pass
    raise RuntimeError("No DATABASE_URL found. Set DATABASE_URL env var.")


def get_gh_token() -> str:
    token = os.getenv("GH_TOKEN", "").strip()
    if not token:
        try:
            from google.cloud import secretmanager
            client = secretmanager.SecretManagerServiceClient()
            project = os.getenv("GCP_PROJECT", "perditio-platform")
            name = f"projects/{project}/secrets/gh-token/versions/latest"
            response = client.access_secret_version(request={"name": name})
            token = response.payload.data.decode("UTF-8").strip()
        except Exception:
            pass
    if not token:
        raise RuntimeError("No GH_TOKEN found. Set GH_TOKEN env var.")
    return token


def fetch_fork_dates_batch(names: list[str], owner: str, token: str) -> dict[str, dict]:
    """
    Fetch forked_at, your_last_push_at, upstream_created_at for a batch of repo names.
    Returns dict: repo_name -> {forked_at, your_last_push_at, upstream_created_at}
    """
    # Build aliases for each repo in a single GraphQL query
    aliases = []
    for i, name in enumerate(names):
        aliases.append(f"""
  r{i}: repository(owner: "{owner}", name: "{name}") {{
    createdAt
    pushedAt
    parent {{
      createdAt
    }}
  }}""")

    query = "query {" + "".join(aliases) + "\n}"

    headers = {"Authorization": f"bearer {token}", "Content-Type": "application/json"}
    resp = httpx.post(GITHUB_GRAPHQL, json={"query": query}, headers=headers, timeout=30)
    resp.raise_for_status()
    body = resp.json()

    if "errors" in body:
        logger.warning(f"GraphQL errors: {body['errors']}")

    data = body.get("data", {})
    results = {}
    for i, name in enumerate(names):
        repo_data = data.get(f"r{i}")
        if not repo_data:
            logger.warning(f"No data returned for {owner}/{name}")
            continue
        parent = repo_data.get("parent") or {}
        results[name] = {
            "forked_at": repo_data.get("createdAt"),          # When user forked it
            "your_last_push_at": repo_data.get("pushedAt"),    # User's last push
            "upstream_created_at": parent.get("createdAt"),    # Original repo created date
        }
    return results


def parse_iso(dt_str: str | None) -> datetime | None:
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except ValueError:
        return None


def main():
    parser = argparse.ArgumentParser(description="Backfill fork dates from GitHub API")
    parser.add_argument("--dry-run", action="store_true", help="Fetch data but don't write to DB")
    parser.add_argument("--owner", default="perditioinc", help="GitHub owner (default: perditioinc)")
    parser.add_argument("--limit", type=int, default=0, help="Max repos to process (0 = all)")
    args = parser.parse_args()

    token = get_gh_token()
    db_url = get_db_url()

    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    # Fetch all forks that are missing date fields
    cur.execute("""
        SELECT name
        FROM repos
        WHERE is_fork = true
          AND (forked_at IS NULL OR upstream_created_at IS NULL OR your_last_push_at IS NULL)
        ORDER BY name;
    """)
    rows = cur.fetchall()
    repo_names = [r[0] for r in rows]

    if args.limit:
        repo_names = repo_names[:args.limit]

    logger.info(f"Found {len(repo_names)} forks needing date backfill")
    if not repo_names:
        logger.info("Nothing to backfill — all forks already have date fields populated.")
        return

    stats = {"updated": 0, "skipped": 0, "errors": 0, "no_parent": 0}
    batches = [repo_names[i:i + BATCH_SIZE] for i in range(0, len(repo_names), BATCH_SIZE)]

    for batch_num, batch in enumerate(batches, 1):
        logger.info(f"Batch {batch_num}/{len(batches)}: fetching {len(batch)} repos from GitHub...")
        try:
            dates = fetch_fork_dates_batch(batch, args.owner, token)
        except Exception as e:
            logger.error(f"Batch {batch_num} failed: {e}")
            stats["errors"] += len(batch)
            time.sleep(5)
            continue

        for name, d in dates.items():
            forked_at = parse_iso(d.get("forked_at"))
            your_last_push = parse_iso(d.get("your_last_push_at"))
            upstream_created = parse_iso(d.get("upstream_created_at"))

            if upstream_created is None:
                stats["no_parent"] += 1
                logger.debug(f"{name}: no parent repo (may have been deleted upstream)")

            if args.dry_run:
                logger.info(
                    f"[DRY RUN] {name}: forked_at={forked_at}, "
                    f"your_last_push_at={your_last_push}, upstream_created_at={upstream_created}"
                )
                stats["updated"] += 1
                continue

            cur.execute("""
                UPDATE repos
                SET forked_at = %s,
                    your_last_push_at = %s,
                    upstream_created_at = COALESCE(%s, upstream_created_at),
                    updated_at = NOW()
                WHERE name = %s AND is_fork = true;
            """, (forked_at, your_last_push, upstream_created, name))
            stats["updated"] += 1

        if not args.dry_run:
            conn.commit()

        # Respect GitHub rate limits — 50 repos/query is fast but be polite
        if batch_num < len(batches):
            time.sleep(1)

    cur.close()
    conn.close()

    logger.info(
        f"Done. Updated: {stats['updated']}, No parent: {stats['no_parent']}, "
        f"Errors: {stats['errors']}"
    )
    if stats["updated"] > 0 and not args.dry_run:
        logger.info("Restart reporium-api or wait 5min for cache to expire to see changes.")


if __name__ == "__main__":
    main()
