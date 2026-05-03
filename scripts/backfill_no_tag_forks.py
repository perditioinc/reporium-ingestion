"""
Backfill tags + categories for forks that ended up with zero rows in repo_tags.

Root cause (see reporium#240 "P7: 184 no-tag repos"):
  These rows are forks whose GitHub `topics` array is empty because GitHub does
  not inherit upstream topics onto forks. Their stored `readme_summary` is a
  one-line string — too short for the keyword tagger. The deterministic tagger
  therefore produced nothing, and the nightly ingest leaves them untouched
  because `_upsert_repo` skips empty tag arrays.

Strategy (no AI spend):
  1. Find fork rows with zero repo_tags entries.
  2. For each, fetch UPSTREAM topics + UPSTREAM README via GitHub REST.
  3. Run the deterministic keyword tagger against upstream text.
  4. Derive primary / all categories from the resulting tags.
  5. Write repo_tags, repo_categories, and repo_taxonomy (tag + category dims)
     so the aggregated taxonomy surface picks them up on next rebuild.

Cost: $0. No Claude calls.

Usage:
    GH_TOKEN=ghp_... DATABASE_URL=postgresql://... \\
        python scripts/backfill_no_tag_forks.py [--dry-run] [--limit N]

Environment:
    GH_TOKEN         GitHub PAT with public repo read (or use `gh auth token`).
    DATABASE_URL     psycopg2-compatible Postgres URL.
    GCP_PROJECT      Secret Manager project (default: perditio-platform).

After the run, the operator should trigger the taxonomy rebuild so
`taxonomy_values` picks up the new tag + category dimensions:

    curl -X POST "$REPORIUM_API_URL/admin/taxonomy/rebuild" \\
        -H "X-Admin-Key: $INGEST_API_KEY"

Or wait for the next nightly Pub/Sub repo-ingested event to fire it.
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

# Make ingestion.enrichment importable when run as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ingestion.enrichment.tagger import enrich_tags  # noqa: E402
from ingestion.enrichment.taxonomy import (  # noqa: E402
    assign_all_categories,
    assign_primary_category,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

GITHUB_API = "https://api.github.com"


# ── Secrets / DB ─────────────────────────────────────────────────────────────


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
    raise RuntimeError("No DATABASE_URL found in env or Secret Manager")


def get_gh_token() -> str:
    token = os.getenv("GH_TOKEN", "").strip() or os.getenv("GITHUB_TOKEN", "").strip()
    if token:
        return token
    try:
        result = subprocess.run(
            ["gh", "auth", "token"], capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception:
        pass
    try:
        from google.cloud import secretmanager
        client = secretmanager.SecretManagerServiceClient()
        project = os.getenv("GCP_PROJECT", "perditio-platform")
        name = f"projects/{project}/secrets/gh-token/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8").strip()
    except Exception:
        pass
    raise RuntimeError("No GH_TOKEN found in env, gh CLI, or Secret Manager")


# ── GitHub fetch helpers ──────────────────────────────────────────────────────


def _gh_get(path: str, token: str, accept: str = "application/vnd.github+json") -> dict | list | str | None:
    url = f"{GITHUB_API}{path}"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": accept,
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "reporium-ingestion-backfill",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read()
            if "raw" in accept:
                return body.decode("utf-8", errors="replace")
            return json.loads(body.decode("utf-8", errors="replace"))
    except urllib.error.HTTPError as e:
        logger.debug(f"    HTTP {e.code} fetching {url}")
        return None
    except Exception as e:
        logger.debug(f"    Error fetching {url}: {e}")
        return None


def fetch_upstream_topics(upstream: str, token: str) -> list[str]:
    """GitHub does not inherit topics onto forks — fetch them from the upstream."""
    data = _gh_get(
        f"/repos/{upstream}/topics",
        token,
        accept="application/vnd.github.mercy-preview+json",
    )
    if isinstance(data, dict):
        topics = data.get("names", [])
        return [t for t in topics if isinstance(t, str)]
    return []


def fetch_upstream_readme(upstream: str, token: str) -> str | None:
    """Fetch the upstream README as raw text."""
    data = _gh_get(
        f"/repos/{upstream}/readme",
        token,
        accept="application/vnd.github.v3.raw",
    )
    if isinstance(data, str) and data.strip():
        return data
    return None


# ── DB query ──────────────────────────────────────────────────────────────────


NO_TAG_FORKS_SQL = """
    SELECT r.id,
           r.name,
           r.owner,
           r.forked_from,
           r.primary_language,
           COALESCE(r.stargazers_count, 0) AS stars,
           COALESCE(r.github_updated_at, r.updated_at) AS updated_at,
           COALESCE(r.parent_is_archived, FALSE) AS is_archived
    FROM repos r
    LEFT JOIN repo_tags rt ON rt.repo_id = r.id
    WHERE r.is_fork = TRUE
      AND r.forked_from IS NOT NULL
      AND r.forked_from <> ''
    GROUP BY r.id
    HAVING COUNT(rt.tag) = 0
    ORDER BY r.parent_stars DESC NULLS LAST, r.name ASC
"""


def fetch_no_tag_forks(cur) -> list[dict]:
    cur.execute(NO_TAG_FORKS_SQL)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in rows]


# ── Per-repo backfill ─────────────────────────────────────────────────────────


def _category_id(name: str) -> str:
    return (
        name.lower()
        .replace(" ", "-")
        .replace("&", "and")
        .replace(":", "")
    )


def backfill_one(conn, cur, repo: dict, token: str, dry_run: bool) -> dict:
    """Compute and write tags + categories for one repo. Returns stats dict."""
    upstream = repo["forked_from"]
    label = f"{repo['owner']}/{repo['name']} (fork of {upstream})"

    topics = fetch_upstream_topics(upstream, token)
    readme_text = fetch_upstream_readme(upstream, token)

    updated_at = repo["updated_at"]
    updated_at_iso = (
        updated_at.isoformat() if hasattr(updated_at, "isoformat") else str(updated_at or "")
    )

    tags = enrich_tags(
        language=repo.get("primary_language"),
        topics=topics,
        stars=int(repo.get("stars") or 0),
        updated_at=updated_at_iso,
        is_fork=True,
        is_archived=bool(repo.get("is_archived")),
        readme_text=readme_text,
    )

    primary_cat = assign_primary_category(tags)
    all_cats = assign_all_categories(tags)

    stats = {
        "tag_count": len(tags),
        "category_count": len(all_cats),
        "topics_from_upstream": len(topics),
        "readme_bytes": len(readme_text or ""),
    }

    if dry_run:
        logger.info(
            f"  DRY {label}: tags={len(tags)} cats={len(all_cats)} "
            f"upstream_topics={len(topics)} readme={len(readme_text or '')}b"
        )
        return stats

    repo_id = str(repo["id"])

    try:
        cur.execute("DELETE FROM repo_tags WHERE repo_id = %s;", (repo_id,))
        for tag in tags:
            cur.execute(
                "INSERT INTO repo_tags (repo_id, tag) VALUES (%s, %s) "
                "ON CONFLICT DO NOTHING;",
                (repo_id, tag),
            )

        cur.execute("DELETE FROM repo_categories WHERE repo_id = %s;", (repo_id,))
        for cat in all_cats:
            cur.execute(
                "INSERT INTO repo_categories (repo_id, category_id, category_name, is_primary) "
                "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;",
                (repo_id, _category_id(cat), cat, cat == primary_cat),
            )

        for tag in tags:
            cur.execute(
                "INSERT INTO repo_taxonomy (repo_id, dimension, raw_value, assigned_by) "
                "VALUES (%s, 'tag', %s, 'backfill_no_tag_forks') "
                "ON CONFLICT (repo_id, dimension, raw_value) DO NOTHING;",
                (repo_id, tag),
            )
        for cat in all_cats:
            cur.execute(
                "INSERT INTO repo_taxonomy (repo_id, dimension, raw_value, assigned_by) "
                "VALUES (%s, 'category', %s, 'backfill_no_tag_forks') "
                "ON CONFLICT (repo_id, dimension, raw_value) DO NOTHING;",
                (repo_id, cat),
            )

        conn.commit()
        logger.info(
            f"  OK  {label}: tags={len(tags)} cats={len(all_cats)} "
            f"primary={primary_cat!r}"
        )
    except Exception as e:
        conn.rollback()
        logger.warning(f"  ERR {label}: {e}")
        stats["error"] = str(e)

    return stats


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else "")
    parser.add_argument("--dry-run", action="store_true", help="Read-only. Print what would be written.")
    parser.add_argument("--limit", type=int, default=0, help="Process at most N repos (0 = all).")
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.3,
        help="Seconds to sleep between repos (GitHub rate-limit courtesy).",
    )
    args = parser.parse_args()

    import psycopg2

    logger.info("=" * 60)
    logger.info("Reporium: Backfill no-tag forks (deterministic tagger, no AI)")
    logger.info(f"Dry run: {args.dry_run}, limit: {args.limit or 'all'}")
    logger.info("=" * 60)

    token = get_gh_token()
    db_url = get_db_url()

    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    candidates = fetch_no_tag_forks(cur)
    logger.info(f"Found {len(candidates)} no-tag forks in DB")

    if args.limit:
        candidates = candidates[: args.limit]
        logger.info(f"  Limiting to first {len(candidates)}")

    if not candidates:
        logger.info("Nothing to backfill. Done.")
        conn.close()
        return 0

    t0 = time.monotonic()
    totals = {
        "processed": 0,
        "with_tags": 0,
        "still_empty": 0,
        "errors": 0,
        "total_tags_written": 0,
        "total_categories_written": 0,
    }

    for i, repo in enumerate(candidates, 1):
        result = backfill_one(conn, cur, repo, token, args.dry_run)
        totals["processed"] += 1
        if result.get("error"):
            totals["errors"] += 1
        elif result["tag_count"] > 0:
            totals["with_tags"] += 1
            totals["total_tags_written"] += result["tag_count"]
            totals["total_categories_written"] += result["category_count"]
        else:
            totals["still_empty"] += 1

        if i % 25 == 0 or i == len(candidates):
            elapsed = time.monotonic() - t0
            logger.info(
                f"  Progress: {i}/{len(candidates)} "
                f"with_tags={totals['with_tags']} still_empty={totals['still_empty']} "
                f"errors={totals['errors']} elapsed={elapsed:.0f}s"
            )

        time.sleep(args.sleep)

    print()
    print("=" * 60)
    print("BACKFILL COMPLETE" + (" (DRY RUN)" if args.dry_run else ""))
    print("=" * 60)
    for k, v in totals.items():
        print(f"  {k}: {v}")

    if not args.dry_run:
        cur.execute(
            "SELECT COUNT(*) FROM repos r "
            "LEFT JOIN repo_tags rt ON rt.repo_id = r.id "
            "WHERE r.is_fork = TRUE AND r.forked_from IS NOT NULL "
            "GROUP BY r.id HAVING COUNT(rt.tag) = 0;"
        )
        remaining = len(cur.fetchall())
        print(f"\n  No-tag forks remaining after run: {remaining}")

    conn.close()
    return 0 if totals["errors"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
