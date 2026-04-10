"""
Post-deploy backfill: populate repo_dependencies for all repos not yet covered.

Intended to be run ONCE after deploying migrations 031-033 in reporium-api.
Migration 031 already backfills any repos that had data in repo_taxonomy
(dimension='dependency'). This script covers the remaining repos by fetching
dependency files from GitHub.

Rate limits to stay well under GitHub's 5,000 req/hour ceiling.
Is idempotent and safely re-runnable — skips any repo_id already in repo_dependencies.

Usage:
    DATABASE_URL=... GH_TOKEN=... python scripts/backfill_repo_dependencies.py
    DATABASE_URL=... GH_TOKEN=... python scripts/backfill_repo_dependencies.py --repo-id <uuid>

Post-deploy checklist:
    1. alembic upgrade head
    2. python scripts/backfill_repo_dependencies.py
       Expected: ~1,680 repos, est. 20-30 min at safe rate limit
    3. Verify: SELECT edge_type, COUNT(*) FROM repo_edges GROUP BY edge_type
       Expected: DEPENDS_ON > 0

Reuses GitHubClient (with its RateLimitManager) rather than raw httpx.
"""

import argparse
import asyncio
import logging
import os
import sys
import time
import uuid

import psycopg2

# Allow running from either project root or scripts/ directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ingestion.github.client import GitHubClient
from ingestion.github.rate_limit import RateLimitManager
from ingestion.extractors.dependencies import DEPENDENCY_FILES, PARSERS, FILE_TO_ECOSYSTEM

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Stay under 5,000 req/hr = 83 req/min ≈ 1.4 req/sec.
# At ~2 API calls per repo average, target ≤ 0.6 repos/sec.
CONCURRENCY = 2
DELAY_BETWEEN_REPOS = 3.5  # seconds — gives 0.57 repos/sec * 2 calls = 1.14 calls/sec
MIN_RATE_LIMIT_REMAINING = 200  # pause if below this


async def fetch_deps_for_repo(
    gh: GitHubClient,
    repo_id: str,
    owner: str,
    repo_name: str,
    forked_from: str | None,
) -> tuple[list[str], str | None, str | None]:
    """
    Fetch dependency files for a single repo using the existing GitHubClient.
    Returns (packages, source_file, ecosystem).
    """
    targets = []
    if forked_from:
        parts = forked_from.split("/")
        if len(parts) == 2:
            targets.append((parts[0], parts[1]))
    targets.append((owner, repo_name))

    for target_owner, target_repo in targets:
        for filepath in DEPENDENCY_FILES:
            content = await gh.get_file(target_owner, target_repo, filepath)
            if content:
                parser = PARSERS.get(filepath)
                if parser:
                    packages = parser(content)
                    ecosystem = FILE_TO_ECOSYSTEM.get(filepath)
                    source_file = f"{target_owner}/{target_repo}/{filepath}"
                    return packages, source_file, ecosystem

    return [], None, None


async def run_backfill(
    db_url: str,
    gh_token: str,
    target_repo_id: str | None,
) -> None:
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    # Build the set of repo_ids already covered (idempotency check)
    cur.execute("SELECT DISTINCT repo_id FROM repo_dependencies;")
    already_done: set[str] = {str(row[0]) for row in cur.fetchall()}
    logger.info("Repos already in repo_dependencies: %d", len(already_done))

    # Select repos to process
    if target_repo_id:
        cur.execute(
            "SELECT id, name, owner, forked_from FROM repos WHERE id = %s;",
            (target_repo_id,),
        )
    else:
        # LEFT JOIN is the clean approach, but we also filter against already_done
        # in Python to avoid re-checking repos that already have entries.
        cur.execute("SELECT id, name, owner, forked_from FROM repos ORDER BY name;")

    all_repos = cur.fetchall()
    repos = [
        (str(r[0]), r[1], r[2], r[3])
        for r in all_repos
        if str(r[0]) not in already_done
    ]
    total = len(repos)

    if total == 0:
        logger.info("All repos already have repo_dependencies entries. Nothing to do.")
        conn.close()
        return

    logger.info("Repos needing backfill: %d", total)

    rate_limiter = RateLimitManager(min_buffer=MIN_RATE_LIMIT_REMAINING)
    semaphore = asyncio.Semaphore(CONCURRENCY)
    processed = 0
    with_deps = 0
    no_deps = 0
    errors = 0
    t_start = time.monotonic()

    async with GitHubClient(rate_limiter, None) as gh:
        # Prime the rate limit counter
        await gh.get_rate_limit()
        remaining = rate_limiter.remaining
        logger.info("GitHub rate limit remaining at start: %d", remaining)
        if remaining < MIN_RATE_LIMIT_REMAINING:
            wait_sec = rate_limiter.reset_in_seconds()
            logger.warning(
                "Rate limit too low (%d remaining). Waiting %ds for reset...",
                remaining,
                wait_sec,
            )
            await asyncio.sleep(wait_sec + 5)

        for i, (repo_id, name, owner, forked_from) in enumerate(repos):
            async with semaphore:
                t_repo = time.monotonic()
                try:
                    packages, source_file, ecosystem = await fetch_deps_for_repo(
                        gh, repo_id, owner, name, forked_from
                    )

                    if packages:
                        # Upsert — safe on re-run thanks to ON CONFLICT DO NOTHING
                        cur.execute(
                            "DELETE FROM repo_dependencies WHERE repo_id = %s",
                            (repo_id,),
                        )
                        for pkg in packages:
                            cur.execute(
                                """
                                INSERT INTO repo_dependencies
                                    (id, repo_id, package_name, package_ecosystem, is_direct)
                                VALUES (%s, %s, %s, %s, true)
                                ON CONFLICT (repo_id, package_name, package_ecosystem)
                                    DO NOTHING
                                """,
                                (str(uuid.uuid4()), repo_id, pkg, ecosystem),
                            )
                        conn.commit()
                        with_deps += 1
                        logger.info(
                            "[%d/%d] %s/%s — %d packages from %s (%.2fs)",
                            i + 1, total, owner, name,
                            len(packages), source_file,
                            time.monotonic() - t_repo,
                        )
                    else:
                        no_deps += 1
                        logger.debug("[%d/%d] %s/%s — no dep file found", i + 1, total, owner, name)

                    processed += 1

                except Exception as exc:
                    errors += 1
                    conn.rollback()
                    logger.warning("[%d/%d] %s/%s — error: %s", i + 1, total, owner, name, exc)

                # Progress checkpoint every 100 repos
                if (i + 1) % 100 == 0:
                    elapsed = time.monotonic() - t_start
                    rate_per_min = processed / (elapsed / 60)
                    eta_min = (total - processed) / max(rate_per_min, 0.01)
                    logger.info(
                        "--- Progress: %d/%d | with_deps=%d no_deps=%d errors=%d "
                        "| %.1f repos/min | ETA %.0f min | GH remaining=%d ---",
                        processed, total, with_deps, no_deps, errors,
                        rate_per_min, eta_min, rate_limiter.remaining,
                    )

                    # Pause if rate limit is getting low
                    if rate_limiter.remaining < MIN_RATE_LIMIT_REMAINING:
                        wait_sec = rate_limiter.reset_in_seconds()
                        logger.warning(
                            "Rate limit low (%d). Pausing %ds...",
                            rate_limiter.remaining, wait_sec,
                        )
                        await asyncio.sleep(wait_sec + 5)

                await asyncio.sleep(DELAY_BETWEEN_REPOS)

    conn.close()

    elapsed = time.monotonic() - t_start
    logger.info(
        "Backfill complete: %d processed, %d with deps, %d no deps, %d errors in %.0fs (%.1f min)",
        processed, with_deps, no_deps, errors, elapsed, elapsed / 60,
    )

    if errors > 0:
        logger.warning("%d repos had errors — re-run to retry them", errors)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill repo_dependencies from GitHub")
    parser.add_argument(
        "--repo-id",
        metavar="UUID",
        default=None,
        help="Only process this specific repo_id (for testing)",
    )
    args = parser.parse_args()

    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        logger.error("DATABASE_URL is required")
        sys.exit(1)

    gh_token = os.getenv("GH_TOKEN", "").strip()
    if not gh_token:
        logger.error("GH_TOKEN is required")
        sys.exit(1)

    asyncio.run(run_backfill(db_url, gh_token, args.repo_id))


if __name__ == "__main__":
    main()
