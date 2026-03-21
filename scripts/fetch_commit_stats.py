"""
FIX 2: Populate commit stats from GitHub API.
Uses GET /repos/{owner}/{repo}/stats/commit_activity — returns 52 weeks of commit counts.
Updates commits_last_7_days, commits_last_30_days, commits_last_90_days on repos table.

Cost: $0 — GitHub API only.
Rate limit: 500ms delay between requests, stops if remaining < 100.

Usage:
    GH_TOKEN=... DATABASE_URL=... python scripts/fetch_commit_stats.py
"""

import json
import logging
import os
import sys
import time

import httpx
import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def get_db_url() -> str:
    url = os.getenv("DATABASE_URL", "").strip()
    if url:
        return url
    try:
        from google.cloud import secretmanager
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/perditio-platform/secrets/reporium-db-url/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8").strip()
    except Exception:
        pass
    raise RuntimeError("No DATABASE_URL found")


def main():
    token = os.getenv("GH_TOKEN", "").strip()
    if not token:
        print("ERROR: GH_TOKEN required")
        sys.exit(1)

    conn = psycopg2.connect(get_db_url())
    cur = conn.cursor()

    # Get all repos — use forked_from (upstream) for stats since perditioinc forks
    # have no commits. The upstream's commit activity is what matters.
    cur.execute("""
        SELECT id, name, owner, forked_from
        FROM repos
        ORDER BY parent_stars DESC NULLS LAST;
    """)
    repos = cur.fetchall()
    logger.info(f"Total repos: {len(repos)}")

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3+json",
    }

    updated = 0
    skipped = 0
    errors = 0
    t0 = time.monotonic()

    with httpx.Client(timeout=30.0) as client:
        for i, (repo_id, name, owner, forked_from) in enumerate(repos):
            # Use upstream repo for commit stats (fork itself usually has 0 commits)
            target = forked_from or f"{owner}/{name}"

            try:
                resp = client.get(
                    f"https://api.github.com/repos/{target}/stats/commit_activity",
                    headers=headers,
                )

                # Check rate limit
                remaining = int(resp.headers.get("x-ratelimit-remaining", 999))
                if remaining < 100:
                    logger.warning(f"Rate limit low ({remaining}), stopping early")
                    break

                if resp.status_code == 202:
                    # GitHub is computing stats, skip for now
                    skipped += 1
                    continue

                if resp.status_code != 200:
                    skipped += 1
                    continue

                weeks = resp.json()
                if not isinstance(weeks, list) or len(weeks) == 0:
                    skipped += 1
                    continue

                # weeks is a list of {total, week, days} for last 52 weeks
                # Most recent week is last element
                c7 = weeks[-1].get("total", 0) if len(weeks) >= 1 else 0
                c30 = sum(w.get("total", 0) for w in weeks[-4:]) if len(weeks) >= 4 else 0
                c90 = sum(w.get("total", 0) for w in weeks[-13:]) if len(weeks) >= 13 else 0

                cur.execute(
                    """UPDATE repos SET
                         commits_last_7_days = %s,
                         commits_last_30_days = %s,
                         commits_last_90_days = %s
                       WHERE id = %s;""",
                    (c7, c30, c90, str(repo_id)),
                )
                updated += 1

            except Exception as e:
                errors += 1
                logger.warning(f"Error fetching {target}: {e}")

            if (i + 1) % 50 == 0:
                conn.commit()
                elapsed = time.monotonic() - t0
                logger.info(f"  Progress: {i+1}/{len(repos)} | updated={updated} skipped={skipped} errors={errors} | {elapsed:.0f}s")

            # Rate limit delay
            time.sleep(0.5)

    conn.commit()
    elapsed = time.monotonic() - t0

    print()
    print("=" * 50)
    print("COMMIT STATS FETCH COMPLETE")
    print("=" * 50)
    print(f"  Time: {elapsed:.0f}s")
    print(f"  Updated: {updated}")
    print(f"  Skipped: {skipped}")
    print(f"  Errors: {errors}")

    # Verify
    cur.execute("SELECT COUNT(*) FROM repos WHERE commits_last_7_days > 0;")
    print(f"  Repos with commits_last_7_days > 0: {cur.fetchone()[0]}")
    cur.execute("SELECT COUNT(*) FROM repos WHERE commits_last_30_days > 0;")
    print(f"  Repos with commits_last_30_days > 0: {cur.fetchone()[0]}")

    conn.close()


if __name__ == "__main__":
    main()
