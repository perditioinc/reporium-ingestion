"""
FIX 2: Populate commit stats from GitHub API.
Uses GET /repos/{owner}/{repo}/stats/commit_activity — returns 52 weeks of commit counts.
Updates commits_last_7_days, commits_last_30_days, commits_last_90_days on repos table.

Cost: $0 — GitHub API only.
Rate limit: 500ms delay between requests, stops if remaining < 100.

202 handling: GitHub returns 202 Accepted with an empty body while it computes
stats asynchronously, then 200 with data on a subsequent request. We retry up
to 3 attempts total (first call + 2 retries) with a 2s sleep between, then give
up and return None. A None return signals "unavailable" — the caller MUST NOT
overwrite the existing commits_last_*_days columns (preserving last good values
rather than overwriting with zeros). Without this retry, repos that hit 202 on
first attempt are silently skipped and their commit-stats columns stay at the
default 0 forever — that was the root cause of universal `last7Days = 0` on
the live /trends page (KAN-DRAFT-trends-202-retry, 2026-04-30).

Usage:
    GH_TOKEN=... DATABASE_URL=... python scripts/fetch_commit_stats.py
"""

import json
import logging
import os
import sys
import time

import httpx

# psycopg2 is imported lazily inside get_db_url() / main() so the fetcher
# function and its tests can be exercised without the (heavy, native-binary)
# DB driver installed. CI installs only the minimum needed to run unit tests.

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# Retry policy for `/stats/commit_activity` — see module docstring for rationale.
COMMIT_ACTIVITY_MAX_ATTEMPTS = 3
COMMIT_ACTIVITY_RETRY_SLEEP_S = 2.0


def fetch_commit_activity(
    client: httpx.Client,
    target: str,
    headers: dict,
    max_attempts: int = COMMIT_ACTIVITY_MAX_ATTEMPTS,
    retry_sleep: float = COMMIT_ACTIVITY_RETRY_SLEEP_S,
):
    """
    Fetch /stats/commit_activity with 202-retry.

    Returns:
        list[dict]: weeks payload on a 200 (may be empty for brand-new repos).
        None:      stats unavailable — caller MUST NOT overwrite stored columns.
                   This covers (a) max_attempts consecutive 202s, (b) 4xx/5xx
                   responses other than 200, (c) malformed JSON.

    Side effect: sets `client._last_rate_limit_remaining` to the most recent
    `x-ratelimit-remaining` header value seen (or None if unparseable). Callers
    can read this for budget enforcement without making an extra request.

    The signature accepts an injected `httpx.Client` for testability.
    """
    url = f"https://api.github.com/repos/{target}/stats/commit_activity"
    last_status: int | None = None

    for attempt in range(1, max_attempts + 1):
        resp = client.get(url, headers=headers)
        last_status = resp.status_code

        # Stash rate-limit info so caller can early-abort on budget exhaustion.
        try:
            setattr(
                client,
                "_last_rate_limit_remaining",
                int(resp.headers.get("x-ratelimit-remaining", 999)),
            )
        except (ValueError, TypeError):
            setattr(client, "_last_rate_limit_remaining", None)

        if resp.status_code == 200:
            try:
                body = resp.json()
            except (ValueError, json.JSONDecodeError):
                logger.warning(
                    "Malformed JSON from /stats/commit_activity for %s — skipping",
                    target,
                )
                return None
            return body if isinstance(body, list) else None

        if resp.status_code == 202:
            if attempt < max_attempts:
                logger.info(
                    "202 Accepted on /stats/commit_activity for %s (attempt %d/%d) — "
                    "GitHub is still computing, retrying in %.1fs",
                    target, attempt, max_attempts, retry_sleep,
                )
                if retry_sleep > 0:
                    time.sleep(retry_sleep)
                continue
            # Out of retries on 202 — stats genuinely unavailable right now.
            logger.warning(
                "Persistent 202 on /stats/commit_activity for %s after %d attempts — "
                "preserving existing commits_last_*_days columns (no overwrite)",
                target, max_attempts,
            )
            return None

        # Any other status (404, 5xx, …) — caller should not overwrite.
        return None

    # Defensive: loop should have returned. Treat as unavailable.
    logger.warning(
        "Exited /stats/commit_activity retry loop without a result for %s "
        "(last status: %s)",
        target, last_status,
    )
    return None


def normalize_db_url(url: str) -> str:
    """
    Convert an asyncpg-style URL to a psycopg2-compatible one.

    Mirrors scripts/enrich_new_repos.py:normalize_db_url — duplicated rather
    than imported because scripts/ is not a package and both files run as
    top-level entrypoints. Keep the two implementations in sync.

    - Strip +asyncpg driver suffix
    - Map asyncpg ssl=require query param to psycopg2 sslmode=require
    """
    import urllib.parse

    url = url.replace("+asyncpg", "")
    if url.startswith("postgresql+"):
        url = "postgresql" + url[url.index("://"):]

    parsed = urllib.parse.urlsplit(url)
    params = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)

    ssl_val = params.pop("ssl", [None])[0]
    if ssl_val and "sslmode" not in params:
        if ssl_val.lower() in ("true", "1", "require"):
            params["sslmode"] = ["require"]
        elif ssl_val.lower() in ("false", "0", "disable"):
            params["sslmode"] = ["disable"]

    new_query = urllib.parse.urlencode(
        {k: v[0] for k, v in params.items()}, safe=""
    )
    return urllib.parse.urlunsplit(parsed._replace(query=new_query))


def get_db_url() -> str:
    url = os.getenv("DATABASE_URL", "").strip()
    if url:
        return normalize_db_url(url)
    try:
        from google.cloud import secretmanager
        client = secretmanager.SecretManagerServiceClient()
        # Use the async URL secret (same one the Cloud Run Job manifest provides
        # via DATABASE_URL env). The previous "reporium-db-url" name was stale —
        # only the async variant exists post-Cloud-SQL-migration.
        name = "projects/perditio-platform/secrets/reporium-db-url-async/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return normalize_db_url(response.payload.data.decode("UTF-8").strip())
    except Exception:
        pass
    raise RuntimeError("No DATABASE_URL found")


def main():
    import psycopg2  # Lazy import — keeps fetcher unit-testable without DB driver.

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
                weeks = fetch_commit_activity(client, target, headers)

                # Rate-limit check using the headers stashed by the fetcher —
                # avoids an extra `/rate_limit` round-trip. If we burned through
                # the budget (e.g. retries on many repos), stop the run early
                # rather than triggering 403 secondary-rate-limit penalties.
                remaining = getattr(client, "_last_rate_limit_remaining", None)
                if remaining is not None and remaining < 100:
                    logger.warning(
                        "Rate limit low (%s remaining), stopping early", remaining
                    )
                    break

                if weeks is None:
                    # Either persistent 202 or non-200/404/etc.
                    # Deliberately do NOT update the row — preserves existing
                    # commits_last_*_days values rather than overwriting with 0.
                    skipped += 1
                    continue

                if len(weeks) == 0:
                    # Brand-new repo with no week data yet. Same skip semantics.
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
