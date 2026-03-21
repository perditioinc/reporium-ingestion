"""
Backfill Neon database from reporium's library.json.
Cost: $0 — reads JSON, writes to DB. No AI or GitHub API calls.

Backfills: enrichedTags, pmSkills, industries, builders, topics, categories, commitStats

Usage:
    DATABASE_URL=... python scripts/backfill_from_library_json.py /path/to/library.json
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

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
        project = os.getenv("GCP_PROJECT", "perditio-platform")
        name = f"projects/{project}/secrets/reporium-db-url/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8").strip()
    except Exception:
        pass
    raise RuntimeError("No DATABASE_URL found")


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/backfill_from_library_json.py /path/to/library.json")
        sys.exit(1)

    json_path = sys.argv[1]
    logger.info(f"Loading library.json from {json_path}")
    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    lib_repos = data.get("repos", [])
    logger.info(f"Library.json has {len(lib_repos)} repos")

    conn = psycopg2.connect(get_db_url())
    cur = conn.cursor()

    # Create repo_industries table if it doesn't exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS repo_industries (
            repo_id UUID NOT NULL REFERENCES repos(id),
            industry TEXT NOT NULL,
            PRIMARY KEY (repo_id, industry)
        );
    """)
    conn.commit()
    logger.info("repo_industries table ready")

    # Build name -> id map from DB
    cur.execute("SELECT id, name, owner FROM repos;")
    db_repos = {}
    for row in cur.fetchall():
        full_name = f"{row[2]}/{row[1]}"
        db_repos[full_name.lower()] = str(row[0])

    logger.info(f"DB has {len(db_repos)} repos")

    # Track stats
    stats = {
        "matched": 0,
        "unmatched": 0,
        "tags_inserted": 0,
        "pm_skills_inserted": 0,
        "industries_inserted": 0,
        "builders_inserted": 0,
        "topics_inserted": 0,
        "categories_replaced": 0,
        "commit_stats_updated": 0,
    }
    unmatched = []

    t0 = time.monotonic()

    for lib_repo in lib_repos:
        full_name = lib_repo.get("fullName", "")
        repo_id = db_repos.get(full_name.lower())

        if not repo_id:
            stats["unmatched"] += 1
            unmatched.append(full_name)
            continue

        stats["matched"] += 1

        # 1. enrichedTags → repo_tags (not repo_ai_dev_skills — those are a different signal)
        enriched_tags = lib_repo.get("enrichedTags", [])
        for tag in enriched_tags:
            if not tag:
                continue
            try:
                cur.execute(
                    "INSERT INTO repo_tags (repo_id, tag) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
                    (repo_id, tag),
                )
                stats["tags_inserted"] += 1
            except Exception:
                conn.rollback()

        # 2. pmSkills → repo_pm_skills
        pm_skills = lib_repo.get("pmSkills", [])
        for skill in pm_skills:
            if not skill:
                continue
            try:
                cur.execute(
                    "INSERT INTO repo_pm_skills (repo_id, skill) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
                    (repo_id, skill),
                )
                stats["pm_skills_inserted"] += 1
            except Exception:
                conn.rollback()

        # 3. industries → repo_industries
        industries = lib_repo.get("industries", [])
        for industry in industries:
            if not industry:
                continue
            try:
                cur.execute(
                    "INSERT INTO repo_industries (repo_id, industry) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
                    (repo_id, industry),
                )
                stats["industries_inserted"] += 1
            except Exception:
                conn.rollback()

        # 4. builders → repo_builders
        builders = lib_repo.get("builders", [])
        for b in builders:
            login = b.get("login", "")
            if not login:
                continue
            try:
                cur.execute(
                    """INSERT INTO repo_builders (repo_id, login, display_name, org_category, is_known_org)
                       VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;""",
                    (
                        repo_id,
                        login,
                        b.get("name") or b.get("login"),
                        b.get("orgCategory", "individual"),
                        b.get("isKnownOrg", False),
                    ),
                )
                stats["builders_inserted"] += 1
            except Exception:
                conn.rollback()

        # 5. topics → repo_tags (topics are also tags)
        topics = lib_repo.get("topics", [])
        for topic in topics:
            if not topic:
                continue
            try:
                cur.execute(
                    "INSERT INTO repo_tags (repo_id, tag) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
                    (repo_id, topic),
                )
                stats["topics_inserted"] += 1
            except Exception:
                conn.rollback()

        # 6. allCategories → repo_categories (replace existing)
        all_categories = lib_repo.get("allCategories", [])
        if all_categories:
            # Delete existing generic categories for this repo
            cur.execute("DELETE FROM repo_categories WHERE repo_id = %s;", (repo_id,))
            primary = lib_repo.get("primaryCategory", all_categories[0] if all_categories else "Other")
            for cat in all_categories:
                if not cat:
                    continue
                try:
                    # Map category name to a simple ID
                    cat_id = cat.lower().replace(" ", "-").replace("&", "and")
                    cur.execute(
                        """INSERT INTO repo_categories (repo_id, category_id, category_name, is_primary)
                           VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;""",
                        (repo_id, cat_id, cat, cat == primary),
                    )
                    stats["categories_replaced"] += 1
                except Exception:
                    conn.rollback()

        # 7. commitStats → repos columns
        commit_stats = lib_repo.get("commitStats", {})
        c7 = commit_stats.get("last7Days", 0) or 0
        c30 = commit_stats.get("last30Days", 0) or 0
        c90 = commit_stats.get("last90Days", 0) or 0
        if c7 > 0 or c30 > 0 or c90 > 0:
            try:
                cur.execute(
                    """UPDATE repos SET
                         commits_last_7_days = %s,
                         commits_last_30_days = %s,
                         commits_last_90_days = %s
                       WHERE id = %s;""",
                    (c7, c30, c90, repo_id),
                )
                stats["commit_stats_updated"] += 1
            except Exception:
                conn.rollback()

        # Commit every 100 repos
        if stats["matched"] % 100 == 0:
            conn.commit()
            logger.info(f"  Processed {stats['matched']}/{len(lib_repos)}...")

    conn.commit()
    elapsed = time.monotonic() - t0

    # Print results
    print()
    print("=" * 60)
    print("BACKFILL COMPLETE")
    print("=" * 60)
    print(f"  Time: {elapsed:.1f}s")
    print(f"  Matched: {stats['matched']}")
    print(f"  Unmatched: {stats['unmatched']}")
    print(f"  Tags inserted: {stats['tags_inserted']}")
    print(f"  PM skills inserted: {stats['pm_skills_inserted']}")
    print(f"  Industries inserted: {stats['industries_inserted']}")
    print(f"  Builders inserted: {stats['builders_inserted']}")
    print(f"  Topics inserted: {stats['topics_inserted']}")
    print(f"  Categories replaced: {stats['categories_replaced']}")
    print(f"  Commit stats updated: {stats['commit_stats_updated']}")

    if unmatched:
        print(f"\n  Unmatched repos ({len(unmatched)}):")
        for name in unmatched[:10]:
            print(f"    - {name}")
        if len(unmatched) > 10:
            print(f"    ... and {len(unmatched) - 10} more")

    # Verification queries
    print()
    print("=" * 60)
    print("VERIFICATION")
    print("=" * 60)
    for query, label in [
        ("SELECT COUNT(*) FROM repo_tags;", "repo_tags"),
        ("SELECT COUNT(*) FROM repo_pm_skills;", "repo_pm_skills"),
        ("SELECT COUNT(*) FROM repo_industries;", "repo_industries"),
        ("SELECT COUNT(*) FROM repo_builders;", "repo_builders"),
        ("SELECT COUNT(DISTINCT category_name) FROM repo_categories;", "distinct categories"),
        ("SELECT COUNT(*) FROM repos WHERE commits_last_7_days > 0;", "repos with commit stats"),
    ]:
        cur.execute(query)
        print(f"  {label}: {cur.fetchone()[0]}")

    conn.close()


if __name__ == "__main__":
    main()
