#!/usr/bin/env python3
"""
KAN-41 — Verify primary_category backfill results.

Prints:
  - Classification coverage (% of repos with primary_category)
  - Distribution across all 16 categories
  - Any unrepresented categories
  - Repos that failed classification (primary_category IS NULL after backfill)
  - Sample repos per category for spot-checking

Usage:
    DATABASE_URL=<psycopg2-url> python scripts/verify_primary_category.py
    DATABASE_URL=<psycopg2-url> python scripts/verify_primary_category.py --samples 3
"""

import argparse
import os
import sys

import psycopg2
import psycopg2.extras

VALID_CATEGORIES = [
    "agents", "rag-retrieval", "llm-serving", "fine-tuning", "evaluation",
    "orchestration", "vector-databases", "observability", "security-safety",
    "code-generation", "data-processing", "computer-vision", "nlp-text",
    "speech-audio", "generative-media", "infrastructure",
]


def get_db_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL not set")
    url = url.replace("+asyncpg", "")
    url = url.replace("?ssl=require", "?sslmode=require")
    return url


def run(samples: int = 2) -> None:
    conn = psycopg2.connect(get_db_url())
    cur = conn.cursor()

    # ── Coverage ──────────────────────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM repos")
    total = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM repos WHERE primary_category IS NOT NULL")
    classified = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM repos WHERE primary_category IS NULL")
    unclassified = cur.fetchone()[0]

    pct = classified / total * 100 if total else 0
    print(f"\n{'='*60}")
    print(f"KAN-41 Verification Report — {__import__('datetime').datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}")
    print(f"\nCOVERAGE")
    print(f"  Total repos:      {total:,}")
    print(f"  Classified:       {classified:,} ({pct:.1f}%)")
    print(f"  Unclassified:     {unclassified:,}")

    # Quality gate
    if pct >= 90:
        print(f"  ✅ Quality gate PASSED (≥90%)")
    else:
        print(f"  ❌ Quality gate FAILED (<90%) — {90 - pct:.1f}pp to go")

    # ── Distribution ──────────────────────────────────────────────────────────
    cur.execute("""
        SELECT primary_category, COUNT(*) as cnt
        FROM repos
        WHERE primary_category IS NOT NULL
        GROUP BY primary_category
        ORDER BY cnt DESC
    """)
    dist = cur.fetchall()
    dist_map = {row[0]: row[1] for row in dist}

    print(f"\nDISTRIBUTION (sorted by count)")
    print(f"  {'Category':<25} {'Count':>6}  {'Bar'}")
    print(f"  {'-'*25} {'-'*6}  {'-'*30}")
    max_count = max((v for v in dist_map.values()), default=1)
    for cat in sorted(dist_map, key=lambda c: -dist_map[c]):
        bar_len = int(dist_map[cat] / max_count * 30)
        bar = "█" * bar_len
        print(f"  {cat:<25} {dist_map[cat]:>6}  {bar}")

    # ── Missing categories ────────────────────────────────────────────────────
    missing = [c for c in VALID_CATEGORIES if c not in dist_map]
    unexpected = [c for c in dist_map if c not in set(VALID_CATEGORIES)]

    print(f"\nCATEGORY HEALTH")
    if missing:
        print(f"  ⚠️  Unrepresented categories ({len(missing)}): {', '.join(missing)}")
    else:
        print(f"  ✅ All 16 categories represented")

    if unexpected:
        print(f"  ⚠️  Unexpected values (not in taxonomy): {', '.join(unexpected)}")
    else:
        print(f"  ✅ No out-of-taxonomy values")

    # ── Unclassified repos ────────────────────────────────────────────────────
    if unclassified > 0:
        cur.execute("""
            SELECT owner || '/' || name, description
            FROM repos
            WHERE primary_category IS NULL
            ORDER BY name
            LIMIT 20
        """)
        rows = cur.fetchall()
        print(f"\nUNCLASSIFIED REPOS (first {min(20, unclassified)})")
        for name, desc in rows:
            print(f"  - {name}: {(desc or 'no description')[:80]}")

    # ── Samples per category ──────────────────────────────────────────────────
    if samples > 0:
        print(f"\nSAMPLES ({samples} per category)")
        for cat in VALID_CATEGORIES:
            cur.execute("""
                SELECT owner || '/' || name
                FROM repos
                WHERE primary_category = %s
                ORDER BY RANDOM()
                LIMIT %s
            """, (cat, samples))
            rows = cur.fetchall()
            if rows:
                names = ", ".join(r[0] for r in rows)
                print(f"  {cat:<25} → {names}")
            else:
                print(f"  {cat:<25} → (none)")

    # ── secondary_categories quick check ─────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM repos WHERE secondary_categories IS NOT NULL AND array_length(secondary_categories, 1) > 0")
    with_secondary = cur.fetchone()[0]
    print(f"\nSECONDARY CATEGORIES")
    print(f"  Repos with secondary_categories: {with_secondary:,}")

    print(f"\n{'='*60}\n")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="KAN-41: Verify primary_category backfill")
    parser.add_argument("--samples", type=int, default=2,
                        help="Sample repos to show per category (0 to skip)")
    args = parser.parse_args()
    run(samples=args.samples)
