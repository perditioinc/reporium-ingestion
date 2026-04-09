"""
Build knowledge graph edges from enriched repo data.
Cost: $0 — uses existing data only.

Edge types:
  COMPATIBLE_WITH — repos sharing 2+ integration tags
  ALTERNATIVE_TO  — repos in the same category (from repo_categories)
  DEPENDS_ON      — repos where one appears in another's repo_dependencies

Requires:
  - Migration 031 (repo_edges table with confidence column)
  - Migration 032 (repo_edges_history table)

Usage:
    DATABASE_URL=... python scripts/build_knowledge_graph.py
"""

import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from itertools import combinations

import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def get_db_url() -> str:
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("Set DATABASE_URL")
    return url


def verify_table(cur):
    """Verify repo_edges table exists (created by migration 031)."""
    cur.execute("""
        SELECT 1 FROM information_schema.tables
        WHERE table_name = 'repo_edges'
    """)
    if not cur.fetchone():
        raise RuntimeError(
            "repo_edges table does not exist. "
            "Run migration 031_formalize_repo_edges first."
        )


# ---------------------------------------------------------------------------
# Edge builders
# ---------------------------------------------------------------------------

def build_compatible_with(cur):
    """
    Create COMPATIBLE_WITH edges for repos sharing 2+ integration tags.
    Weight = shared_tag_count / max(tags_a, tags_b).
    Confidence = min(0.85, shared_tag_count / 5).

    Uses a top-K-per-repo strategy: each repo keeps at most MAX_PER_REPO
    strongest matches to prevent O(n^2) explosion on ubiquitous tags.
    """
    MAX_PER_REPO = 30
    logger.info("Building COMPATIBLE_WITH edges...")

    cur.execute("""
        SELECT id, name, forked_from, integration_tags
        FROM repos
        WHERE integration_tags IS NOT NULL AND integration_tags::text != '[]';
    """)

    repos_with_tags = []
    repo_by_id = {}
    for row in cur.fetchall():
        tags = row[3] if isinstance(row[3], list) else json.loads(row[3]) if row[3] else []
        if len(tags) >= 1:
            repo = {
                "id": row[0],
                "name": row[1],
                "forked_from": row[2],
                "tags": set(t.lower() for t in tags),
            }
            repos_with_tags.append(repo)
            repo_by_id[str(row[0])] = repo

    logger.info(f"  Repos with integration tags: {len(repos_with_tags)}")

    # Build tag -> repos index
    tag_to_repos = defaultdict(list)
    for repo in repos_with_tags:
        for tag in repo["tags"]:
            tag_to_repos[tag].append(repo)

    logger.info(f"  Unique tags: {len(tag_to_repos)}")
    big_tags = {t: len(rs) for t, rs in tag_to_repos.items() if len(rs) > 300}
    if big_tags:
        logger.info(f"  Tags with >300 repos (included): {big_tags}")

    # Accumulate shared tags per pair
    pair_shared = defaultdict(set)
    for tag, repos in tag_to_repos.items():
        if len(repos) < 2:
            continue
        if len(repos) > 500:
            logger.info(f"  Processing tag '{tag}' with {len(repos)} repos...")
        for r1, r2 in combinations(repos, 2):
            pair_key = tuple(sorted([str(r1["id"]), str(r2["id"])]))
            pair_shared[pair_key].add(tag)

    # Build candidate edges (pairs sharing 2+ tags)
    repo_candidates = defaultdict(list)
    for pair_key, shared_tags in pair_shared.items():
        if len(shared_tags) < 2:
            continue
        r1_id, r2_id = pair_key
        r1 = repo_by_id.get(r1_id)
        r2 = repo_by_id.get(r2_id)
        if not r1 or not r2:
            continue
        weight = len(shared_tags) / max(len(r1["tags"]), len(r2["tags"]))
        entry = (weight, pair_key, shared_tags, r1, r2)
        repo_candidates[r1_id].append(entry)
        repo_candidates[r2_id].append(entry)

    # Top-K per repo
    selected_pairs = set()
    for repo_id, candidates in repo_candidates.items():
        candidates.sort(key=lambda x: x[0], reverse=True)
        for weight, pair_key, shared_tags, r1, r2 in candidates[:MAX_PER_REPO]:
            selected_pairs.add(pair_key)

    # Build final edge list
    edges = []
    for pair_key in selected_pairs:
        shared_tags = pair_shared[pair_key]
        r1_id, r2_id = pair_key
        r1 = repo_by_id[r1_id]
        r2 = repo_by_id[r2_id]
        weight = len(shared_tags) / max(len(r1["tags"]), len(r2["tags"]))
        confidence = min(0.85, len(shared_tags) / 5)
        edges.append({
            "source": r1["id"],
            "target": r2["id"],
            "weight": weight,
            "confidence": confidence,
            "evidence": {"shared_tags": sorted(shared_tags), "count": len(shared_tags)},
            "source_name": r1["forked_from"] or r1["name"],
            "target_name": r2["forked_from"] or r2["name"],
        })

    logger.info(f"  COMPATIBLE_WITH edges (top-{MAX_PER_REPO}/repo): {len(edges)}")
    return edges


def build_alternative_to(cur):
    """
    Create ALTERNATIVE_TO edges for repos in the same category.
    Confidence = 0.7 for primary_category, 0.4 for keyword fallback.
    """
    MAX_PER_REPO = 30
    logger.info("Building ALTERNATIVE_TO edges...")

    cur.execute("SELECT COUNT(*) FROM repo_categories;")
    cat_count = cur.fetchone()[0]
    logger.info(f"  Entries in repo_categories: {cat_count}")

    if cat_count == 0:
        logger.info("  No categories in DB. Using problem_solved grouping as fallback...")

        cur.execute("""
            SELECT id, name, forked_from, problem_solved
            FROM repos
            WHERE problem_solved IS NOT NULL AND problem_solved != '';
        """)

        PROBLEM_GROUPS = {
            "llm-framework": ["llm", "language model", "large language"],
            "vector-db": ["vector database", "vector search", "similarity search", "embedding"],
            "ocr": ["ocr", "optical character", "text extraction", "document parsing"],
            "agent": ["autonomous agent", "ai agent", "agent framework", "task planning"],
            "code-editor": ["code editor", "ide", "development environment"],
            "web-framework": ["web framework", "web application", "http server"],
            "ml-framework": ["machine learning framework", "deep learning", "neural network", "training"],
            "container": ["container", "kubernetes", "docker", "orchestration"],
            "monitoring": ["monitoring", "observability", "metrics", "logging"],
            "data-pipeline": ["data pipeline", "etl", "data processing", "data flow"],
        }

        group_repos = defaultdict(list)
        for row in cur.fetchall():
            problem = (row[3] or "").lower()
            for group_name, keywords in PROBLEM_GROUPS.items():
                if any(kw in problem for kw in keywords):
                    group_repos[group_name].append({
                        "id": row[0], "name": row[1], "forked_from": row[2],
                    })
                    break

        edges = []
        seen = set()
        for group_name, repos in group_repos.items():
            if len(repos) < 2:
                continue
            for r1, r2 in combinations(repos, 2):
                pair_key = tuple(sorted([str(r1["id"]), str(r2["id"])]))
                if pair_key in seen:
                    continue
                seen.add(pair_key)
                edges.append({
                    "source": r1["id"],
                    "target": r2["id"],
                    "weight": 0.7,
                    "confidence": 0.4,  # keyword fallback = low confidence
                    "evidence": {"category": group_name, "method": "problem_solved_keywords"},
                    "source_name": r1["forked_from"] or r1["name"],
                    "target_name": r2["forked_from"] or r2["name"],
                })

        logger.info(f"  ALTERNATIVE_TO edges (keyword fallback): {len(edges)}")
        return edges

    # Use primary_category to avoid multi-category explosion.
    cur.execute("""
        SELECT r.primary_category, r.id, r.name, r.forked_from
        FROM repos r
        WHERE r.primary_category IS NOT NULL
        ORDER BY r.primary_category;
    """)

    cat_repos = defaultdict(list)
    repo_by_id = {}
    for row in cur.fetchall():
        repo = {"id": row[1], "name": row[2], "forked_from": row[3]}
        cat_repos[row[0]].append(repo)
        repo_by_id[str(row[1])] = repo

    logger.info(f"  Categories (primary_category): {len(cat_repos)}")

    # Top-K-per-repo strategy
    repo_candidates = defaultdict(list)
    seen_pairs = set()
    for cat, repos in cat_repos.items():
        if len(repos) < 2:
            continue
        for r1, r2 in combinations(repos, 2):
            pair_key = tuple(sorted([str(r1["id"]), str(r2["id"])]))
            if pair_key in seen_pairs:
                continue
            seen_pairs.add(pair_key)
            entry = (1.0, pair_key, cat)
            repo_candidates[str(r1["id"])].append(entry)
            repo_candidates[str(r2["id"])].append(entry)

    selected_pairs = set()
    for repo_id, candidates in repo_candidates.items():
        for weight, pair_key, cat in candidates[:MAX_PER_REPO]:
            selected_pairs.add((pair_key, cat))

    edges = []
    for pair_key, cat in selected_pairs:
        r1_id, r2_id = pair_key
        r1 = repo_by_id[r1_id]
        r2 = repo_by_id[r2_id]
        edges.append({
            "source": r1["id"],
            "target": r2["id"],
            "weight": 1.0,
            "confidence": 0.7,  # same DB category row
            "evidence": {"category": cat, "method": "primary_category"},
            "source_name": r1["forked_from"] or r1["name"],
            "target_name": r2["forked_from"] or r2["name"],
        })

    logger.info(f"  ALTERNATIVE_TO edges (primary_category, top-{MAX_PER_REPO}/repo): {len(edges)}")
    return edges


def build_depends_on(cur):
    """
    Create DEPENDS_ON edges from the repo_dependencies table (migration 029).
    Reads direct dependencies and matches package_name (normalized:
    lowercase, strip hyphens/underscores) against repo names in the DB.

    Confidence = 0.95 (deterministic, from actual package files).
    """
    logger.info("Building DEPENDS_ON edges...")

    # Verify repo_dependencies table exists
    cur.execute("""
        SELECT 1 FROM information_schema.tables
        WHERE table_name = 'repo_dependencies'
    """)
    if not cur.fetchone():
        logger.warning("  repo_dependencies table not found - skipping DEPENDS_ON")
        return []

    # Read direct dependencies, excluding sentinel rows
    cur.execute("""
        SELECT rd.repo_id, rd.package_name, rd.package_ecosystem
        FROM repo_dependencies rd
        WHERE rd.is_direct = true
          AND rd.package_name != '__none__';
    """)
    dep_rows = cur.fetchall()
    logger.info(f"  Direct dependencies in repo_dependencies: {len(dep_rows)}")

    if not dep_rows:
        logger.warning("  No direct dependencies found - DEPENDS_ON will be empty")
        logger.warning("  Run dependency extraction to populate repo_dependencies")
        return []

    # Group by repo_id
    repo_deps = defaultdict(list)
    for repo_id, pkg_name, ecosystem in dep_rows:
        repo_deps[str(repo_id)].append((pkg_name, ecosystem))

    logger.info(f"  Repos with direct dependencies: {len(repo_deps)}")

    # Build repo name index
    cur.execute("SELECT id, name, forked_from FROM repos;")
    name_to_repo = {}
    id_to_repo = {}
    for row in cur.fetchall():
        repo_info = {"id": row[0], "name": row[1], "forked_from": row[2]}
        id_to_repo[str(row[0])] = repo_info
        upstream = row[2] or row[1]
        repo_name = upstream.split("/")[1] if "/" in upstream else upstream
        normalized = repo_name.lower().replace("-", "").replace("_", "")
        name_to_repo[normalized] = repo_info

    logger.info(f"  Repo name index size: {len(name_to_repo)}")

    # Match dependencies against repos
    edges = []
    seen = set()
    for repo_id, deps in repo_deps.items():
        src = id_to_repo.get(repo_id)
        if not src:
            continue
        for pkg_name, ecosystem in deps:
            normalized_pkg = pkg_name.lower().replace("-", "").replace("_", "")
            if normalized_pkg in name_to_repo:
                target = name_to_repo[normalized_pkg]
                if repo_id == str(target["id"]):
                    continue  # Skip self-reference
                pair_key = (repo_id, str(target["id"]))
                if pair_key in seen:
                    continue
                seen.add(pair_key)

                edges.append({
                    "source": repo_id,
                    "target": target["id"],
                    "weight": 1.0,
                    "confidence": 0.95,  # deterministic, from actual package files
                    "evidence": {
                        "package": pkg_name,
                        "ecosystem": ecosystem,
                        "method": "repo_dependencies",
                    },
                    "source_name": src["forked_from"] or src["name"],
                    "target_name": target["forked_from"] or target["name"],
                })

    logger.info(f"  DEPENDS_ON edges (no cap): {len(edges)}")
    return edges


# ---------------------------------------------------------------------------
# Edge insertion
# ---------------------------------------------------------------------------

def insert_edges(cur, edges, edge_type):
    """Batch-insert edges into repo_edges table with confidence."""
    inserted = 0
    BATCH = 500
    for i in range(0, len(edges), BATCH):
        batch = edges[i:i + BATCH]
        values = []
        params = []
        for e in batch:
            values.append("(%s, %s, %s, %s, %s, %s)")
            params.extend([
                str(e["source"]), str(e["target"]), edge_type,
                e["weight"], e.get("confidence", 0.5), json.dumps(e["evidence"]),
            ])
        sql = (
            "INSERT INTO repo_edges "
            "(source_repo_id, target_repo_id, edge_type, weight, confidence, evidence) "
            "VALUES " + ", ".join(values)
            + " ON CONFLICT (source_repo_id, target_repo_id, edge_type) DO UPDATE SET"
            " weight = EXCLUDED.weight,"
            " confidence = EXCLUDED.confidence,"
            " evidence = EXCLUDED.evidence,"
            " updated_at = NOW()"
        )
        try:
            cur.execute(sql, params)
            inserted += len(batch)
        except Exception as ex:
            logger.warning(f"  Batch insert failed: {ex}")
        if (i + BATCH) % 5000 == 0 or i + BATCH >= len(edges):
            logger.info(f"  {edge_type}: inserted {inserted}/{len(edges)}")
    return inserted


def record_history(cur, edge_counts: dict, run_id: int | None = None):
    """Record edge counts in repo_edges_history for velocity tracking."""
    for edge_type, count in edge_counts.items():
        cur.execute(
            """INSERT INTO repo_edges_history (run_id, edge_type, edge_count)
               VALUES (%s, %s, %s)""",
            (run_id, edge_type, count),
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    t0 = time.monotonic()
    logger.info("Building knowledge graph edges ($0)")

    conn = psycopg2.connect(get_db_url())
    cur = conn.cursor()

    # Verify migration 031 has run
    verify_table(cur)
    logger.info("repo_edges table verified")

    # Clear only managed edge types (preserve others like MAINTAINED_BY)
    cur.execute(
        "DELETE FROM repo_edges "
        "WHERE edge_type IN ('COMPATIBLE_WITH', 'ALTERNATIVE_TO', 'DEPENDS_ON');"
    )
    deleted = cur.rowcount
    conn.commit()
    logger.info(f"Cleared {deleted} existing managed edges")

    # Build each edge type
    compatible_edges = build_compatible_with(cur)
    alternative_edges = build_alternative_to(cur)
    depends_edges = build_depends_on(cur)

    # Insert all
    c1 = insert_edges(cur, compatible_edges, "COMPATIBLE_WITH")
    conn.commit()
    c2 = insert_edges(cur, alternative_edges, "ALTERNATIVE_TO")
    conn.commit()
    c3 = insert_edges(cur, depends_edges, "DEPENDS_ON")
    conn.commit()

    # Record history for velocity tracking
    edge_counts = {
        "COMPATIBLE_WITH": c1,
        "ALTERNATIVE_TO": c2,
        "DEPENDS_ON": c3,
    }
    try:
        record_history(cur, edge_counts)
        conn.commit()
    except Exception as ex:
        logger.warning(f"Failed to record edge history: {ex}")
        conn.rollback()

    elapsed = time.monotonic() - t0

    # Summary
    cur.execute(
        "SELECT edge_type, COUNT(*) FROM repo_edges GROUP BY edge_type ORDER BY edge_type;"
    )
    print()
    print("=" * 60)
    print("KNOWLEDGE GRAPH SUMMARY")
    print("=" * 60)
    total = 0
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]} edges")
        total += row[1]
    print(f"  TOTAL: {total} edges")
    print(f"  Time: {elapsed:.1f}s")
    print(f"  Cost: $0.00")
    print()

    for label, edges_list in [
        ("COMPATIBLE_WITH", compatible_edges),
        ("ALTERNATIVE_TO", alternative_edges),
        ("DEPENDS_ON", depends_edges),
    ]:
        print(f"EXAMPLE {label} EDGES:")
        for e in edges_list[:3]:
            confidence_str = f" (confidence={e.get('confidence', '?')})"
            print(f"  {e['source_name']} <-> {e['target_name']}{confidence_str}")
            print(f"    evidence: {e['evidence']}")
        print()

    conn.close()


if __name__ == "__main__":
    main()
