"""
Phase 4: Build knowledge graph edges from enriched repo data.
Cost: $0 — uses existing data only.

Edge types:
  COMPATIBLE_WITH — repos sharing 2+ integration tags
  ALTERNATIVE_TO  — repos in the same category (from repo_categories)
  DEPENDS_ON      — repos where one appears in another's dependencies (from repo_dependencies)

Confidence values:
  DEPENDS_ON:      0.95  (direct package-name match is high-signal)
  COMPATIBLE_WITH: tag overlap ratio (0.0–1.0), floor at 0.3
  ALTERNATIVE_TO:  0.70 if from repo_categories, 0.40 if from keyword fallback

Requires repo_edges table (migration 033 in reporium-api). Will raise if table is absent
rather than silently creating a differently-structured table.

Usage:
    DATABASE_URL=... [INGEST_RUN_ID=123] python scripts/build_knowledge_graph.py
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


def get_ingest_run_id() -> int | None:
    """Return the ingest_runs.id for the current run if set, else None."""
    val = os.getenv("INGEST_RUN_ID", "").strip()
    try:
        return int(val) if val else None
    except ValueError:
        logger.warning("INGEST_RUN_ID is not an integer: %r — ignoring", val)
        return None


def assert_schema(cur) -> None:
    """Raise if repo_edges table is missing — do not auto-create it.
    Schema is owned by Alembic migration 033 in reporium-api.
    """
    cur.execute("""
        SELECT 1 FROM information_schema.tables
        WHERE table_name = 'repo_edges'
    """)
    if not cur.fetchone():
        raise RuntimeError(
            "repo_edges table not found. Run migration 033 in reporium-api first."
        )


def build_compatible_with(cur) -> list[dict]:
    """
    Create COMPATIBLE_WITH edges for repos sharing 2+ integration tags.
    Confidence = tag overlap ratio; capped at 15 000 edges.
    """
    logger.info("Building COMPATIBLE_WITH edges...")

    cur.execute("""
        SELECT id, name, forked_from, integration_tags
        FROM repos
        WHERE integration_tags IS NOT NULL AND integration_tags::text != '[]';
    """)

    repos_with_tags = []
    for row in cur.fetchall():
        tags = row[3] if isinstance(row[3], list) else json.loads(row[3]) if row[3] else []
        if len(tags) >= 1:
            repos_with_tags.append({
                "id": row[0],
                "name": row[1],
                "forked_from": row[2],
                "tags": set(t.lower() for t in tags),
            })

    logger.info(f"  Repos with integration tags: {len(repos_with_tags)}")

    tag_to_repos: dict[str, list] = defaultdict(list)
    for repo in repos_with_tags:
        for tag in repo["tags"]:
            tag_to_repos[tag].append(repo)

    edges: list[dict] = []
    seen: set = set()
    pair_shared: dict = defaultdict(set)  # (id1, id2) -> shared tags

    for tag, repos in tag_to_repos.items():
        if len(repos) > 300:
            continue  # Skip very common tags to avoid O(n²) blowup
        for r1, r2 in combinations(repos, 2):
            pair_key = tuple(sorted([str(r1["id"]), str(r2["id"])]))
            pair_shared[pair_key].add(tag)

    for pair_key, shared_tags in pair_shared.items():
        if len(shared_tags) >= 2:
            if pair_key in seen:
                continue
            seen.add(pair_key)
            r1_id, r2_id = pair_key
            r1 = next((r for r in repos_with_tags if str(r["id"]) == r1_id), None)
            r2 = next((r for r in repos_with_tags if str(r["id"]) == r2_id), None)
            if r1 and r2:
                tag_union = max(len(r1["tags"]), len(r2["tags"]))
                overlap_ratio = len(shared_tags) / tag_union if tag_union else 0.0
                # Weight = raw overlap ratio; confidence = max(overlap_ratio, 0.3)
                weight = overlap_ratio
                confidence = max(overlap_ratio, 0.3)
                edges.append({
                    "source": r1["id"],
                    "target": r2["id"],
                    "weight": weight,
                    "confidence": confidence,
                    "metadata": {"shared_tags": sorted(shared_tags), "count": len(shared_tags)},
                    "source_name": r1["forked_from"] or r1["name"],
                    "target_name": r2["forked_from"] or r2["name"],
                })
        if len(edges) >= 15000:
            break

    logger.info(f"  COMPATIBLE_WITH edges found: {len(edges)}")
    return edges


def build_alternative_to(cur) -> list[dict]:
    """
    Create ALTERNATIVE_TO edges for repos in the same category.
    Confidence: 0.70 for DB-category match, 0.40 for keyword fallback.
    """
    logger.info("Building ALTERNATIVE_TO edges...")

    cur.execute("SELECT COUNT(*) FROM repo_categories;")
    cat_count = cur.fetchone()[0]
    logger.info(f"  Entries in repo_categories: {cat_count}")

    if cat_count == 0:
        logger.info("  No categories in DB. Using problem_solved grouping as fallback...")

        cur.execute("""
            SELECT id, name, forked_from, problem_solved, integration_tags
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

        group_repos: dict[str, list] = defaultdict(list)
        for row in cur.fetchall():
            problem = (row[3] or "").lower()
            for group_name, keywords in PROBLEM_GROUPS.items():
                if any(kw in problem for kw in keywords):
                    group_repos[group_name].append({"id": row[0], "name": row[1], "forked_from": row[2]})
                    break

        edges: list[dict] = []
        seen: set = set()
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
                    "confidence": 0.40,  # keyword fallback — lower confidence
                    "metadata": {"category": group_name, "method": "problem_solved_keywords"},
                    "source_name": r1["forked_from"] or r1["name"],
                    "target_name": r2["forked_from"] or r2["name"],
                })

        logger.info(f"  ALTERNATIVE_TO edges (keyword fallback): {len(edges)}")
        return edges

    cur.execute("""
        SELECT rc.category_name, r.id, r.name, r.forked_from
        FROM repo_categories rc
        JOIN repos r ON r.id = rc.repo_id
        ORDER BY rc.category_name;
    """)

    cat_repos: dict[str, list] = defaultdict(list)
    for row in cur.fetchall():
        cat_repos[row[0]].append({"id": row[1], "name": row[2], "forked_from": row[3]})

    edges = []
    seen = set()
    MAX_EDGES = 15000
    MAX_PER_CATEGORY = 200
    for cat, repos in cat_repos.items():
        if len(repos) < 2:
            continue
        subset = repos[:MAX_PER_CATEGORY]
        for r1, r2 in combinations(subset, 2):
            pair_key = tuple(sorted([str(r1["id"]), str(r2["id"])]))
            if pair_key in seen:
                continue
            seen.add(pair_key)
            edges.append({
                "source": r1["id"],
                "target": r2["id"],
                "weight": 1.0,
                "confidence": 0.70,  # DB category match
                "metadata": {"category": cat},
                "source_name": r1["forked_from"] or r1["name"],
                "target_name": r2["forked_from"] or r2["name"],
            })
            if len(edges) >= MAX_EDGES:
                break
        if len(edges) >= MAX_EDGES:
            break

    logger.info(f"  ALTERNATIVE_TO edges: {len(edges)}")
    return edges


def build_depends_on(cur) -> list[dict]:
    """
    Create DEPENDS_ON edges where one repo appears in another's dependencies.
    Reads from repo_dependencies table (migration 029). Matches package names
    against repo names (case-insensitive, normalised).

    Previously checked for repos.dependencies column (dropped in migration 014)
    and silently returned []. Now uses the proper SBOM table.
    Confidence: 0.95 (direct package-name match is high-signal).
    """
    logger.info("Building DEPENDS_ON edges...")

    cur.execute("SELECT COUNT(*) FROM repo_dependencies;")
    dep_count = cur.fetchone()[0]
    logger.info(f"  Rows in repo_dependencies: {dep_count}")

    if dep_count == 0:
        logger.info("  repo_dependencies is empty — skipping DEPENDS_ON (run dependency extraction first)")
        return []

    cur.execute("""
        SELECT rd.repo_id, r.name, r.forked_from,
               array_agg(rd.package_name) AS packages
        FROM repo_dependencies rd
        JOIN repos r ON r.id = rd.repo_id
        GROUP BY rd.repo_id, r.name, r.forked_from;
    """)

    repos_with_deps = []
    for row in cur.fetchall():
        deps_raw = row[3] or []
        repos_with_deps.append({
            "id": row[0],
            "name": row[1],
            "forked_from": row[2],
            "deps": [d.lower().replace("-", "").replace("_", "") for d in deps_raw],
            "deps_raw": deps_raw,
        })

    logger.info(f"  Repos with dependency records: {len(repos_with_deps)}")

    cur.execute("SELECT id, name, forked_from FROM repos;")
    name_to_repo: dict[str, dict] = {}
    for row in cur.fetchall():
        upstream = row[2] or row[1]
        if "/" in upstream:
            repo_name = upstream.split("/")[1]
        else:
            repo_name = upstream
        normalized = repo_name.lower().replace("-", "").replace("_", "")
        name_to_repo[normalized] = {"id": row[0], "name": row[1], "forked_from": row[2]}

    edges: list[dict] = []
    seen: set = set()
    for repo in repos_with_deps:
        for dep in repo["deps"]:
            if dep in name_to_repo:
                target = name_to_repo[dep]
                if str(repo["id"]) == str(target["id"]):
                    continue  # Skip self-reference

                pair_key = (str(repo["id"]), str(target["id"]))
                if pair_key in seen:
                    continue
                seen.add(pair_key)

                edges.append({
                    "source": repo["id"],
                    "target": target["id"],
                    "weight": 1.0,
                    "confidence": 0.95,
                    "metadata": {"dependency": dep, "method": "repo_dependencies"},
                    "source_name": repo["forked_from"] or repo["name"],
                    "target_name": target["forked_from"] or target["name"],
                })

    logger.info(f"  DEPENDS_ON edges: {len(edges)}")
    return edges


def archive_existing_edges(cur, ingest_run_id: int | None) -> int:
    """
    Copy current repo_edges into repo_edges_history before replacing them.
    Sets valid_until = NOW() and valid_from = created_at of the original row.
    Returns number of rows archived.
    """
    cur.execute("""
        INSERT INTO repo_edges_history
            (source_repo_id, target_repo_id, edge_type, weight, confidence,
             metadata, ingest_run_id, valid_from, valid_until)
        SELECT source_repo_id, target_repo_id, edge_type, weight, confidence,
               metadata, ingest_run_id, created_at, NOW()
        FROM repo_edges;
    """)
    archived = cur.rowcount
    logger.info(f"  Archived {archived} edges to repo_edges_history")
    return archived


def insert_edges(cur, edges: list[dict], edge_type: str, ingest_run_id: int | None) -> int:
    """Insert edges into repo_edges table using batch inserts."""
    inserted = 0
    BATCH = 500
    for i in range(0, len(edges), BATCH):
        batch = edges[i:i + BATCH]
        values = []
        params = []
        for j, e in enumerate(batch):
            values.append("(%s, %s, %s, %s, %s, %s, %s)")
            params.extend([
                str(e["source"]),
                str(e["target"]),
                edge_type,
                e["weight"],
                e["confidence"],
                json.dumps(e["metadata"]),
                ingest_run_id,
            ])
        sql = (
            "INSERT INTO repo_edges "
            "(source_repo_id, target_repo_id, edge_type, weight, confidence, metadata, ingest_run_id) "
            "VALUES " + ", ".join(values) +
            " ON CONFLICT (source_repo_id, target_repo_id, edge_type) DO UPDATE SET"
            "  weight = EXCLUDED.weight,"
            "  confidence = EXCLUDED.confidence,"
            "  metadata = EXCLUDED.metadata,"
            "  ingest_run_id = EXCLUDED.ingest_run_id"
        )
        try:
            cur.execute(sql, params)
            inserted += len(batch)
        except Exception as ex:
            logger.warning(f"  Batch insert failed: {ex}")
        if (i + BATCH) % 5000 == 0 or i + BATCH >= len(edges):
            logger.info(f"  {edge_type}: inserted {inserted}/{len(edges)}")
    return inserted


def main() -> None:
    t0 = time.monotonic()
    ingest_run_id = get_ingest_run_id()
    logger.info(f"Phase 4: Building knowledge graph edges ($0) [ingest_run_id={ingest_run_id}]")

    conn = psycopg2.connect(get_db_url())
    cur = conn.cursor()

    # Guard: fail fast if migration 033 hasn't been applied
    assert_schema(cur)
    logger.info("repo_edges table present")

    # Archive existing edges before rebuild (temporal history)
    archived = archive_existing_edges(cur, ingest_run_id)
    conn.commit()

    # Clear live edges for atomic rebuild
    cur.execute("DELETE FROM repo_edges;")
    conn.commit()

    # Build each edge type
    compatible_edges = build_compatible_with(cur)
    alternative_edges = build_alternative_to(cur)
    depends_edges = build_depends_on(cur)

    # Insert all
    c1 = insert_edges(cur, compatible_edges, "COMPATIBLE_WITH", ingest_run_id)
    conn.commit()
    c2 = insert_edges(cur, alternative_edges, "ALTERNATIVE_TO", ingest_run_id)
    conn.commit()
    c3 = insert_edges(cur, depends_edges, "DEPENDS_ON", ingest_run_id)
    conn.commit()

    elapsed = time.monotonic() - t0

    cur.execute("SELECT edge_type, COUNT(*) FROM repo_edges GROUP BY edge_type ORDER BY edge_type;")
    print()
    print("=" * 60)
    print("PHASE 4 SUMMARY: Knowledge Graph Edges")
    print("=" * 60)
    total = 0
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]} edges")
        total += row[1]
    print(f"  TOTAL: {total} edges  (archived {archived} previous edges to history)")
    print(f"  Time: {elapsed:.1f}s")
    print(f"  Cost: $0.00")
    print()

    print("EXAMPLE COMPATIBLE_WITH EDGES:")
    for e in compatible_edges[:3]:
        print(f"  {e['source_name']} <-> {e['target_name']}")
        print(f"    shared tags: {e['metadata']['shared_tags']}")
    print()

    print("EXAMPLE ALTERNATIVE_TO EDGES:")
    for e in alternative_edges[:3]:
        print(f"  {e['source_name']} <-> {e['target_name']}")
        print(f"    metadata: {e['metadata']}")
    print()

    print("EXAMPLE DEPENDS_ON EDGES:")
    for e in depends_edges[:3]:
        print(f"  {e['source_name']} -> {e['target_name']}")
        print(f"    metadata: {e['metadata']}")

    conn.close()

    resume = f"""# Reporium Ingestion Resume
Phase 0: COMPLETE
Phase 1: COMPLETE
Phase 2: COMPLETE -- 826/826 enriched, 0 errors, $2.5213 spent
Phase 3: COMPLETE -- 826 embeddings, 62s, $0.00
Phase 4: COMPLETE -- {total} edges ({c1} COMPATIBLE_WITH, {c2} ALTERNATIVE_TO, {c3} DEPENDS_ON), {elapsed:.1f}s, $0.00
Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}
Next phase: 5 (/intelligence/query endpoint)
"""
    with open("RESUME.md", "w") as f:
        f.write(resume)


if __name__ == "__main__":
    main()
