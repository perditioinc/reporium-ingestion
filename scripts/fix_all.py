"""FIX 2+3+4: Backfill junction tables, fix categories, fix null descriptions."""
import json
import os
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import psycopg2

DB_URL = os.environ["DB_URL"]
conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

# Load library.json for rich data
with open("C:/DEV/PERDITIO_PLATFORM/reporium/public/data/library.json", encoding="utf-8") as f:
    lib = json.load(f)
lib_by_name = {r["fullName"]: r for r in lib["repos"]}

# Get all repos
cur.execute("SELECT id, name, owner, forked_from, integration_tags, primary_language FROM repos;")
db_repos = cur.fetchall()
print(f"DB repos: {len(db_repos)}")

backfilled = {"categories": 0, "tags": 0, "builders": 0, "pm_skills": 0, "industries": 0}

CATEGORY_PM_MAP = {
    "AI Agents": ["Product Discovery", "User Experience"],
    "Inference & Serving": ["Scale & Reliability", "Cost & Efficiency"],
    "RAG & Retrieval": ["Product Discovery", "Data & Evaluation"],
    "Dev Tools & Automation": ["Developer Platform"],
    "MLOps & Infrastructure": ["Scale & Reliability", "Developer Platform"],
    "Model Training": ["Data & Evaluation", "Cost & Efficiency"],
    "Computer Vision": ["Product Discovery"],
    "Foundation Models": ["Scale & Reliability"],
    "Learning Resources": ["Product Discovery"],
    "Security & Safety": ["Scale & Reliability"],
}

CATEGORY_INDUSTRY_MAP = {
    "AI Agents": ["Developer Tools", "Enterprise Software"],
    "Inference & Serving": ["Developer Tools", "Cloud Infrastructure"],
    "RAG & Retrieval": ["Developer Tools", "Enterprise Software"],
    "Dev Tools & Automation": ["Developer Tools"],
    "MLOps & Infrastructure": ["Cloud Infrastructure", "Developer Tools"],
    "Computer Vision": ["Healthcare", "Robotics"],
    "Foundation Models": ["Research & Academia", "Developer Tools"],
}

for rid, name, owner, forked_from, integration_tags, lang in db_repos:
    rid_str = str(rid)
    full_name = f"{owner}/{name}"
    lib_repo = lib_by_name.get(full_name)

    # --- Builders ---
    cur.execute("SELECT COUNT(*) FROM repo_builders WHERE repo_id = %s;", (rid_str,))
    if cur.fetchone()[0] == 0:
        if lib_repo and lib_repo.get("builders"):
            for b in lib_repo["builders"]:
                cur.execute(
                    "INSERT INTO repo_builders (repo_id, login, display_name, org_category, is_known_org) "
                    "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;",
                    (rid_str, b["login"], b.get("name") or b["login"],
                     b.get("orgCategory", "individual"), b.get("isKnownOrg", False)),
                )
                backfilled["builders"] += 1
        elif forked_from:
            upstream_owner = forked_from.split("/")[0] if "/" in forked_from else forked_from
            cur.execute(
                "INSERT INTO repo_builders (repo_id, login, display_name, org_category, is_known_org) "
                "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;",
                (rid_str, upstream_owner, upstream_owner, "individual", False),
            )
            backfilled["builders"] += 1

    # --- Tags ---
    cur.execute("SELECT COUNT(*) FROM repo_tags WHERE repo_id = %s;", (rid_str,))
    if cur.fetchone()[0] == 0:
        if lib_repo and lib_repo.get("enrichedTags"):
            for tag in lib_repo["enrichedTags"]:
                cur.execute(
                    "INSERT INTO repo_tags (repo_id, tag) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
                    (rid_str, tag),
                )
                backfilled["tags"] += 1
        elif integration_tags:
            tags_list = integration_tags if isinstance(integration_tags, list) else []
            for tag in tags_list:
                cur.execute(
                    "INSERT INTO repo_tags (repo_id, tag) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
                    (rid_str, tag),
                )
                backfilled["tags"] += 1

    # --- Categories ---
    cur.execute("SELECT COUNT(*) FROM repo_categories WHERE repo_id = %s;", (rid_str,))
    if cur.fetchone()[0] == 0:
        if lib_repo and lib_repo.get("allCategories"):
            for i, cat in enumerate(lib_repo["allCategories"]):
                cur.execute(
                    "INSERT INTO repo_categories (repo_id, category_id, category_name, is_primary) "
                    "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;",
                    (rid_str, cat.lower().replace(" ", "-").replace("&", "and"), cat, i == 0),
                )
                backfilled["categories"] += 1
        else:
            cur.execute(
                "INSERT INTO repo_categories (repo_id, category_id, category_name, is_primary) "
                "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;",
                (rid_str, "dev-tools-automation", "Dev Tools & Automation", True),
            )
            backfilled["categories"] += 1

    # --- PM Skills ---
    cur.execute("SELECT COUNT(*) FROM repo_pm_skills WHERE repo_id = %s;", (rid_str,))
    if cur.fetchone()[0] == 0:
        if lib_repo and lib_repo.get("pmSkills"):
            for skill in lib_repo["pmSkills"]:
                cur.execute(
                    "INSERT INTO repo_pm_skills (repo_id, skill) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
                    (rid_str, skill),
                )
                backfilled["pm_skills"] += 1
        else:
            cur.execute(
                "SELECT category_name FROM repo_categories WHERE repo_id = %s AND is_primary = true LIMIT 1;",
                (rid_str,),
            )
            cat_row = cur.fetchone()
            if cat_row:
                for skill in CATEGORY_PM_MAP.get(cat_row[0], ["Developer Platform"]):
                    cur.execute(
                        "INSERT INTO repo_pm_skills (repo_id, skill) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
                        (rid_str, skill),
                    )
                    backfilled["pm_skills"] += 1

    # --- Industries ---
    cur.execute("SELECT COUNT(*) FROM repo_industries WHERE repo_id = %s;", (rid_str,))
    if cur.fetchone()[0] == 0:
        if lib_repo and lib_repo.get("industries"):
            for ind in lib_repo["industries"]:
                cur.execute(
                    "INSERT INTO repo_industries (repo_id, industry) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
                    (rid_str, ind),
                )
                backfilled["industries"] += 1
        else:
            cur.execute(
                "SELECT category_name FROM repo_categories WHERE repo_id = %s AND is_primary = true LIMIT 1;",
                (rid_str,),
            )
            cat_row = cur.fetchone()
            if cat_row:
                for ind in CATEGORY_INDUSTRY_MAP.get(cat_row[0], ["Developer Tools"]):
                    cur.execute(
                        "INSERT INTO repo_industries (repo_id, industry) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
                        (rid_str, ind),
                    )
                    backfilled["industries"] += 1

conn.commit()
print(f"Backfilled: {backfilled}")

# FIX 4: Fix null descriptions
print("\n=== FIX 4: Fix null descriptions ===")
cur.execute("SELECT id, name, readme_summary FROM repos WHERE description IS NULL OR description = '';")
null_desc = cur.fetchall()
print(f"Repos with null description: {len(null_desc)}")
fixed = 0
for rid, name, summary in null_desc:
    if summary:
        first_sentence = summary.split(".")[0].strip() + "."
        if len(first_sentence) > 10:
            cur.execute("UPDATE repos SET description = %s WHERE id = %s;", (first_sentence[:255], str(rid)))
            fixed += 1
conn.commit()
print(f"Fixed descriptions: {fixed}")

# Verify
print("\n=== POST-FIX VERIFICATION ===")
cur.execute("""SELECT
  COUNT(CASE WHEN r.id NOT IN (SELECT DISTINCT repo_id FROM repo_categories) THEN 1 END),
  COUNT(CASE WHEN r.id NOT IN (SELECT DISTINCT repo_id FROM repo_builders) THEN 1 END),
  COUNT(CASE WHEN r.id NOT IN (SELECT DISTINCT repo_id FROM repo_tags) THEN 1 END),
  COUNT(CASE WHEN r.id NOT IN (SELECT DISTINCT repo_id FROM repo_pm_skills) THEN 1 END),
  COUNT(CASE WHEN r.id NOT IN (SELECT DISTINCT repo_id FROM repo_industries) THEN 1 END),
  COUNT(CASE WHEN description IS NULL OR description = '' THEN 1 END)
FROM repos r;""")
r = cur.fetchone()
print(f"Missing categories: {r[0]}")
print(f"Missing builders: {r[1]}")
print(f"Missing tags: {r[2]}")
print(f"Missing pm_skills: {r[3]}")
print(f"Missing industries: {r[4]}")
print(f"Null descriptions: {r[5]}")

# Category distribution on owned repos
print("\n=== Owned repo category distribution ===")
cur.execute("""
SELECT rc.category_name, COUNT(DISTINCT rc.repo_id) as cnt
FROM repo_categories rc
JOIN repos r ON r.id = rc.repo_id
WHERE r.is_fork = false AND rc.is_primary = true
GROUP BY rc.category_name ORDER BY cnt DESC;
""")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]}")

conn.close()
