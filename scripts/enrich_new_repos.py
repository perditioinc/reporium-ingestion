"""
Ingest newly forked perditioinc repos into the Reporium database.

Phases:
  0. Find diff: gh repo list perditioinc → compare against DB
  1. Insert new repos into repos table
  2. Extract dependencies (requirements.txt / package.json / pyproject.toml)
  3. AI enrichment via Claude API (readme_summary, problem_solved, categories, integration_tags)
  4. Generate embeddings (sentence-transformers all-MiniLM-L6-v2)
  5. Rebuild knowledge graph edges

Usage:
    python scripts/enrich_new_repos.py
    python scripts/enrich_new_repos.py --dry-run   # show diff, don't insert
"""

import json
import logging
import os
import re
import subprocess
import sys
import time
import io
from datetime import datetime, timezone
from pathlib import Path

# Fix Windows console encoding
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

DRY_RUN = "--dry-run" in sys.argv

OWNER = "perditioinc"
MODEL = "claude-sonnet-4-20250514"
GCP_PROJECT = os.getenv("GCP_PROJECT", "perditio-platform")


# ── Secrets / DB ─────────────────────────────────────────────────────────────

def get_secret(secret_id: str) -> str:
    """Fetch a secret from GCP Secret Manager."""
    from google.cloud import secretmanager
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{GCP_PROJECT}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8").strip()


def normalize_db_url(url: str) -> str:
    """
    Convert an asyncpg-style URL to a psycopg2-compatible one.
    - Strip +asyncpg driver suffix
    - Strip asyncpg query params (ssl, sslmode with asyncpg values)
    - Keep sslmode if it's a valid psycopg2 value
    """
    import urllib.parse

    # Strip async driver prefix (e.g. postgresql+asyncpg://...)
    url = url.replace("+asyncpg", "")
    if url.startswith("postgresql+"):
        url = "postgresql" + url[url.index("://"):]

    # Parse and sanitize query string
    parsed = urllib.parse.urlsplit(url)
    params = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)

    # asyncpg uses ?ssl=True or ?ssl=require; psycopg2 uses sslmode=require
    ssl_val = params.pop("ssl", [None])[0]
    if ssl_val and "sslmode" not in params:
        # Map asyncpg ssl values to psycopg2 sslmode
        if ssl_val.lower() in ("true", "1", "require"):
            params["sslmode"] = ["require"]
        elif ssl_val.lower() in ("false", "0", "disable"):
            params["sslmode"] = ["disable"]

    new_query = urllib.parse.urlencode(
        {k: v[0] for k, v in params.items()}, safe=""
    )
    clean = urllib.parse.urlunsplit(parsed._replace(query=new_query))
    return clean


def get_db_url() -> str:
    url = os.getenv("DATABASE_URL", "").strip()
    if url:
        return normalize_db_url(url)
    try:
        url = get_secret("reporium-db-url-async")
        return normalize_db_url(url)
    except Exception as e:
        raise RuntimeError(f"No DATABASE_URL found in env or Secret Manager: {e}")


def get_anthropic_key() -> str:
    key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if key:
        return key
    try:
        return get_secret("anthropic-api-key")
    except Exception as e:
        raise RuntimeError(f"No ANTHROPIC_API_KEY found: {e}")


def get_gh_token() -> str:
    """Get GitHub token via gh CLI."""
    result = subprocess.run(
        ["gh", "auth", "token"], capture_output=True, text=True
    )
    if result.returncode == 0:
        return result.stdout.strip()
    return os.getenv("GH_TOKEN", os.getenv("GITHUB_TOKEN", ""))


# ── Phase 0: Find diff ────────────────────────────────────────────────────────

def list_gh_repos() -> list[dict]:
    """List all perditioinc repos via gh CLI (user account, not org)."""
    logger.info(f"Listing repos for user {OWNER} via gh CLI...")
    result = subprocess.run(
        [
            "gh", "repo", "list", OWNER,
            "--limit", "2000",
            "--json", "name,description,primaryLanguage,isFork,parent,url,nameWithOwner",
        ],
        capture_output=True,  # capture as bytes to handle any encoding
    )
    if result.returncode != 0:
        raise RuntimeError(f"gh repo list failed: {result.stderr.decode('utf-8', errors='replace')}")
    repos = json.loads(result.stdout.decode("utf-8", errors="replace"))
    logger.info(f"  gh returned {len(repos)} repos for {OWNER}")
    return repos


def get_db_repo_names(cur) -> set[str]:
    """Return set of (lowercase) repo names currently in the DB."""
    cur.execute("SELECT name FROM repos WHERE owner = %s;", (OWNER,))
    return {row[0].lower() for row in cur.fetchall()}


def find_new_repos(gh_repos: list[dict], existing_names: set[str]) -> list[dict]:
    """Return repos from gh that are not yet in the DB."""
    new = []
    for r in gh_repos:
        if r["name"].lower() not in existing_names:
            new.append(r)
    return new


# ── Phase 1: Insert repos ─────────────────────────────────────────────────────

def safe_name(conn, cur, base_name: str) -> str:
    """
    Return a name that doesn't conflict with existing DB names.
    Appends _ suffix until unique (up to 5 attempts).
    """
    name = base_name
    for _ in range(5):
        cur.execute("SELECT 1 FROM repos WHERE name = %s;", (name,))
        if cur.fetchone() is None:
            return name
        name = name + "_"
    return name


def insert_repos(conn, cur, new_repos: list[dict]) -> list[dict]:
    """
    Insert new repos into the repos table.
    Returns list of dicts with inserted id/name for subsequent phases.
    """
    logger.info(f"Phase 1: Inserting {len(new_repos)} new repos...")
    inserted = []
    skipped = 0

    for r in new_repos:
        base_name = r["name"]
        name = safe_name(conn, cur, base_name)

        # Build forked_from from parent info
        parent = r.get("parent") or {}
        forked_from = parent.get("nameWithOwner") or parent.get("fullName") or None

        description = (r.get("description") or "")[:500] or None
        primary_language = (r.get("primaryLanguage") or {}).get("name") or None
        is_fork = r.get("isFork", True)
        github_url = f"https://github.com/{OWNER}/{base_name}"

        try:
            cur.execute(
                """INSERT INTO repos
                   (name, owner, description, primary_language, forked_from,
                    is_fork, github_url, ingested_at, updated_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                   RETURNING id;""",
                (name, OWNER, description, primary_language, forked_from,
                 is_fork, github_url),
            )
            row = cur.fetchone()
            conn.commit()
            inserted.append({
                "id": str(row[0]),
                "name": name,
                "owner": OWNER,
                "description": description,
                "primary_language": primary_language,
                "forked_from": forked_from,
                "github_url": github_url,
                "gh_name": base_name,  # original name from gh (for API calls)
            })
        except Exception as e:
            conn.rollback()
            logger.warning(f"  Failed to insert {base_name}: {e}")
            skipped += 1

    logger.info(f"  Inserted: {len(inserted)}, skipped/errors: {skipped}")
    return inserted


# ── Phase 2: Extract dependencies ────────────────────────────────────────────

def fetch_file_from_github(token: str, owner: str, repo: str, path: str) -> str | None:
    """Fetch raw file content from GitHub API. Returns None if not found."""
    import urllib.request
    import urllib.error
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github.v3.raw",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        if e.code in (404, 403):
            return None
        logger.debug(f"    HTTP {e.code} fetching {owner}/{repo}/{path}")
        return None
    except Exception as e:
        logger.debug(f"    Error fetching {owner}/{repo}/{path}: {e}")
        return None


def parse_requirements_txt(content: str) -> list[str]:
    deps = []
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("-"):
            continue
        # Strip version specifiers
        pkg = re.split(r"[>=<!~\s\[;]", line)[0].strip()
        if pkg:
            deps.append(pkg)
    return deps[:50]


def parse_package_json(content: str) -> list[str]:
    try:
        data = json.loads(content)
    except Exception:
        return []
    deps = {}
    deps.update(data.get("dependencies", {}))
    deps.update(data.get("devDependencies", {}))
    return list(deps.keys())[:50]


def parse_pyproject_toml(content: str) -> list[str]:
    deps = []
    in_deps = False
    for line in content.splitlines():
        stripped = line.strip()
        if stripped in ("[project]", "[tool.poetry.dependencies]", "[tool.poetry.dev-dependencies]"):
            in_deps = True
            continue
        if stripped.startswith("[") and in_deps:
            in_deps = False
        if in_deps and "=" in stripped and not stripped.startswith("#"):
            pkg = stripped.split("=")[0].strip().strip('"').strip("'")
            if pkg and pkg != "python":
                deps.append(pkg)
    return deps[:50]


def extract_dependencies(token: str, repo: dict) -> list[str]:
    """Try to extract dependency names from common manifest files."""
    name = repo.get("gh_name") or repo["name"]
    deps: list[str] = []

    for path, parser in [
        ("requirements.txt", parse_requirements_txt),
        ("package.json", parse_package_json),
        ("pyproject.toml", parse_pyproject_toml),
    ]:
        content = fetch_file_from_github(token, OWNER, name, path)
        if content:
            parsed = parser(content)
            if parsed:
                deps.extend(parsed)
                logger.debug(f"    {name}/{path}: {len(parsed)} deps")
            break  # use first manifest found

    return list(dict.fromkeys(deps))  # dedupe, preserve order


def run_dependency_extraction(conn, cur, repos: list[dict], token: str) -> None:
    """Phase 2: Extract and save dependencies for new repos."""
    logger.info(f"Phase 2 (deps): Extracting dependencies for {len(repos)} repos...")
    updated = 0

    for i, repo in enumerate(repos, 1):
        deps = extract_dependencies(token, repo)
        if deps:
            try:
                cur.execute(
                    "UPDATE repos SET dependencies = %s::jsonb WHERE id = %s;",
                    (json.dumps(deps), repo["id"]),
                )
                conn.commit()
                repo["dependencies"] = deps
                updated += 1
            except Exception as e:
                conn.rollback()
                logger.warning(f"  Failed to save deps for {repo['name']}: {e}")

        if i % 25 == 0:
            logger.info(f"  Deps progress: {i}/{len(repos)} ({updated} with deps)")

    logger.info(f"  Dependencies extracted: {updated}/{len(repos)} repos had deps")


# ── Phase 3: AI enrichment ────────────────────────────────────────────────────

ENRICHMENT_PROMPT = """Analyze this AI/ML GitHub repository and return a JSON object with these fields:

Repository information:
{repo_context}

{{
  "readme_summary": "2-3 sentence plain language description of what this repo does and who uses it",
  "problem_solved": "1 sentence: what specific problem does this solve",
  "quality_assessment": "high|medium|low — based on documentation quality, activity, and stars",
  "maturity_level": "research|prototype|beta|production",
  "skill_areas": ["list of AI/ML expertise domains this repo demonstrates or requires — be specific and descriptive, generate as many as apply, e.g. 'Retrieval-Augmented Generation', 'LoRA Fine-tuning', 'Transformer Architecture'"],
  "industries": ["industry verticals or domains this applies to — e.g. 'Healthcare', 'FinTech', 'Legal Tech', 'Developer Tools', 'Education', 'Robotics' — only include if genuinely applicable, omit if general-purpose"],
  "use_cases": ["specific problems or applications this solves — e.g. 'Document Question Answering', 'Code Review Automation', 'Real-time Voice Transcription' — be concrete"],
  "modalities": ["data types this works with — e.g. 'Text', 'Code', 'Image', 'Audio', 'Video', 'Tabular', 'Multimodal', '3D'"],
  "ai_trends": ["current AI movements or paradigms this relates to — e.g. 'Agentic AI', 'Small Language Models', 'Compound AI Systems', 'AI Safety', 'Multimodal Reasoning', 'On-device AI'"],
  "deployment_context": ["where/how this runs — e.g. 'Cloud API', 'Self-hosted', 'Edge/Mobile', 'Browser/WASM', 'On-premise', 'Serverless'"],
  "integration_tags": ["specific frameworks, libraries, tools used — e.g. 'langchain', 'pytorch', 'huggingface', 'vllm', 'fastapi' — lowercase, specific"]
}}

Rules:
- Generate as many values per field as genuinely apply — don't artificially limit
- All values must be based on evidence in the README/description — no speculation
- integration_tags: lowercase, specific library/tool names only
- industries: omit entirely if the repo is general-purpose AI infrastructure
- Return ONLY valid JSON, no markdown"""

VALID_CATEGORIES = {
    "agents", "llm-serving", "embeddings", "vector-databases", "evaluation",
    "fine-tuning", "rag", "orchestration", "observability", "data-processing",
    "ocr", "vision", "audio", "code-generation", "security", "deployment",
    "tooling", "datasets", "research", "other",
}


def build_repo_context(repo: dict) -> str:
    parts = [
        f"Name: {repo['owner']}/{repo['name']}",
        f"Description: {repo.get('description') or 'None'}",
        f"Primary Language: {repo.get('primary_language') or 'Unknown'}",
    ]
    if repo.get("forked_from"):
        parts.append(f"Forked from: {repo['forked_from']}")
    deps = repo.get("dependencies")
    if deps:
        parts.append(f"Dependencies: {', '.join(deps[:20])}")
    return "\n".join(parts)


def _clean_list(values: list) -> list[str]:
    """Strip whitespace, filter empty strings, deduplicate, preserve order."""
    seen: set = set()
    result: list = []
    for v in values:
        if not isinstance(v, str):
            continue
        v = v.strip()
        if v and v not in seen:
            seen.add(v)
            result.append(v)
    return result


def parse_enrichment_response(text: str) -> dict:
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
        text = text.strip()
    data = json.loads(text)

    qa = data.get("quality_assessment", "medium")
    if qa not in ("high", "medium", "low"):
        qa = "medium"
    data["quality_assessment"] = qa

    ml = data.get("maturity_level", "")
    if ml not in ("research", "prototype", "beta", "production"):
        ml = None
    data["maturity_level"] = ml

    # Open list fields — no validation against any fixed set, no max limit
    data["skill_areas"] = _clean_list(data.get("skill_areas", []))
    data["industries"] = _clean_list(data.get("industries", []))
    data["use_cases"] = _clean_list(data.get("use_cases", []))
    data["modalities"] = _clean_list(data.get("modalities", []))
    data["ai_trends"] = _clean_list(data.get("ai_trends", []))
    data["deployment_context"] = _clean_list(data.get("deployment_context", []))

    # integration_tags — lowercase, deduplicated, no max limit
    raw_tags = data.get("integration_tags", [])
    data["integration_tags"] = _clean_list(
        [t.lower() if isinstance(t, str) else t for t in raw_tags]
    )

    return data


def run_ai_enrichment(conn, cur, repos: list[dict], api_key: str) -> dict:
    """Phase 3: Enrich each repo with Claude API."""
    logger.info(f"Phase 3 (AI): Enriching {len(repos)} repos with {MODEL}...")

    import anthropic
    client = anthropic.Anthropic(api_key=api_key)

    stats = {"enriched": 0, "errors": 0, "input_tokens": 0, "output_tokens": 0}
    t0 = time.monotonic()

    for i, repo in enumerate(repos, 1):
        repo_label = f"{repo['owner']}/{repo['name']}"
        try:
            context = build_repo_context(repo)
            prompt = ENRICHMENT_PROMPT.format(repo_context=context)

            response = client.messages.create(
                model=MODEL,
                max_tokens=800,
                messages=[{"role": "user", "content": prompt}],
            )
            stats["input_tokens"] += response.usage.input_tokens
            stats["output_tokens"] += response.usage.output_tokens

            data = parse_enrichment_response(response.content[0].text)

            # Update repos table with all taxonomy dimensions
            cur.execute(
                """UPDATE repos SET
                    readme_summary = %s,
                    problem_solved = %s,
                    integration_tags = %s::jsonb,
                    quality_assessment = %s,
                    maturity_level = %s,
                    skill_areas = %s::jsonb,
                    industries = %s::jsonb,
                    use_cases = %s::jsonb,
                    modalities = %s::jsonb,
                    ai_trends = %s::jsonb,
                    deployment_context = %s::jsonb
                WHERE id = %s;""",
                (
                    data.get("readme_summary"),
                    data.get("problem_solved"),
                    json.dumps(data.get("integration_tags", [])),
                    data.get("quality_assessment"),
                    data.get("maturity_level"),
                    json.dumps(data.get("skill_areas", [])),
                    json.dumps(data.get("industries", [])),
                    json.dumps(data.get("use_cases", [])),
                    json.dumps(data.get("modalities", [])),
                    json.dumps(data.get("ai_trends", [])),
                    json.dumps(data.get("deployment_context", [])),
                    repo["id"],
                ),
            )

            conn.commit()
            stats["enriched"] += 1

            # Store for embedding phase
            repo["readme_summary"] = data.get("readme_summary")
            repo["problem_solved"] = data.get("problem_solved")
            repo["integration_tags"] = data.get("integration_tags", [])

        except json.JSONDecodeError as e:
            stats["errors"] += 1
            logger.warning(f"  JSON parse error for {repo_label}: {e}")
            conn.rollback()
        except Exception as e:
            stats["errors"] += 1
            logger.warning(f"  Error enriching {repo_label}: {e}")
            conn.rollback()
            time.sleep(2)  # brief pause on API errors

        if i % 25 == 0 or i == len(repos):
            elapsed = time.monotonic() - t0
            input_cost = stats["input_tokens"] / 1_000_000 * 3.0
            output_cost = stats["output_tokens"] / 1_000_000 * 15.0
            logger.info(
                f"  AI progress: {i}/{len(repos)} enriched={stats['enriched']} "
                f"errors={stats['errors']} cost=${input_cost+output_cost:.4f} elapsed={elapsed:.0f}s"
            )

        time.sleep(0.3)  # rate limit courtesy

    return stats


# ── Phase 4: Embeddings ───────────────────────────────────────────────────────

def build_embedding_text(repo: dict) -> str:
    parts = []
    for field in ("name", "forked_from", "description", "readme_summary", "problem_solved"):
        val = repo.get(field)
        if val:
            parts.append(val)
    tags = repo.get("integration_tags")
    if tags:
        parts.append("integrations: " + " ".join(tags))
    deps = repo.get("dependencies")
    if deps:
        parts.append("dependencies: " + " ".join(deps[:20]))
    return " ".join(parts)[:2048]


def run_embeddings(conn, cur, repos: list[dict], db_url: str) -> int:
    """Phase 4: Generate and store embeddings for new repos."""
    logger.info(f"Phase 4 (embeddings): Generating embeddings for {len(repos)} repos...")

    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        logger.warning("sentence-transformers not installed — skipping embeddings")
        return 0

    MODEL_NAME = "all-MiniLM-L6-v2"
    BATCH_SIZE = 64

    model = SentenceTransformer(MODEL_NAME)
    logger.info(f"  Model loaded: {MODEL_NAME}")

    # Reload repos from DB to pick up all enriched fields
    ids = [r["id"] for r in repos]
    cur.execute(
        """SELECT r.id, r.name, r.forked_from, r.description,
                  r.readme_summary, r.problem_solved, r.integration_tags, r.dependencies
           FROM repos r
           WHERE r.id = ANY(%s::uuid[]);""",
        (ids,),
    )
    cols = [d[0] for d in cur.description]
    db_rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    texts = [build_embedding_text(r) for r in db_rows]
    repo_ids = [str(r["id"]) for r in db_rows]

    all_embeddings = []
    for i in range(0, len(texts), BATCH_SIZE):
        batch = texts[i:i + BATCH_SIZE]
        batch_emb = model.encode(batch, show_progress_bar=False)
        all_embeddings.extend(batch_emb)

    now = datetime.now(timezone.utc).isoformat()
    inserted = 0
    for repo_id, embedding in zip(repo_ids, all_embeddings):
        emb_json = json.dumps(embedding.tolist())
        try:
            cur.execute(
                """INSERT INTO repo_embeddings (repo_id, embedding, model, generated_at)
                   VALUES (%s, %s, %s, %s)
                   ON CONFLICT (repo_id) DO UPDATE SET
                     embedding = EXCLUDED.embedding,
                     model = EXCLUDED.model,
                     generated_at = EXCLUDED.generated_at;""",
                (repo_id, emb_json, MODEL_NAME, now),
            )
            inserted += 1
        except Exception as e:
            logger.warning(f"  Failed embedding for {repo_id}: {e}")
            conn.rollback()
            import psycopg2
            conn = psycopg2.connect(db_url)
            cur = conn.cursor()

    conn.commit()
    logger.info(f"  Embeddings generated: {inserted}/{len(repos)}")
    return inserted


# ── Phase 5: Knowledge graph ──────────────────────────────────────────────────

def rebuild_knowledge_graph(conn, cur) -> None:
    """Phase 5: Delegate to existing build_knowledge_graph script."""
    logger.info("Phase 5 (graph): Rebuilding knowledge graph edges...")
    script = Path(__file__).parent / "build_knowledge_graph.py"
    if not script.exists():
        logger.warning(f"  {script} not found — skipping knowledge graph rebuild")
        return

    db_url = conn.dsn if hasattr(conn, "dsn") else os.getenv("DATABASE_URL", "")
    # Get DSN string from connection
    try:
        dsn = conn.info.dsn if hasattr(conn, "info") else None
    except Exception:
        dsn = None

    result = subprocess.run(
        [sys.executable, str(script)],
        env={**os.environ},
        capture_output=False,
    )
    if result.returncode != 0:
        logger.warning(f"  Knowledge graph script exited with code {result.returncode}")
    else:
        logger.info("  Knowledge graph rebuild complete")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    import psycopg2

    t_start = time.monotonic()
    logger.info("=" * 60)
    logger.info("Reporium: Ingest new perditioinc repos")
    logger.info(f"Dry run: {DRY_RUN}")
    logger.info("=" * 60)

    # Get credentials
    db_url = get_db_url()
    gh_token = get_gh_token()
    logger.info(f"DB URL acquired ({'env' if os.getenv('DATABASE_URL') else 'Secret Manager'})")
    logger.info(f"GitHub token: {'set' if gh_token else 'MISSING'}")

    # Connect to DB
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    # Phase 0: find diff
    gh_repos = list_gh_repos()
    existing_names = get_db_repo_names(cur)
    new_repos_raw = find_new_repos(gh_repos, existing_names)

    logger.info(f"DB has {len(existing_names)} perditioinc repos")
    logger.info(f"gh lists {len(gh_repos)} repos for {OWNER}")
    logger.info(f"New repos to ingest: {len(new_repos_raw)}")

    if not new_repos_raw:
        logger.info("No new repos — nothing to do.")
        cur.execute("SELECT COUNT(*) FROM repos;")
        total = cur.fetchone()[0]
        logger.info(f"Total repos in DB: {total}")
        conn.close()
        return

    # Print the new repo names
    logger.info("New repos:")
    for r in new_repos_raw:
        logger.info(f"  + {r['name']}")

    if DRY_RUN:
        logger.info("Dry run — stopping before insert.")
        conn.close()
        return

    # Phase 1: Insert
    inserted_repos = insert_repos(conn, cur, new_repos_raw)
    if not inserted_repos:
        logger.error("No repos inserted — check errors above.")
        conn.close()
        return

    # Phase 2: Dependencies
    run_dependency_extraction(conn, cur, inserted_repos, gh_token)

    # Phase 3: AI enrichment
    api_key = get_anthropic_key()
    ai_stats = run_ai_enrichment(conn, cur, inserted_repos, api_key)

    # Phase 4: Embeddings
    emb_count = run_embeddings(conn, cur, inserted_repos, db_url)

    # Phase 5: Knowledge graph
    rebuild_knowledge_graph(conn, cur)

    # Final verification
    elapsed = time.monotonic() - t_start

    cur.execute("SELECT COUNT(*) FROM repos;")
    total_repos = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM repo_embeddings;")
    total_embeddings = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM repos WHERE readme_summary IS NOT NULL;")
    total_enriched = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM repos WHERE owner = %s;", (OWNER,))
    owner_count = cur.fetchone()[0]

    try:
        cur.execute("SELECT COUNT(*) FROM repo_edges;")
        edge_count = cur.fetchone()[0]
    except Exception:
        edge_count = "n/a"

    input_cost = ai_stats["input_tokens"] / 1_000_000 * 3.0
    output_cost = ai_stats["output_tokens"] / 1_000_000 * 15.0
    total_cost = input_cost + output_cost

    print()
    print("=" * 60)
    print("INGESTION COMPLETE")
    print("=" * 60)
    print(f"  New repos inserted:   {len(inserted_repos)}")
    print(f"  AI enriched:          {ai_stats['enriched']}")
    print(f"  AI errors:            {ai_stats['errors']}")
    print(f"  Embeddings generated: {emb_count}")
    print(f"  AI cost:              ${total_cost:.4f}")
    print(f"  Elapsed:              {elapsed:.0f}s")
    print()
    print("DB TOTALS:")
    print(f"  Total repos:          {total_repos}")
    print(f"  perditioinc repos:    {owner_count}")
    print(f"  Enriched repos:       {total_enriched}")
    print(f"  Total embeddings:     {total_embeddings}")
    print(f"  Knowledge graph edges:{edge_count}")
    print()

    conn.close()


if __name__ == "__main__":
    main()
