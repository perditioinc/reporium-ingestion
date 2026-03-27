#!/usr/bin/env python3
"""
KAN-41 — Targeted backfill: assign primary_category + secondary_categories to all repos.

Reads existing readme_summary, description, and repo_taxonomy rows from DB
(no GitHub API calls needed). Sends a slim prompt to Claude asking only for
category assignment. Writes back additively.

Resumable: skips repos WHERE primary_category IS NOT NULL.
Batches of 10 with rate limiting.
Logs cost to COST_LOG.md every 50 repos.

Cost estimate: ~$1-2 for 1,468 repos (slim prompt ~200 input + ~40 output tokens each).

Usage:
    DATABASE_URL=<psycopg2-url> ANTHROPIC_API_KEY=<key> python scripts/backfill_primary_category.py

    # Dry-run (show first 5 prompts without calling Claude):
    DATABASE_URL=... python scripts/backfill_primary_category.py --dry-run

    # Limit to N repos (for testing):
    DATABASE_URL=... ANTHROPIC_API_KEY=... python scripts/backfill_primary_category.py --limit 20
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import anthropic
import httpx
import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

VALID_CATEGORIES = [
    "agents", "rag-retrieval", "llm-serving", "fine-tuning", "evaluation",
    "orchestration", "vector-databases", "observability", "security-safety",
    "code-generation", "data-processing", "computer-vision", "nlp-text",
    "speech-audio", "generative-media", "infrastructure",
]
VALID_CATEGORIES_SET = set(VALID_CATEGORIES)

BATCH_SIZE = 10
BATCH_DELAY = 1.0        # seconds between batches (rate limit courtesy)
CHECKPOINT_EVERY = 50    # log cost every N repos
MODEL = "claude-sonnet-4-20250514"

SLIM_PROMPT = """Given this AI/ML repository:
Name: {name}
Description: {description}
Summary: {readme_summary}
Taxonomy: {taxonomy_dims}

Assign exactly ONE primary_category from:
agents, rag-retrieval, llm-serving, fine-tuning, evaluation, orchestration, vector-databases, observability, security-safety, code-generation, data-processing, computer-vision, nlp-text, speech-audio, generative-media, infrastructure

Also assign up to 3 secondary_categories from the same list (must differ from primary).

Respond with JSON only: {{"primary_category": "...", "secondary_categories": ["..."]}}"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_db_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL not set.\n"
            "  export DATABASE_URL=$(gcloud secrets versions access latest "
            "--secret=reporium-db-url-async --project=perditio-platform | "
            "sed 's/+asyncpg//' | sed 's/?ssl=require/?sslmode=require/')"
        )
    url = url.replace("+asyncpg", "")
    url = url.replace("?ssl=require", "?sslmode=require")
    return url


def build_taxonomy_string(taxonomy_rows: list[tuple]) -> str:
    """Collapse repo_taxonomy rows into a readable string for the prompt."""
    by_dim: dict[str, list[str]] = {}
    for dim, val in taxonomy_rows:
        by_dim.setdefault(dim, []).append(val)
    parts = []
    for dim, vals in by_dim.items():
        parts.append(f"{dim}: {', '.join(vals[:5])}")  # cap at 5 per dim
    return " | ".join(parts) if parts else "none"


def build_prompt(repo: dict, taxonomy_rows: list[tuple]) -> str:
    return SLIM_PROMPT.format(
        name=f"{repo['owner']}/{repo['name']}",
        description=(repo.get("description") or "").strip()[:300],
        readme_summary=(repo.get("readme_summary") or "").strip()[:500],
        taxonomy_dims=build_taxonomy_string(taxonomy_rows),
    )


def parse_response(text: str) -> dict | None:
    """Parse Claude's JSON, validate categories, return dict or None on failure."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
        text = text.strip()
    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        logger.warning("JSON parse error: %s | raw: %r", e, text[:200])
        return None

    primary = data.get("primary_category", "").strip().lower()
    if primary not in VALID_CATEGORIES_SET:
        logger.warning("Invalid primary_category %r — skipping", primary)
        return None

    raw_secondary = data.get("secondary_categories", [])
    if not isinstance(raw_secondary, list):
        raw_secondary = []
    secondary = [
        s.strip().lower() for s in raw_secondary
        if isinstance(s, str) and s.strip().lower() in VALID_CATEGORIES_SET
        and s.strip().lower() != primary
    ][:3]

    return {"primary_category": primary, "secondary_categories": secondary}


def write_cost_log(path: Path, enriched: int, total: int, errors: int,
                   input_tokens: int, output_tokens: int, start: float) -> None:
    # claude-sonnet-4: $3/M input, $15/M output
    cost_in = input_tokens / 1_000_000 * 3.0
    cost_out = output_tokens / 1_000_000 * 15.0
    elapsed = time.time() - start
    entry = (
        f"{datetime.now(timezone.utc).isoformat()} | KAN-41 category backfill | "
        f"enriched: {enriched}/{total} | errors: {errors} | "
        f"tokens: {input_tokens:,} in + {output_tokens:,} out | "
        f"cost: ${cost_in + cost_out:.4f} (in: ${cost_in:.4f} + out: ${cost_out:.4f}) | "
        f"elapsed: {elapsed:.0f}s\n"
    )
    with open(path, "a", encoding="utf-8") as f:
        f.write(entry)


# ── Main ──────────────────────────────────────────────────────────────────────

def run(dry_run: bool = False, limit: int | None = None) -> None:
    db_url = get_db_url()
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key and not dry_run:
        raise RuntimeError("ANTHROPIC_API_KEY not set")

    # SSL cert workaround: gcloud secrets on Windows appends \r; httpx needs
    # an explicit cert path. certifi is the standard fix.
    ssl_cert = os.environ.get("SSL_CERT_FILE")
    if not ssl_cert:
        try:
            import certifi
            ssl_cert = certifi.where()
        except ImportError:
            pass
    if ssl_cert:
        logger.info("Using SSL cert: %s", ssl_cert)

    conn = psycopg2.connect(db_url)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Fetch repos needing classification
    query = """
        SELECT id, name, owner, description, readme_summary
        FROM repos
        WHERE primary_category IS NULL
        ORDER BY name
    """
    if limit:
        query += f" LIMIT {int(limit)}"

    cur.execute(query)
    repos = cur.fetchall()
    total = len(repos)
    logger.info("Repos needing primary_category: %d", total)

    if total == 0:
        logger.info("All repos already classified — nothing to do.")
        conn.close()
        return

    if dry_run:
        logger.info("=== DRY RUN — showing first 5 prompts ===")
        for repo in repos[:5]:
            tax_cur = conn.cursor()
            tax_cur.execute(
                "SELECT dimension, raw_value FROM repo_taxonomy WHERE repo_id = %s",
                (str(repo["id"]),),
            )
            taxonomy_rows = tax_cur.fetchall()
            print(f"\n--- {repo['owner']}/{repo['name']} ---")
            print(build_prompt(dict(repo), taxonomy_rows))
        conn.close()
        return

    http_client = httpx.Client(verify=ssl_cert) if ssl_cert else None
    client = anthropic.Anthropic(
        api_key=api_key,
        **({"http_client": http_client} if http_client else {}),
    )

    cost_log_path = Path(__file__).parent.parent / "COST_LOG.md"
    if not cost_log_path.exists():
        cost_log_path.write_text("# Reporium Ingestion Cost Log\n\n", encoding="utf-8")

    enriched = 0
    errors = 0
    error_repos: list[str] = []
    input_tokens = 0
    output_tokens = 0
    start = time.time()

    # Use a plain cursor for taxonomy lookups (non-dict)
    tax_cur = conn.cursor()
    write_cur = conn.cursor()

    for i, repo in enumerate(repos):
        repo_name = f"{repo['owner']}/{repo['name']}"

        # Fetch taxonomy context for this repo
        tax_cur.execute(
            "SELECT dimension, raw_value FROM repo_taxonomy WHERE repo_id = %s",
            (str(repo["id"]),),
        )
        taxonomy_rows = tax_cur.fetchall()

        prompt = build_prompt(dict(repo), taxonomy_rows)

        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=120,
                messages=[{"role": "user", "content": prompt}],
            )
            input_tokens += response.usage.input_tokens
            output_tokens += response.usage.output_tokens

            result = parse_response(response.content[0].text)
            if result is None:
                errors += 1
                error_repos.append(repo_name)
                logger.warning("Classification failed for %s", repo_name)
                continue

            write_cur.execute(
                """UPDATE repos
                   SET primary_category = %s,
                       secondary_categories = %s
                   WHERE id = %s""",
                (
                    result["primary_category"],
                    result["secondary_categories"] or None,
                    repo["id"],
                ),
            )
            conn.commit()
            enriched += 1
            logger.debug(
                "%s → %s (secondary: %s)",
                repo_name, result["primary_category"], result["secondary_categories"]
            )

        except anthropic.RateLimitError:
            logger.warning("Rate limit hit — sleeping 10s")
            time.sleep(10)
            errors += 1
            error_repos.append(repo_name)
            conn.rollback()

        except anthropic.APIError as e:
            logger.warning("Claude API error for %s: %s", repo_name, e)
            errors += 1
            error_repos.append(repo_name)
            conn.rollback()
            time.sleep(2)

        except Exception as e:
            logger.warning("Unexpected error for %s: %s", repo_name, e)
            errors += 1
            error_repos.append(repo_name)
            conn.rollback()

        # Batch delay every BATCH_SIZE repos
        if (i + 1) % BATCH_SIZE == 0:
            time.sleep(BATCH_DELAY)

        # Checkpoint every CHECKPOINT_EVERY repos
        if (i + 1) % CHECKPOINT_EVERY == 0 or (i + 1) == total:
            cost_in = input_tokens / 1_000_000 * 3.0
            cost_out = output_tokens / 1_000_000 * 15.0
            logger.info(
                "Progress: %d/%d classified | errors: %d | cost so far: $%.4f",
                enriched, total, errors, cost_in + cost_out,
            )
            write_cost_log(cost_log_path, enriched, total, errors,
                           input_tokens, output_tokens, start)

    conn.close()

    cost_in = input_tokens / 1_000_000 * 3.0
    cost_out = output_tokens / 1_000_000 * 15.0
    elapsed = time.time() - start
    logger.info(
        "COMPLETE: %d/%d classified | %d errors | $%.4f total | %.0fs elapsed",
        enriched, total, errors, cost_in + cost_out, elapsed,
    )
    if error_repos:
        logger.warning("Failed repos (%d): %s", len(error_repos), ", ".join(error_repos[:20]))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="KAN-41: Backfill primary_category")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print prompts without calling Claude")
    parser.add_argument("--limit", type=int, default=None,
                        help="Only process N repos (for testing)")
    args = parser.parse_args()
    run(dry_run=args.dry_run, limit=args.limit)
