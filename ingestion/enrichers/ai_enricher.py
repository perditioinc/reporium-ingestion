"""
AI Enrichment using Claude API (claude-sonnet-4-20250514).
One call per repo. Generates 8 open taxonomy dimensions freely — no hardcoded skill lists.

Cost: ~$7-10 total for 1460 repos (~600 input + ~200 output tokens per call).
Every call is logged to COST_LOG.md. Progress saved to RESUME.md every 50 repos.

Schema alignment (2026-03-25):
  Writes to repos:       readme_summary, problem_solved, integration_tags (jsonb),
                         quality_signals (jsonb — stores quality_assessment + maturity_level)
  Writes to repo_taxonomy: one row per (repo_id, dimension, raw_value) for each of
                         skill_area, industry, use_case, modality, ai_trend, deployment_context
  Does NOT use:          repos.skill_areas / industries / etc. (columns never existed)
                         repos.dependencies (dropped in migration 014)
"""

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import anthropic
import psycopg2

logger = logging.getLogger(__name__)

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

# Taxonomy dimension mapping: Claude output field → repo_taxonomy.dimension value
_TAXONOMY_DIMENSIONS = {
    "skill_areas":          "skill_area",
    "industries":           "industry",
    "use_cases":            "use_case",
    "modalities":           "modality",
    "ai_trends":            "ai_trend",
    "deployment_context":   "deployment_context",
}


def _clean_list(values: list) -> list[str]:
    """Strip whitespace, filter empty strings, deduplicate, preserve order."""
    seen: set[str] = set()
    result: list[str] = []
    for v in values:
        if not isinstance(v, str):
            continue
        v = v.strip()
        if v and v not in seen:
            seen.add(v)
            result.append(v)
    return result


@dataclass
class EnrichmentResult:
    repo_id: str
    repo_name: str
    readme_summary: Optional[str] = None
    problem_solved: Optional[str] = None
    integration_tags: list[str] = field(default_factory=list)
    quality_assessment: str = "medium"
    maturity_level: Optional[str] = None
    skill_areas: list[str] = field(default_factory=list)
    industries: list[str] = field(default_factory=list)
    use_cases: list[str] = field(default_factory=list)
    modalities: list[str] = field(default_factory=list)
    ai_trends: list[str] = field(default_factory=list)
    deployment_context: list[str] = field(default_factory=list)
    input_tokens: int = 0
    output_tokens: int = 0
    error: Optional[str] = None


@dataclass
class RunStats:
    total: int = 0
    enriched: int = 0
    skipped: int = 0
    errors: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    start_time: float = 0.0
    error_repos: list[str] = field(default_factory=list)


def _build_repo_context(row: dict) -> str:
    """Build the context string sent to Claude for one repo."""
    parts = [
        f"Name: {row['owner']}/{row['name']}",
        f"Description: {row.get('description') or 'None'}",
        f"Primary Language: {row.get('primary_language') or 'Unknown'}",
    ]
    if row.get('forked_from'):
        parts.append(f"Forked from: {row['forked_from']}")
    return "\n".join(parts)


def _parse_enrichment_response(text: str) -> dict:
    """Parse Claude's JSON response, handling common formatting issues."""
    text = text.strip()
    # Remove markdown code fences if present
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
        text = text.strip()

    data = json.loads(text)

    # readme_summary and problem_solved — plain strings
    data["readme_summary"] = data.get("readme_summary") or None
    data["problem_solved"] = data.get("problem_solved") or None

    # quality_assessment
    qa = data.get("quality_assessment", "medium")
    if qa not in ("high", "medium", "low"):
        qa = "medium"
    data["quality_assessment"] = qa

    # maturity_level
    ml = data.get("maturity_level", "")
    if ml not in ("research", "prototype", "beta", "production"):
        ml = None
    data["maturity_level"] = ml

    # Open taxonomy list fields
    for field_name in ("skill_areas", "industries", "use_cases", "modalities", "ai_trends", "deployment_context"):
        data[field_name] = _clean_list(data.get(field_name, []))

    # integration_tags — lowercase, deduplicated
    raw_tags = data.get("integration_tags", [])
    data["integration_tags"] = _clean_list(
        [t.lower() if isinstance(t, str) else t for t in raw_tags]
    )

    return data


def _write_cost_log(path: Path, stats: RunStats) -> None:
    """Append current cost summary to COST_LOG.md."""
    # Sonnet pricing: $3/M input, $15/M output
    input_cost = stats.total_input_tokens / 1_000_000 * 3.0
    output_cost = stats.total_output_tokens / 1_000_000 * 15.0
    total_cost = input_cost + output_cost
    elapsed = time.time() - stats.start_time

    entry = (
        f"{datetime.now(timezone.utc).isoformat()} | "
        f"enriched: {stats.enriched}/{stats.total} | "
        f"errors: {stats.errors} | "
        f"input_tokens: {stats.total_input_tokens:,} | "
        f"output_tokens: {stats.total_output_tokens:,} | "
        f"cost: ${total_cost:.4f} (in: ${input_cost:.4f} + out: ${output_cost:.4f}) | "
        f"elapsed: {elapsed:.0f}s\n"
    )

    with open(path, "a", encoding="utf-8") as f:
        f.write(entry)


def _write_resume(path: Path, stats: RunStats) -> None:
    """Update RESUME.md with current progress."""
    input_cost = stats.total_input_tokens / 1_000_000 * 3.0
    output_cost = stats.total_output_tokens / 1_000_000 * 15.0
    total_cost = input_cost + output_cost

    content = f"""# Reporium Ingestion Resume
Phase 0: COMPLETE
Phase 1: COMPLETE
Phase 2: IN PROGRESS — {stats.enriched}/{stats.total} enriched, {stats.errors} errors, ${total_cost:.4f} spent
Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}
Next: continue Phase 2 from where left off (skip repos with non-null readme_summary)
"""
    path.write_text(content, encoding="utf-8")


async def run_ai_enrichment(
    db_url: str,
    api_key: str,
    model: str = "claude-sonnet-4-20250514",
    base_dir: str = ".",
) -> RunStats:
    """
    Main entry point: enrich all repos that have null readme_summary.
    Writes to repos (readme_summary, problem_solved, integration_tags, quality_signals)
    and inserts taxonomy dimensions directly into repo_taxonomy junction table.
    Writes COST_LOG.md and RESUME.md every 50 repos.
    """
    base = Path(base_dir)
    cost_log_path = base / "COST_LOG.md"
    resume_path = base / "RESUME.md"

    # Initialize cost log if first run
    if not cost_log_path.exists():
        cost_log_path.write_text("# Reporium Ingestion Cost Log\n\n", encoding="utf-8")

    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    # Get repos needing enrichment (readme_summary IS NULL).
    # Only select columns that actually exist in the production schema.
    cur.execute("""
        SELECT id, name, owner, description, primary_language, forked_from
        FROM repos
        WHERE readme_summary IS NULL
        ORDER BY name;
    """)
    columns = [d[0] for d in cur.description]
    repos = [dict(zip(columns, row)) for row in cur.fetchall()]

    stats = RunStats(total=len(repos), start_time=time.time())
    logger.info("Phase 2: %d repos need enrichment", stats.total)

    if stats.total == 0:
        logger.info("All repos already enriched — nothing to do")
        conn.close()
        return stats

    client = anthropic.Anthropic(api_key=api_key)

    for i, repo in enumerate(repos):
        repo_name = f"{repo['owner']}/{repo['name']}"
        repo_id = str(repo["id"])

        try:
            context = _build_repo_context(repo)
            prompt = ENRICHMENT_PROMPT.format(repo_context=context)

            response = client.messages.create(
                model=model,
                max_tokens=800,
                messages=[{"role": "user", "content": prompt}],
            )

            stats.total_input_tokens += response.usage.input_tokens
            stats.total_output_tokens += response.usage.output_tokens

            text = response.content[0].text
            data = _parse_enrichment_response(text)

            # ── 1. Update columns that actually exist on repos ────────────────
            # Pack quality_assessment + maturity_level into quality_signals JSONB
            quality_signals = {
                "quality": data.get("quality_assessment", "medium"),
                "maturity": data.get("maturity_level"),
            }
            cur.execute(
                """UPDATE repos SET
                    readme_summary    = %s,
                    problem_solved    = %s,
                    integration_tags  = %s::jsonb,
                    quality_signals   = %s::jsonb
                WHERE id = %s""",
                (
                    data.get("readme_summary"),
                    data.get("problem_solved"),
                    json.dumps(data.get("integration_tags", [])),
                    json.dumps(quality_signals),
                    repo["id"],
                ),
            )

            # ── 2. Write taxonomy dimensions to repo_taxonomy junction table ──
            # Delete existing enrichment rows for this repo first (safe re-run)
            cur.execute(
                "DELETE FROM repo_taxonomy WHERE repo_id = %s AND assigned_by = 'enrichment'",
                (repo_id,),
            )
            for field_name, dimension in _TAXONOMY_DIMENSIONS.items():
                for raw_value in data.get(field_name, []):
                    if not raw_value or not isinstance(raw_value, str):
                        continue
                    cur.execute(
                        """INSERT INTO repo_taxonomy (repo_id, dimension, raw_value, assigned_by)
                           VALUES (%s, %s, %s, 'enrichment')
                           ON CONFLICT (repo_id, dimension, raw_value) DO NOTHING""",
                        (repo_id, dimension, raw_value.strip()),
                    )

            conn.commit()
            stats.enriched += 1

        except json.JSONDecodeError as e:
            stats.errors += 1
            stats.error_repos.append(repo_name)
            logger.warning("JSON parse error for %s: %s", repo_name, e)
            conn.rollback()

        except anthropic.APIError as e:
            stats.errors += 1
            stats.error_repos.append(repo_name)
            logger.warning("Claude API error for %s: %s", repo_name, e)
            conn.rollback()
            await asyncio.sleep(2)

        except Exception as e:
            stats.errors += 1
            stats.error_repos.append(repo_name)
            logger.warning("Unexpected error for %s: %s", repo_name, e)
            conn.rollback()

        # Progress + checkpoint every 50 repos
        if (i + 1) % 50 == 0 or (i + 1) == stats.total:
            logger.info(
                "Progress: %d/%d enriched (errors: %d, tokens: %d in + %d out)",
                stats.enriched, stats.total, stats.errors,
                stats.total_input_tokens, stats.total_output_tokens,
            )
            _write_cost_log(cost_log_path, stats)
            _write_resume(resume_path, stats)

        # Small delay to be respectful of rate limits
        await asyncio.sleep(0.3)

    conn.close()

    # Final summary
    input_cost = stats.total_input_tokens / 1_000_000 * 3.0
    output_cost = stats.total_output_tokens / 1_000_000 * 15.0
    total_cost = input_cost + output_cost
    elapsed = time.time() - stats.start_time

    logger.info(
        "Phase 2 COMPLETE: %d enriched, %d errors, %d input tokens, %d output tokens, $%.4f total, %.0fs elapsed",
        stats.enriched, stats.errors, stats.total_input_tokens, stats.total_output_tokens,
        total_cost, elapsed,
    )

    resume_path.write_text(
        f"""# Reporium Ingestion Resume
Phase 0: COMPLETE
Phase 1: COMPLETE
Phase 2: COMPLETE — {stats.enriched}/{stats.total} enriched, {stats.errors} errors, ${total_cost:.4f} spent
Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}
Next phase: 3 (embeddings + taxonomy rebuild)
""",
        encoding="utf-8",
    )

    return stats
