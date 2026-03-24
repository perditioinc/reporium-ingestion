"""
AI Enrichment using Claude API (claude-sonnet-4-20250514).
One call per repo. Generates: readme_summary, problem_solved, categories, integration_tags.

Cost: ~$4-5 total for 826 repos (~600 input + ~200 output tokens per call).
Every call is logged to COST_LOG.md. Progress saved to RESUME.md every 50 repos.
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

ENRICHMENT_PROMPT = """You are analyzing a GitHub repository to classify it for a knowledge graph of AI development tools.

Given this repository information:
{repo_context}

Respond with ONLY valid JSON, no markdown, no explanation:
{{
  "readme_summary": "2-3 sentence plain language description of what this repo does and who uses it",
  "problem_solved": "1 sentence: what specific problem does this solve",
  "categories": ["category1", "category2"],
  "ai_dev_skills": ["skill1", "skill2"],
  "lifecycleGroup": "Foundation & Training|Inference & Deployment|LLM Application Layer|Eval/Safety/Ops|Modality-Specific|Applied AI",
  "integration_tags": ["tool1", "tool2"],
  "quality_assessment": "high|medium|low"
}}

Categories must only come from this list:
agents, llm-serving, embeddings, vector-databases, evaluation, fine-tuning,
rag, orchestration, observability, data-processing, ocr, vision, audio,
code-generation, security, deployment, tooling, datasets, research, other

ai_dev_skills must only come from this list of 28 skill areas (a repo can match multiple):
Foundation & Training group: Foundation Model Architecture, Fine-tuning & Alignment, Data Engineering, Synthetic Data
Inference & Deployment group: Inference & Serving, Model Compression, Edge AI
LLM Application Layer group: Agents & Orchestration, RAG & Retrieval, Context Engineering, Tool Use, Structured Output, Prompt Engineering, Knowledge Graphs
Eval/Safety/Ops group: Evaluation, Security & Guardrails, Observability, MLOps, AI Governance
Modality-Specific group: Computer Vision, Speech & Audio, Generative Media, NLP, Multimodal
Applied AI group: Coding Assistants, Robotics, AI for Science, Recommendation Systems

lifecycleGroup must be the single best-matching group name from the 6 groups above.

Integration tags are frameworks/tools this repo integrates with (e.g. langchain, openai, huggingface, fastapi).
Maximum 5 integration tags. If unknown, use empty array."""

VALID_CATEGORIES = {
    "agents", "llm-serving", "embeddings", "vector-databases", "evaluation",
    "fine-tuning", "rag", "orchestration", "observability", "data-processing",
    "ocr", "vision", "audio", "code-generation", "security", "deployment",
    "tooling", "datasets", "research", "other",
}

VALID_AI_DEV_SKILLS = {
    # Foundation & Training
    "Foundation Model Architecture", "Fine-tuning & Alignment", "Data Engineering", "Synthetic Data",
    # Inference & Deployment
    "Inference & Serving", "Model Compression", "Edge AI",
    # LLM Application Layer
    "Agents & Orchestration", "RAG & Retrieval", "Context Engineering", "Tool Use",
    "Structured Output", "Prompt Engineering", "Knowledge Graphs",
    # Eval/Safety/Ops
    "Evaluation", "Security & Guardrails", "Observability", "MLOps", "AI Governance",
    # Modality-Specific
    "Computer Vision", "Speech & Audio", "Generative Media", "NLP", "Multimodal",
    # Applied AI
    "Coding Assistants", "Robotics", "AI for Science", "Recommendation Systems",
}

VALID_LIFECYCLE_GROUPS = {
    "Foundation & Training",
    "Inference & Deployment",
    "LLM Application Layer",
    "Eval/Safety/Ops",
    "Modality-Specific",
    "Applied AI",
}


@dataclass
class EnrichmentResult:
    repo_id: str
    repo_name: str
    readme_summary: Optional[str] = None
    problem_solved: Optional[str] = None
    categories: list[str] = field(default_factory=list)
    integration_tags: list[str] = field(default_factory=list)
    quality_assessment: str = "medium"
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
    if row.get('dependencies'):
        deps = row['dependencies']
        if isinstance(deps, str):
            deps = json.loads(deps)
        if deps:
            parts.append(f"Dependencies: {', '.join(deps[:20])}")
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

    # Validate categories
    categories = data.get("categories", [])
    valid = [c for c in categories if c in VALID_CATEGORIES]
    if not valid and categories:
        valid = ["other"]
    data["categories"] = valid[:5]

    # Validate ai_dev_skills (28-skill taxonomy)
    raw_skills = data.get("ai_dev_skills", [])
    valid_skills = [s for s in raw_skills if s in VALID_AI_DEV_SKILLS]
    data["ai_dev_skills"] = valid_skills[:10]

    # Validate lifecycleGroup
    lifecycle = data.get("lifecycleGroup", "")
    if lifecycle not in VALID_LIFECYCLE_GROUPS:
        lifecycle = ""
    data["lifecycleGroup"] = lifecycle

    # Validate integration_tags
    tags = data.get("integration_tags", [])
    data["integration_tags"] = [t.lower().strip() for t in tags[:5]]

    # Validate quality_assessment
    qa = data.get("quality_assessment", "medium")
    if qa not in ("high", "medium", "low"):
        qa = "medium"
    data["quality_assessment"] = qa

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
Repos in DB: 826
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

    # Get repos needing enrichment (readme_summary IS NULL)
    cur.execute("""
        SELECT id, name, owner, description, primary_language, forked_from, dependencies
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

        try:
            context = _build_repo_context(repo)
            prompt = ENRICHMENT_PROMPT.format(repo_context=context)

            response = client.messages.create(
                model=model,
                max_tokens=500,
                messages=[{"role": "user", "content": prompt}],
            )

            stats.total_input_tokens += response.usage.input_tokens
            stats.total_output_tokens += response.usage.output_tokens

            text = response.content[0].text
            data = _parse_enrichment_response(text)

            # Write to database
            cur.execute(
                """UPDATE repos SET
                    readme_summary = %s,
                    problem_solved = %s,
                    integration_tags = %s::jsonb
                WHERE id = %s""",
                (
                    data.get("readme_summary"),
                    data.get("problem_solved"),
                    json.dumps(data.get("integration_tags", [])),
                    repo["id"],
                ),
            )

            # Write categories to junction table
            categories = data.get("categories", [])
            for j, cat in enumerate(categories):
                cur.execute(
                    """INSERT INTO repo_categories (repo_id, category_id, category_name, is_primary)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING""",
                    (
                        repo["id"],
                        cat,
                        cat.replace("-", " ").title(),
                        j == 0,
                    ),
                )

            # Write ai_dev_skills from the 28-skill taxonomy
            ai_dev_skills = data.get("ai_dev_skills", [])
            for skill in ai_dev_skills:
                cur.execute(
                    """INSERT INTO repo_ai_dev_skills (repo_id, skill)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING""",
                    (repo["id"], skill),
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
            # Brief pause on API errors
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

        # Small delay between calls to be respectful of rate limits
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

    # Write final resume
    resume_path.write_text(
        f"""# Reporium Ingestion Resume
Phase 0: COMPLETE
Phase 1: COMPLETE
Phase 2: COMPLETE — {stats.enriched}/{stats.total} enriched, {stats.errors} errors, ${total_cost:.4f} spent
Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}
Repos in DB: 826
Next phase: 3 (embeddings)
""",
        encoding="utf-8",
    )

    return stats
