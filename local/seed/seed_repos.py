"""
Seed the local substrate with a few synthetic repos via the real
ReporiumAPIClient -> stub reporium-api -> local Postgres path.

This exercises the actual ingestion write contract (ingestion.api.client) end
to end against the OSS substitutes, without any live GitHub / Cloud / Anthropic
calls. AI fields are pre-filled here (the way the Claude enricher would have
populated them) so the embeddings text has content; enrichment itself degrades
gracefully when ANTHROPIC_API_KEY is unset, which is the local default.

Run from the repo root with the local env loaded (see local/.env.local).
"""
import asyncio
import sys

from ingestion.api.client import ReporiumAPIClient

_SAMPLE = [
    {
        "name": "vectordb-lite",
        "owner": "perditioinc",
        "description": "A small embeddings-native vector store for RAG demos.",
        "is_fork": False,
        "is_private": False,
        "primary_language": "Python",
        "github_url": "https://example.invalid/vectordb-lite",
        "readme_summary": "vectordb-lite is a lightweight vector database for "
                          "retrieval-augmented generation prototypes.",
        "problem_solved": "Fast local similarity search without a cloud service.",
        "integration_tags": ["pgvector", "fastapi", "sentence-transformers"],
        "maturity_level": "prototype",
        "quality_assessment": "medium",
        "github_updated_at": "2026-06-01T00:00:00Z",
    },
    {
        "name": "agent-runner",
        "owner": "perditioinc",
        "description": "Minimal agent orchestration loop with tool calling.",
        "is_fork": False,
        "is_private": False,
        "primary_language": "Python",
        "github_url": "https://example.invalid/agent-runner",
        "readme_summary": "agent-runner is a compact agentic loop that wires an "
                          "LLM to tools for autonomous task execution.",
        "problem_solved": "Run tool-using agents without a heavy framework.",
        "integration_tags": ["anthropic", "langchain", "fastapi"],
        "maturity_level": "beta",
        "quality_assessment": "high",
        "github_updated_at": "2026-06-02T00:00:00Z",
    },
    {
        "name": "embed-bench",
        "owner": "perditioinc",
        "description": "Benchmark harness for sentence-embedding models.",
        "is_fork": False,
        "is_private": False,
        "primary_language": "Python",
        "github_url": "https://example.invalid/embed-bench",
        "readme_summary": "embed-bench measures latency and recall of local "
                          "sentence-embedding models for semantic search.",
        "problem_solved": "Compare embedding models on your own corpus offline.",
        "integration_tags": ["sentence-transformers", "pgvector", "numpy"],
        "maturity_level": "research",
        "quality_assessment": "medium",
        "github_updated_at": "2026-06-03T00:00:00Z",
    },
]


async def main() -> int:
    client = ReporiumAPIClient()
    ok = await client.check_connection()
    if not ok:
        print("ERROR: stub reporium-api /health not reachable", file=sys.stderr)
        return 1
    result = await client.upsert_repos(_SAMPLE)
    print(f"seeded: upserted={result.upserted} errors={result.errors}")
    if result.errors:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
