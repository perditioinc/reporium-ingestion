# Reporium Ingestion — Cost Report

**Date:** 2026-03-21
**Status:** Historical snapshot of the March 2026 ingestion/backfill milestone
**Repos processed:** 826

## Summary

| Phase | Description | Cost | Duration |
|-------|-------------|------|----------|
| Phase 0 | Database audit, schema migration | $0.00 | 5 min |
| Phase 1 | Dependency extraction from GitHub | $0.00 | 45 min |
| Phase 2 | Claude API enrichment (826 repos, historical run) | **$2.52** | 78 min |
| Phase 3 | Embedding generation (sentence-transformers) | $0.00 | 62 sec |
| Phase 4 | Knowledge graph edge building | $0.00 | 6 min |
| Phase 5 | /intelligence/query endpoint | $0.00 | Deploy only |
| Phase 6 | Documentation | $0.00 | — |
| Backfill | library.json to Neon (825 repos) | $0.00 | 27 min |
| **Total** | | **$2.52** | ~3 hours |

## Phase 2 Breakdown (only paid phase)

| Metric | Value |
|--------|-------|
| Model | claude-sonnet-4-20250514 |
| Input tokens | 279,788 |
| Output tokens | 112,126 |
| Input cost | $0.84 |
| Output cost | $1.68 |
| **Total cost** | **$2.52** |
| Repos enriched | 826/826 |
| Errors | 0 |
| Per-repo cost | $0.003 |

## Phase 5 Per-Query Cost (ongoing)

| Metric | Value |
|--------|-------|
| Model | claude-sonnet-4-20250514 |
| Avg input tokens/query | ~2,000 |
| Avg output tokens/query | ~370 |
| **Avg cost/query** | **~$0.012** |

At 100 queries/day = $1.20/day = ~$36/month

## What $2.52 Bought

- 826 repos with AI-generated summaries, problem descriptions, and integration tags in the March 2026 run
- 826 semantic embeddings for natural language search in the March 2026 run
- 5,418 knowledge graph edges (compatibility, alternatives, dependencies)
- A working /intelligence/query endpoint that answers natural language questions
- Full database backfill: 14K tags, 2K pmSkills, 918 industries, 825 builders
- reporium.com switched from static JSON to live API

## Budget vs Actual

| Item | Budget | Actual |
|------|--------|--------|
| Claude API enrichment | $4-5 | $2.52 |
| Embeddings | $0 | $0 |
| Knowledge graph | $0 | $0 |
| **Total** | **$4-5** | **$2.52** |

49% under budget.

## Current Usage Note

Use this report as a dated milestone record. Current ingestion cost and corpus assumptions should be derived from current pipeline runs, not from this March 2026 snapshot.
