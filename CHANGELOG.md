# Changelog

## [1.1.0] - 2026-03-21

### Added
- Database backfill script (`scripts/backfill_from_library_json.py`) — populates Neon from library.json
- Backfilled: 14,077 tags, 2,062 pmSkills, 918 industries, 825 builders, 4,920 categories (29 distinct)
- `repo_industries` table created for industry classification
- Ingestion log record documenting the full weekend's work

### Changed
- RESUME.md and COST_REPORT.md updated with backfill results
- BACKFILL_NEEDED.md documents the gap analysis and switch-back conditions

## [1.0.0] - 2026-03-21

### Added
- Phase 1: Dependency extraction from GitHub (392/826 repos, $0)
- Phase 2: Claude API enrichment with claude-sonnet-4-20250514 (826/826 repos, $2.52, 0 errors)
- Phase 3: Sentence-transformer embeddings (826 x 384-dim, all-MiniLM-L6-v2, 62s, $0)
- Phase 4: Knowledge graph edges (5,418 edges: 1,988 COMPATIBLE_WITH, 3,000 ALTERNATIVE_TO, 430 DEPENDS_ON)
- Phase 5: Semantic search verification (5 queries, all returning relevant results)
- Phase 6: COST_REPORT.md and ARCHITECTURE_DECISIONS.md
- Schema migration: added dependencies, problem_solved, integration_tags, quality_signals columns
- ANTHROPIC_API_KEY loaded from GCP Secret Manager with whitespace stripping
