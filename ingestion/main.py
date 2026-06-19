"""
Reporium Ingestion — main pipeline orchestrator and CLI entry point.

Usage:
    python -m ingestion run [--mode quick|weekly|full]
    python -m ingestion fix --repos repo1 repo2
    python -m ingestion status
    python -m ingestion cache stats
    python -m ingestion cache clean
    python -m ingestion bootstrap
    python -m ingestion schedule
"""
import asyncio
import logging
import math
import os
import sys
import time
import json
from datetime import datetime, timezone
from typing import Any

import httpx
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from rich.table import Table
from rich.panel import Panel
from rich import print as rprint

from .config import get_settings, RunMode
from .budget import (
    select_work_for_run,
    compute_freshness,
    run_time_budget_seconds,
    is_checkpointed,
)
from .cache.database import CacheDatabase
from .github.rate_limit import RateLimitManager
from .github.client import GitHubClient
from .github.fetcher import RepoFetcher, FetchedRepo
from .enrichment.tagger import enrich_tags
from .enrichment.taxonomy import (
    assign_primary_category, assign_all_categories,
    assign_dimension, build_builder, PM_SKILLS,
)
from .enrichment.summarizer import RepoSummarizer
from .enrichers.ai_enricher import (
    ENRICHMENT_PROMPT,
    _build_repo_context,
    _parse_enrichment_response,
    run_ai_enrichment,
)
from .api.client import ReporiumAPIClient
from .analysis.trends import build_trend_snapshot
from .analysis.gaps import detect_gaps
from .extractors.dependencies import FILE_TO_ECOSYSTEM

console = Console()
logger = logging.getLogger(__name__)


def _compute_commit_stats(commits: list[dict]) -> dict:
    now = datetime.now(timezone.utc)
    stats = {'today': 0, 'last7Days': 0, 'last30Days': 0, 'last90Days': 0}
    for c in commits:
        try:
            committed = datetime.fromisoformat(c['committed_at'].replace('Z', '+00:00'))
            days = (now - committed).days
            if days == 0:
                stats['today'] += 1
            if days < 7:
                stats['last7Days'] += 1
            if days < 30:
                stats['last30Days'] += 1
            if days < 90:
                stats['last90Days'] += 1
        except Exception:
            logger.warning(
                "Skipping malformed commit while computing stats",
                extra={"commit_sha": c.get("sha"), "committed_at": c.get("committed_at")},
                exc_info=True,
            )
    return stats


# ── Commit-stats refresh — fixes the corpus-wide `last7Days = 0` freeze ───────
#
# The per-repo daily fetch only re-pulls commits when a repo's own
# `github_updated_at` changes (cache.needs_daily_fetch). For the fork-heavy
# corpus that timestamp rarely moves, so commits served from cache age past the
# 90-day window and commits_last_*_days decay to 0 everywhere. This phase fetches
# GitHub's /stats/commit_activity for each repo's UPSTREAM (forks have no commits
# of their own) and overwrites the payload counts + activity_score with fresh
# values. On unavailable stats it sets the fields to None so the API preserves
# the stored values — a transient outage can never blank real commit counts.

COMMIT_STATS_RATE_LIMIT_FLOOR = 100


def _windows_from_weeks(weeks: list[dict]) -> tuple[int, int, int]:
    """Collapse GitHub's 52-week commit_activity payload into 7/30/90-day totals."""
    c7 = weeks[-1].get("total", 0) if len(weeks) >= 1 else 0
    c30 = sum(w.get("total", 0) for w in weeks[-4:]) if len(weeks) >= 4 else 0
    c90 = sum(w.get("total", 0) for w in weeks[-13:]) if len(weeks) >= 13 else 0
    return c7, c30, c90


def _refresh_commit_stats_blocking(
    items: list[tuple[dict, int, int, bool]],
    token: str,
    *,
    max_repos: int = 0,
    rate_limit_floor: int = COMMIT_STATS_RATE_LIMIT_FLOOR,
    deadline: float | None = None,
) -> dict:
    """Mutate each payload's commit counts + activity_score in place from
    GitHub /stats/commit_activity (upstream-targeted).

    Synchronous/blocking by design (reuses the proven sync fetcher with its
    202-retry) — call via ``asyncio.to_thread``. Bounded by the GitHub rate
    limit (stops when remaining < ``rate_limit_floor``), an optional
    ``max_repos`` cap, and an optional wall-clock ``deadline`` (a
    ``time.monotonic()`` value): when crossed the loop stops early so this
    phase -- the slowest, at 0.5s sleep/repo -- can never push the invocation
    past the Cloud Run task timeout. When stats are unavailable for a repo
    the payload's counts + activity fields are set to None so the API's
    null-skip upsert preserves the stored values rather than overwriting them
    with 0.

    ``items`` pairs each payload with (stars, forks, is_archived) so the
    activity score can be recomputed from the fresh counts.
    """
    from scripts.fetch_commit_stats import fetch_commit_activity

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3+json",
    }

    # Default EVERY payload to the preserve sentinel (None) up front. Anything we
    # don't successfully refresh below — capped, rate-limited, errored, or with
    # stats unavailable, including repos never reached after an early break —
    # then stays None, so the API's null-skip upsert keeps the stored value
    # instead of overwriting it with a stale 0. Fresh values are written only on
    # a confirmed success.
    for payload, *_rest in items:
        payload["commits_last_7_days"] = None
        payload["commits_last_30_days"] = None
        payload["commits_last_90_days"] = None
        payload["activity_score"] = None
        payload["activity_score_breakdown"] = None

    updated = skipped = errors = deadline_stops = 0
    with httpx.Client(timeout=30.0) as client:
        for i, (payload, stars, forks, is_archived) in enumerate(items):
            if max_repos and i >= max_repos:
                break
            if deadline is not None and time.monotonic() >= deadline:
                deadline_stops = len(items) - i
                logger.warning(
                    "commit-stats: per-run time budget exhausted after %d repos "
                    "-- deferring %d to next run",
                    i, deadline_stops,
                )
                break
            target = payload.get("forked_from") or f"{payload['owner']}/{payload['name']}"
            try:
                weeks = fetch_commit_activity(client, target, headers)

                remaining = getattr(client, "_last_rate_limit_remaining", None)
                if remaining is not None and remaining < rate_limit_floor:
                    logger.warning(
                        "commit-stats: GitHub rate limit low (%s remaining) — stopping early",
                        remaining,
                    )
                    break

                if not weeks:
                    # None (unavailable / persistent 202 / non-200) or [] (brand-new
                    # repo): leave the preserve sentinel in place.
                    skipped += 1
                    continue

                c7, c30, c90 = _windows_from_weeks(weeks)
                # Compute the score BEFORE touching the payload so a raising
                # _compute_activity_score can't leave a partial mutation (counts
                # set, score still the None sentinel). Either all four land, or
                # none do and the preserve sentinel stands.
                score = _compute_activity_score(
                    stars=stars,
                    forks=forks,
                    commits_last7=c7,
                    commits_last30=c30,
                    commits_last90=c90,
                    is_archived=is_archived,
                )
                payload["commits_last_7_days"] = c7
                payload["commits_last_30_days"] = c30
                payload["commits_last_90_days"] = c90
                payload.update(score)
                updated += 1
            except Exception as exc:
                errors += 1
                logger.warning("commit-stats: error fetching %s: %s", target, exc)

            time.sleep(0.5)

    return {
        "updated": updated,
        "skipped": skipped,
        "errors": errors,
        "deadline_deferred": deadline_stops,
    }


def _budget_exhausted(deadline: float | None) -> bool:
    """Fix #4: end-to-end MONOTONIC budget guard. ``deadline`` is a
    ``time.monotonic()`` value captured at run start + the wall-clock budget.
    Returns True once the monotonic clock has crossed it, so EACH expensive
    per-repo phase (fetch / summarise / enrich / commit-stats / post) can stop
    cleanly before the Cloud Run task timeout instead of only commit-stats.

    Uses ``time.monotonic()`` (never ``time.time()``) so an NTP step / wall-clock
    jump can neither prematurely abort nor blow past the budget. ``None``
    deadline (budget disabled) never trips.
    """
    if deadline is None:
        return False
    return time.monotonic() >= deadline


def _compute_run_status(
    full_corpus_repos: list,
    cached: dict,
    *,
    fix_mode: bool = False,
) -> tuple[str, int]:
    """Fix #5: recompute the TRUE run status from checkpoint state at END.

    ``_partial = deferred_repos > 0`` (the old logic) only knew about the
    work-selection cap. It missed runs where selected work was NOT fully
    processed -- a mid-pipeline kill, a commit-stats ``deadline_deferred``, or
    per-repo failures that left repos uncheckpointed. The honest signal is: how
    many repos are STILL pending (not ``is_checkpointed``) against the FULL
    corpus, using the cache as it stands AFTER this run's completed-markers were
    written.

    Returns ``(status, pending_count)`` where ``status`` is ``"completed"`` only
    when the corpus is actually drained (0 pending), else ``"partial"``. Fix
    mode targets an explicit repo set and is always reported ``"completed"``.
    """
    if fix_mode:
        return "completed", 0
    pending = sum(1 for r in full_corpus_repos if not is_checkpointed(r, cached))
    return ("completed" if pending == 0 else "partial"), pending


def _apply_cached_forked_from(all_repos: list, cached: dict) -> int:
    """Fill `forked_from` on forks from the durable cache so the subsequent
    hydrate_fork_parents() call only has to API-fetch genuinely NEW forks.

    The repo-list endpoint omits `parent`, so every fork arrives with
    forked_from=None; hydrating all of them costs ~1 GitHub call per fork
    (~1900 calls / ~27 min for this corpus) on every run. Repos we've already
    ingested have forked_from stored in cache, so we reuse it here and leave
    only brand-new forks for the API. Mutates `all_repos` in place; returns the
    number of forks populated from cache.
    """
    filled = 0
    for repo in all_repos:
        if getattr(repo, "is_fork", False) and getattr(repo, "forked_from", None) is None:
            cached_row = cached.get(repo.name)
            if cached_row is not None and getattr(cached_row, "forked_from", None):
                repo.forked_from = cached_row.forked_from
                filled += 1
    return filled


def _build_language_percentages(breakdown: dict[str, int]) -> dict[str, float]:
    total = sum(breakdown.values())
    if not total:
        return {}
    return {lang: round(bytes_ / total * 100, 1) for lang, bytes_ in breakdown.items()}


def _compute_activity_score(
    *,
    stars: int,
    forks: int,
    commits_last7: int,
    commits_last30: int,
    commits_last90: int,
    is_archived: bool,
) -> dict:
    """
    Returns {'activity_score': int, 'activity_score_breakdown': dict}.

    Archived repos are scored differently — they can't accumulate new commit velocity
    so we cap them at 10, driven only by log-scaled star count as a proxy for
    historical relevance.

    Active repos:
      commits   : min(60, last30d * 3 + last7d * 5)  — velocity signal
      stars     : min(15, log2(stars+1) * 2)          — popularity
      forks     : min(15, log2(forks+1) * 3)          — ecosystem adoption
      recency   : 10 if any commit in last 90d else 0  — still alive?
    Total max = 100.
    """
    if is_archived:
        score = min(10, int(math.log2(stars + 1) * 1.5))
        return {
            "activity_score": score,
            "activity_score_breakdown": {
                "archived": True,
                "stars_component": score,
                "total": score,
            },
        }

    commits_component = min(60, commits_last30 * 3 + commits_last7 * 5)
    stars_component = min(15, int(math.log2(stars + 1) * 2))
    forks_component = min(15, int(math.log2(forks + 1) * 3))
    recency_bonus = 10 if commits_last90 > 0 else 0
    total = min(100, commits_component + stars_component + forks_component + recency_bonus)

    return {
        "activity_score": total,
        "activity_score_breakdown": {
            "archived": False,
            "commits_component": commits_component,
            "stars_component": stars_component,
            "forks_component": forks_component,
            "recency_bonus": recency_bonus,
            "total": total,
        },
    }


async def _to_api_payload(
    fetched: FetchedRepo,
    summarizer: RepoSummarizer,
) -> dict:
    repo = fetched.github_repo

    # Tag enrichment
    tags = enrich_tags(
        language=repo.primary_language,
        topics=repo.topics,
        stars=repo.stars,
        updated_at=repo.updated_at,
        is_fork=repo.is_fork,
        is_archived=repo.is_archived,
        readme_text=fetched.readme,
    )

    # Taxonomy
    primary_category = assign_primary_category(tags)
    all_categories = assign_all_categories(tags)
    pm_skills = assign_dimension(tags, PM_SKILLS)

    # Builder
    builder = build_builder(
        is_fork=repo.is_fork,
        forked_from=repo.forked_from,
        full_name=repo.full_name,
    )

    # AI summary
    summary = None
    if fetched.readme:
        summary = await summarizer.summarize(repo.name, fetched.readme, tags)

    # Commit stats
    commit_stats = _compute_commit_stats(fetched.commits)
    language_pcts = _build_language_percentages(fetched.languages)

    # Language list for API
    languages_list = [
        {
            'language': lang,
            'bytes': bytes_,
            'percentage': language_pcts.get(lang, 0.0),
        }
        for lang, bytes_ in fetched.languages.items()
    ]

    # Categories for API
    categories_list = []
    if primary_category:
        categories_list.append({
            'category_id': primary_category.lower().replace(' ', '-').replace('&', 'and').replace(':', ''),
            'category_name': primary_category,
            'is_primary': True,
        })
    for cat in all_categories:
        if cat != primary_category:
            categories_list.append({
                'category_id': cat.lower().replace(' ', '-').replace('&', 'and').replace(':', ''),
                'category_name': cat,
                'is_primary': False,
            })

    # Commits for API
    commits_list = [
        {
            'sha': c.get('sha', ''),
            'message': c.get('message', ''),
            'author': c.get('author', ''),
            'committed_at': c.get('committed_at', ''),
            'url': c.get('url', ''),
        }
        for c in fetched.commits[:20]
    ]

    return {
        'name': repo.name,
        'owner': repo.owner,
        'description': repo.description,
        'is_fork': repo.is_fork,
        'is_private': repo.is_private,
        'forked_from': repo.forked_from,
        'primary_language': repo.primary_language,
        'github_url': repo.github_url,
        'open_issues_count': repo.open_issues_count,
        'forks_count': repo.forks_count,
        'fork_sync_state': fetched.fork_sync_state,
        'behind_by': fetched.behind_by,
        'ahead_by': fetched.ahead_by,
        'commits_last_7_days': commit_stats['last7Days'],
        'commits_last_30_days': commit_stats['last30Days'],
        'commits_last_90_days': commit_stats['last90Days'],
        # Timeline fields — powers the "Your last push / Upstream last push /
        # Last indexed" card on the repo detail page. The DB columns have always
        # existed, but the ingestion payload omitted them, so the frontend
        # timeline was frozen to whatever was backfilled manually.
        'github_updated_at': repo.updated_at,
        'your_last_push_at': repo.pushed_at,
        'upstream_last_push_at': fetched.upstream_last_push_at,
        # KAN-DRAFT-trends-payload-timestamps: github_created_at and forked_at
        # were silently omitted, leaving both columns blank in the DB. The
        # /trends "New This Week" panel filters on `github_created_at > NOW() -
        # INTERVAL '7 days'` and so showed an empty list even though the daily
        # Cloud Run Job WAS topping the corpus up. GitHub does not expose a
        # separate "forked_at" timestamp — the convention is forked_at == the
        # fork's own created_at. For non-forks, forked_at is meaningless, so
        # we send None (the API schema accepts `datetime | None`, the DB
        # stores NULL — never the empty string the frontend was choking on).
        'github_created_at': repo.created_at,
        'forked_at': repo.created_at if repo.is_fork else None,
        'readme_summary': summary,
        **_compute_activity_score(
            stars=repo.stars or 0,
            forks=repo.forks_count or 0,
            commits_last7=commit_stats['last7Days'],
            commits_last30=commit_stats['last30Days'],
            commits_last90=commit_stats['last90Days'],
            is_archived=repo.is_archived,
        ),
        'tags': tags,
        'categories': categories_list,
        'builders': [builder],
        'pm_skills': pm_skills,
        # Open taxonomy dimensions — populated by the AI enricher, not the tagger.
        # dependencies and license_spdx are fetched directly by the fetcher (no AI cost).
        'skill_areas': [],
        'industries': [],
        'use_cases': [],
        'modalities': [],
        'ai_trends': [],
        'deployment_context': [],
        'maturity_level': None,
        'quality_assessment': None,
        'has_tests': fetched.has_tests,
        'has_ci': fetched.has_ci,
        'integration_tags': [],
        'dependencies': fetched.dependencies,
        # Derive ecosystem from the source file so the API can tag repo_dependencies rows correctly.
        'dep_ecosystem': (
            FILE_TO_ECOSYSTEM.get(fetched.dep_source_file.split('/')[-1])
            if fetched.dep_source_file else None
        ),
        'license_spdx': fetched.github_repo.license_spdx,
        'languages': languages_list,
        'commits': commits_list,
    }


# ── KAN-199: AI enrichment for ingest payloads ───────────────────────────────
#
# Wires the AI enricher into `python -m ingestion run` (the nightly Cloud Run
# Job entry point). KAN-191's quality probe surfaced the regression: every
# nightly since PR #64 cutover shipped empty `integration_tags` because the
# tagger pass hard-codes `[]` for AI-populated fields and `ingestion.main`
# never invoked the enricher. See
# `.audit/2026-05-03-12h-run/enrichment-regression-rca.md` (KAN-196).
#
# We invoke ai_enricher.run_ai_enrichment() AFTER the API post for any
# already-existing repos with NULL readme_summary. For the just-upserted
# repos (whose AI fields would otherwise stay default in the payload), we
# also run a per-payload Claude pass BEFORE the API post and merge the
# result into the payload — that way `integration_tags`, `skill_areas`,
# `industries`, `use_cases`, `modalities`, `ai_trends`, `deployment_context`,
# `quality_assessment`, `maturity_level` flow through the existing
# /ingest/repos endpoint (the same API path the tagger uses), instead of
# direct-DB UPDATEs that target columns the live schema doesn't have
# (`quality_assessment`, `maturity_level`, etc. are stored in junction
# tables / `repo_taxonomy`, not as columns on `repos`).
#
# Errors from Claude or the parser are logged and the run continues —
# enrichment failure must NOT abort the ingestion run (per KAN-199 design
# constraint). Per-repo failure leaves the payload's empty AI fields in
# place; the next nightly will retry that repo.


def _merge_ai_fields_into_payload(payload: dict, ai_data: dict) -> None:
    """Merge parsed Claude output into a tagger-built API payload.

    Only non-empty AI fields overwrite. The API's `_upsert_repo` skip-empty
    guard means this is safe even if Claude returned partial fields.
    """
    if ai_data.get("readme_summary"):
        # Prefer AI summary over the local fallback summary, which often
        # falls below the probe's 50-char floor (per KAN-196 RCA §2).
        payload["readme_summary"] = ai_data["readme_summary"]
    for field in (
        "integration_tags",
        "skill_areas",
        "industries",
        "use_cases",
        "modalities",
        "ai_trends",
        "deployment_context",
    ):
        val = ai_data.get(field)
        if val:
            payload[field] = val
    if ai_data.get("quality_assessment") is not None:
        payload["quality_assessment"] = ai_data["quality_assessment"]
    if ai_data.get("maturity_level") is not None:
        payload["maturity_level"] = ai_data["maturity_level"]


# KAN-230: bound to ~10 concurrent Anthropic calls. Anthropic's standard
# tier accepts well past that, but 10× is enough to bring 1866 sequential
# calls × ~5s each (~2.5h) down to ~16 min — comfortably inside the 30 min
# Cloud Run Job timeout we want to drop to once cache + parallelism land.
# Override via env var if Anthropic upgrades the tier or new repo volumes
# justify a different ceiling.
ENRICHMENT_CONCURRENCY = int(os.environ.get("ENRICHMENT_CONCURRENCY", "10"))


def _enrich_force_all() -> bool:
    """KAN-230: opt-in full re-enrich override.

    Read at call time (not import) so tests and ops can flip it per run.
    Truthy values: ``1``, ``true``, ``yes`` (case-insensitive). Default OFF.
    """
    return os.environ.get("ENRICH_FORCE_ALL", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def _needs_ai_enrichment(fetched: "FetchedRepo", *, force_all: bool) -> bool:
    """KAN-230 gating predicate: should this repo be sent to Claude this run?

    Root cause this fixes: `ingestion.main` built a payload for every fetched
    repo and called the per-payload Claude pass on the ENTIRE ~1866-repo
    corpus every nightly run. With no "already enriched / unchanged" filter
    the run blew past the Cloud Run Job timeout and `integration_tags` never
    persisted (Nightly Enrichment Quality Probe red since ~2026-05-12).

    The durable, post-#96 signal that a repo has already been fully processed
    (and therefore already enriched on a prior run) is its ``repo_cache`` row,
    which now lives in PostgreSQL and survives across Cloud Run Job
    executions. A repo is "unchanged & already processed" — and so must be
    skipped — iff ALL of:

      * a durable cache row exists, AND
      * ``cache.daily_fetched_at`` is set (it went through a full daily
        fetch at least once → the per-payload enricher already ran for it
        on that run), AND
      * ``cache.github_updated_at`` equals the repo's current GitHub
        ``updated_at`` (GitHub has not changed it since).

    This mirrors the existing ``CacheDatabase.needs_daily_fetch`` logic —
    the same change signal that already drives the structural fetch tiers —
    so gating cannot drift from fetch behaviour. No DB schema change.

    Returns ``True`` (enrich) for: ``force_all``; never-seen repos (no cache
    row); repos fetched only at the permanent tier (``daily_fetched_at`` is
    ``None``); and repos whose GitHub ``updated_at`` moved since last
    processed. Returns ``False`` only for the unchanged-and-processed case.
    """
    if force_all:
        return True

    cache = getattr(fetched, "cache", None)
    repo = fetched.github_repo

    # Never-seen repo: FetchedRepo synthesizes an empty RepoCacheRow when the
    # DB had no row, so a missing row presents as cache.daily_fetched_at=None
    # and cache.github_updated_at=None — both caught below. Guard None too in
    # case a caller passes a bare FetchedRepo.
    if cache is None:
        return True

    # Never fully processed (only permanent tier, or brand new) → enrich.
    if not cache.daily_fetched_at:
        return True

    # GitHub changed the repo since we last processed it → re-enrich.
    if cache.github_updated_at != repo.updated_at:
        return True

    # Cache row + daily fetch + unchanged updated_at → already enriched on a
    # prior run and nothing changed. Skip the Claude call.
    return False


def _select_payloads_for_enrichment(
    pairs: list[tuple[dict, "FetchedRepo"]],
) -> list[dict]:
    """KAN-230: from (payload, fetched) pairs, return only the payloads whose
    repo still needs AI enrichment this run.

    Structural payloads for ALL repos are still built and posted to the API
    by the caller (freshness of stars/commits/timeline is unaffected); this
    only trims the expensive per-payload Claude pass to new/changed/forced
    repos. WEEKLY/FULL still run the corpus-wide ``run_ai_enrichment``
    catch-up (its own NULL-``readme_summary`` WHERE clause), so genuinely
    under-enriched older rows are still backfilled there.
    """
    force_all = _enrich_force_all()
    return [
        payload
        for payload, fetched in pairs
        if _needs_ai_enrichment(fetched, force_all=force_all)
    ]


async def _enrich_payloads_with_ai(
    payloads: list[dict],
    *,
    api_key: str,
    model: str,
) -> dict:
    """For every just-built payload, call Claude and merge AI fields back in.

    Mutates `payloads` in place. Returns a stats dict for logging.

    Uses the same prompt + parser as `ingestion.enrichers.ai_enricher` so
    output shape is identical to the `run_ai_enrichment` direct-DB path.
    Anthropic SDK calls are dispatched to a worker thread to avoid blocking
    the asyncio event loop.

    KAN-230: per-payload calls run concurrently under an
    ``asyncio.Semaphore(ENRICHMENT_CONCURRENCY)`` (default 10). The previous
    sequential loop took ~5s per call × ~1866 repos = ~2.5h serial, blowing
    past the Cloud Run Job timeout on every nightly run. Concurrency drops
    that to ~16 min for the same workload while staying inside Anthropic's
    rate limits at the standard tier.
    """
    stats = {
        "attempted": len(payloads),
        "enriched": 0,
        "errors": 0,
        "input_tokens": 0,
        "output_tokens": 0,
    }
    if not payloads:
        return stats
    if not api_key:
        logger.warning(
            "KAN-199: ANTHROPIC_API_KEY not set — skipping per-payload AI "
            "enrichment; integration_tags will remain empty for this run."
        )
        return stats

    try:
        import anthropic
    except ImportError:
        logger.warning(
            "KAN-199: anthropic SDK not installed — skipping per-payload AI "
            "enrichment. Add `anthropic` to requirements.txt to enable."
        )
        return stats

    # KAN-230 follow-up: switch from sync `Anthropic` + asyncio.to_thread to
    # native `AsyncAnthropic`. The to_thread approach is bottlenecked by the
    # default thread pool (~5 workers on 1 vCPU containers), which capped
    # effective concurrency below ENRICHMENT_CONCURRENCY=10 and caused a
    # 1866-payload run to take ~60 min instead of the predicted ~16. Native
    # async removes the thread-pool ceiling entirely; the semaphore is the
    # only concurrency cap that matters.
    client = anthropic.AsyncAnthropic(api_key=api_key)
    sem = asyncio.Semaphore(ENRICHMENT_CONCURRENCY)
    stats_lock = asyncio.Lock()

    async def _enrich_one(payload: dict) -> None:
        repo_label = f"{payload.get('owner')}/{payload.get('name')}"
        async with sem:
            try:
                # Build the same context shape the existing AI enricher uses.
                context_row = {
                    "owner": payload.get("owner") or "",
                    "name": payload.get("name") or "",
                    "description": payload.get("description"),
                    "primary_language": payload.get("primary_language"),
                    "forked_from": payload.get("forked_from"),
                    "dependencies": payload.get("dependencies"),
                }
                prompt = ENRICHMENT_PROMPT.format(
                    repo_context=_build_repo_context(context_row)
                )

                response = await client.messages.create(
                    model=model,
                    max_tokens=800,
                    messages=[{"role": "user", "content": prompt}],
                )
                data = _parse_enrichment_response(response.content[0].text)
                _merge_ai_fields_into_payload(payload, data)

                async with stats_lock:
                    stats["input_tokens"] += response.usage.input_tokens
                    stats["output_tokens"] += response.usage.output_tokens
                    stats["enriched"] += 1

            except Exception as exc:
                async with stats_lock:
                    stats["errors"] += 1
                logger.warning(
                    "KAN-199: AI enrichment failed for %s: %s",
                    repo_label,
                    exc,
                    exc_info=False,
                )
                # Best-effort Sentry capture. No-op if sentry-sdk isn't installed
                # or hasn't been initialized.
                try:
                    import sentry_sdk
                    sentry_sdk.capture_exception(exc)
                except Exception:
                    pass

    try:
        await asyncio.gather(*(_enrich_one(p) for p in payloads))
    finally:
        await client.close()

    return stats


def _make_cache(settings):
    """Select the cache backend (KAN-230).

    Durable Postgres cache (survives Cloud Run Job executions) when
    DATABASE_URL is a postgres URL; the module-level SQLite ``CacheDatabase``
    otherwise (local/dev/tests). The factory import is lazy and gated on a
    string check so unit tests that stub ``ingestion.cache`` or monkeypatch
    ``main.CacheDatabase`` keep working unchanged.
    """
    url = getattr(settings, "database_url", "") or ""
    if isinstance(url, str) and url.strip().lower().startswith(("postgres://", "postgresql")):
        from .cache import build_cache_database
        return build_cache_database(settings)
    return CacheDatabase(settings.cache_db_path)


async def run_ingestion(mode: RunMode, fix_repos: list[str] | None = None) -> None:
    settings = get_settings()
    start_time = time.time()

    console.rule(f'[bold blue]Reporium Ingestion — {mode.value.capitalize()} Mode[/bold blue]')

    db = _make_cache(settings)
    await db.init()

    rate_limiter = RateLimitManager(min_buffer=settings.min_rate_limit_buffer)
    summarizer = RepoSummarizer()
    api_client = ReporiumAPIClient()

    run_id = await db.start_run(mode.value)

    async with GitHubClient(rate_limiter, db) as gh_client:
        # Check rate limit
        with console.status('Checking rate limit...'):
            await gh_client.get_rate_limit()

        rl = rate_limiter.remaining
        console.print(f'Rate limit: [cyan]{rl:,}[/cyan] / 5,000 remaining')

        # Fetch repo list
        with console.status('Fetching repo list...'):
            if fix_repos:
                # Fix mode: construct minimal GitHubRepo objects from cache
                all_repos = await gh_client.get_repos(settings.gh_username)
                all_repos = [r for r in all_repos if r.name in fix_repos]
            else:
                all_repos = await gh_client.get_repos(settings.gh_username)

        # Load the durable cache ONCE, up front. It already stores forked_from
        # for every fork we've seen, so we can fill that in locally and let
        # hydrate_fork_parents API-fetch only genuinely NEW forks. Previously
        # the caller hydrated ALL ~1900 forks every run (~1900 API calls / ~27
        # min — the dominant cost of the corpus-scale run that kept tripping the
        # Cloud Run task timeout). See hydrate_fork_parents' own docstring.
        cached = {r.name: r for r in await db.get_all_repos()}
        from_cache = _apply_cached_forked_from(all_repos, cached)

        # Hydrate forked_from for the remaining (new) forks via secondary fetch.
        # The list endpoint omits `parent`, so brand-new forks still have None.
        with console.status('Hydrating fork parents...'):
            await gh_client.hydrate_fork_parents(all_repos)

        api_calls_after_list = rate_limiter.calls_this_run
        console.print(
            f'Fetching repo list... [green]✓[/green]  {len(all_repos)} repos '
            f'({api_calls_after_list} API calls; {from_cache} fork parents from cache)'
        )

        # Estimate budget
        est = rate_limiter.estimate_calls(len(all_repos), mode)
        budget = await rate_limiter.check_budget(est)
        console.print(f'Estimated calls: [cyan]{est}[/cyan]')
        if budget.ok:
            console.print(f'Budget: [green]✓ sufficient[/green]')
        else:
            console.print(f'Budget: [yellow]⚠ {budget.message}[/yellow]')
            if budget.wait_seconds > 0:
                console.print(f'[yellow]Waiting {budget.wait_seconds}s for rate limit reset...[/yellow]')
                await asyncio.sleep(budget.wait_seconds)

        # Check cache (reuse the dict loaded above for fork-parent hydration)
        with console.status('Checking cache...'):
            unchanged = sum(
                1 for repo in all_repos
                if repo.name in cached and cached[repo.name].github_updated_at == repo.updated_at
                and cached[repo.name].daily_fetched_at is not None
            )
            changed = len(all_repos) - unchanged

        console.print(f'Checking cache... [green]✓[/green]  {unchanged} unchanged, {changed} updated')

        # ── Resumable per-run budgeting (fix for the 3600s Cloud Run timeout) ──
        #
        # Process only a BOUNDED, PRIORITISED slice of the pending set this
        # invocation so the run finishes well under the task timeout. The
        # durable Postgres cache is the checkpoint: repos processed on a prior
        # run are skipped, so the next scheduled (or re-triggered) run drains
        # whatever is still pending. Newly-forked repos are prioritised so
        # freshness recovers in the first run even with a backlog.
        #
        # Fix mode is exempt -- the operator named an explicit, small repo set
        # and expects ALL of them processed regardless of checkpoint state.
        # Fix #2 (full corpus vs slice): keep the FULL corpus list intact for
        # GLOBAL / whole-corpus phases (freshness SLO, trend snapshot + gap
        # analysis, the repo-ingested event that triggers the API's taxonomy /
        # graph-edge / diff-delete / relationship rebuild). The per-run budget
        # cap applies ONLY to the per-repo phases (fetch / summarise / enrich /
        # commit-stats / post). Previously `all_repos = selection.selected`
        # overwrote the corpus, so those global phases silently became
        # slice-only (e.g. the trend snapshot's total_repos collapsed to the
        # batch size and tag/category counts undercounted).
        full_corpus_repos = all_repos
        corpus_size = len(full_corpus_repos)
        freshness = compute_freshness(full_corpus_repos, cached)
        logger.info("freshness SLO: %s", freshness.as_metrics())
        if freshness.breached:
            console.print(
                f'[yellow]Freshness SLO breached: newest pending repo '
                f'"{freshness.newest_pending_repo}" is '
                f'{freshness.lag_hours:.1f}h old (SLO {freshness.slo_hours}h)[/yellow]'
            )
        else:
            console.print(
                f'Freshness: [green]✓[/green]  '
                f'{freshness.pending_total} pending, '
                f'lag '
                + (f'{freshness.lag_hours:.1f}h' if freshness.lag_hours is not None else 'n/a')
                + f' (SLO {freshness.slo_hours}h)'
            )
        if freshness.oldest_pending_repo and freshness.oldest_lag_hours is not None:
            console.print(
                f'  [dim]oldest pending: "{freshness.oldest_pending_repo}" '
                f'{freshness.oldest_lag_hours:.1f}h behind[/dim]'
            )

        if fix_repos:
            selection = None
            deferred_repos = 0
            selected_repos = full_corpus_repos
        else:
            selection = select_work_for_run(full_corpus_repos, cached)
            deferred_repos = selection.deferred
            # Per-repo phases operate on the bounded slice ONLY. The full corpus
            # is preserved in `full_corpus_repos` for the global phases above/below.
            selected_repos = selection.selected
            logger.info(
                "phase: work selection -- corpus=%d pending=%d selected=%d "
                "(new=%d changed=%d) deferred=%d capped=%s reserved_oldest=%d",
                corpus_size,
                selection.pending_total,
                len(selection.selected),
                selection.new_count,
                selection.changed_count,
                selection.deferred,
                selection.capped,
                selection.reserved_oldest,
            )
            console.print(
                f'Work selection: [cyan]{len(selected_repos)}[/cyan] this run '
                f'([green]{selection.new_count} new[/green], '
                f'{selection.changed_count} changed)'
                + (
                    f'  [yellow]{selection.deferred} deferred to next run[/yellow]'
                    if selection.deferred
                    else '  [dim](corpus fully drained)[/dim]'
                )
            )

        # Per-run wall-clock budget (Fix #4): captured as a MONOTONIC deadline at
        # the START of the per-repo work, so EACH expensive per-repo phase can
        # check it and stop cleanly before the Cloud Run task timeout -- not just
        # commit-stats. Uses time.monotonic() (immune to NTP / wall-clock jumps).
        # The cache is checkpointed per-repo only AFTER the API post, so a
        # budget-stopped run leaves durable COMPLETED markers for what finished
        # and the next run resumes the rest. time_budget_s <= 0 disables the cap.
        time_budget_s = run_time_budget_seconds()
        run_deadline = (
            time.monotonic() + time_budget_s if time_budget_s and time_budget_s > 0 else None
        )

        # Fetch updated repos
        fetcher = RepoFetcher(gh_client, db)
        payloads: list[dict] = []
        repos_updated = 0

        if _budget_exhausted(run_deadline):
            logger.warning(
                "phase: fetch skipped -- per-run budget already exhausted; "
                "deferring all %d selected repos to next run", len(selected_repos),
            )
            selected_repos = []

        with Progress(
            SpinnerColumn(),
            TextColumn('[progress.description]{task.description}'),
            BarColumn(),
            TextColumn('{task.completed}/{task.total}'),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task('Fetching updated repos...', total=len(selected_repos))
            fetched_repos = await fetcher.fetch_changed_repos(selected_repos, mode)
            progress.update(task, completed=len(selected_repos))

        api_calls_fetch = rate_limiter.calls_this_run
        # KAN-230: explicit phase boundaries on the structured logger so a
        # silent timeout becomes traceable. The Rich `console.status(...)`
        # spinner suppresses everything until the phase completes; once a
        # phase hangs, you cannot tell from the logs which phase you are
        # in. logger.info() bypasses the spinner and lands in Cloud Logging
        # immediately.
        logger.info(
            "phase: fetch_changed_repos complete (api_calls=%d, fetched=%d)",
            api_calls_fetch,
            len(fetched_repos),
        )

        # Enrich with AI
        logger.info("phase: building payloads (summarizer pass) — count=%d", len(fetched_repos))
        # KAN-230: keep each payload paired with its FetchedRepo so the
        # enrichment gate can read the durable (post-#96 Postgres) cache
        # snapshot. ALL payloads are still built and posted to the API —
        # structural freshness (stars/commits/timeline) is unaffected.
        enrich_pairs: list[tuple[dict, FetchedRepo]] = []
        with console.status('Enriching with AI...'):
            for fetched in fetched_repos:
                payload = await _to_api_payload(fetched, summarizer)
                payloads.append(payload)
                enrich_pairs.append((payload, fetched))

        enriched_count = len(payloads)
        console.print(f'Enriching with AI... [green]✓[/green]  {enriched_count} repos enriched')
        logger.info("phase: payload build complete — count=%d", enriched_count)

        # KAN-230: gate the expensive per-payload Claude pass. Without this,
        # `_enrich_payloads_with_ai` was called on the ENTIRE ~1866-repo
        # corpus every nightly run — ~1866 Claude calls that blew past the
        # Cloud Run Job timeout, so `integration_tags` never persisted and
        # `COMPATIBLE_WITH` edges stayed dead. We now only enrich repos that
        # are new, changed, or forced (ENRICH_FORCE_ALL=1). Unchanged repos
        # that were already enriched on a prior run are skipped — their AI
        # fields already landed in the DB and the API `_upsert_repo`
        # skip-empty guard means re-posting their structural payload with
        # empty AI fields does NOT clobber them.
        payloads_to_enrich = _select_payloads_for_enrichment(enrich_pairs)
        _force_all = _enrich_force_all()
        logger.info(
            "phase: enrichment gate — total=%d to_enrich=%d skipped=%d force_all=%s",
            len(enrich_pairs),
            len(payloads_to_enrich),
            len(enrich_pairs) - len(payloads_to_enrich),
            _force_all,
        )
        console.print(
            f'Enrichment gate (KAN-230): [cyan]{len(payloads_to_enrich)}[/cyan] '
            f'to enrich, [dim]{len(enrich_pairs) - len(payloads_to_enrich)} '
            f'unchanged/skipped[/dim]'
            + ('  [yellow](ENRICH_FORCE_ALL)[/yellow]' if _force_all else '')
        )

        # Fix #4: budget guard BEFORE the (expensive) Claude enrichment phase.
        # If the per-run deadline is already crossed, skip enrichment cleanly --
        # the structural payloads are still posted below, and the unenriched
        # repos are NOT marked completed, so the next run re-enriches them.
        budget_deferred_enrich_names: set = set()
        if payloads_to_enrich and _budget_exhausted(run_deadline):
            # Record which repos had enrichment deferred by the budget so we do
            # NOT mark them completed below (they must be re-enriched next run).
            _deferred_ids = {id(p) for p in payloads_to_enrich}
            budget_deferred_enrich_names = {
                f.github_repo.name for (p, f) in enrich_pairs if id(p) in _deferred_ids
            }
            logger.warning(
                "phase: AI enrichment skipped -- per-run budget exhausted; "
                "deferring enrichment of %d repos to next run",
                len(payloads_to_enrich),
            )
            payloads_to_enrich = []

        # KAN-199: AI enrichment on the gated payloads. Populates
        # integration_tags + open-taxonomy dimensions BEFORE the API post,
        # so they flow through /ingest/repos like every other field. Failure
        # here logs and returns; it MUST NOT abort the structural run.
        if payloads_to_enrich:
            logger.info("phase: AI enrichment (KAN-199) starting — count=%d", len(payloads_to_enrich))
            with console.status('AI enrichment (KAN-199)...'):
                try:
                    ai_stats = await _enrich_payloads_with_ai(
                        payloads_to_enrich,
                        api_key=settings.anthropic_api_key,
                        model=settings.enrichment_model,
                    )
                except Exception as exc:
                    logger.warning(
                        "KAN-199: AI enrichment phase crashed; continuing "
                        "ingestion run. Error: %s",
                        exc,
                        exc_info=True,
                    )
                    try:
                        import sentry_sdk
                        sentry_sdk.capture_exception(exc)
                    except Exception:
                        pass
                    ai_stats = {
                        "attempted": len(payloads_to_enrich),
                        "enriched": 0,
                        "errors": len(payloads_to_enrich),
                        "input_tokens": 0,
                        "output_tokens": 0,
                    }
            console.print(
                f'AI enrichment: [green]✓[/green]  '
                f'{ai_stats["enriched"]}/{ai_stats["attempted"]} enriched, '
                f'{ai_stats["errors"]} errors '
                f'(tokens: {ai_stats["input_tokens"]} in / '
                f'{ai_stats["output_tokens"]} out)'
            )
            logger.info(
                "phase: AI enrichment complete — enriched=%d errors=%d input_tokens=%d output_tokens=%d",
                ai_stats["enriched"],
                ai_stats["errors"],
                ai_stats["input_tokens"],
                ai_stats["output_tokens"],
            )

        # Commit-stats refresh — overwrite stale per-repo commit windows with
        # fresh upstream /stats/commit_activity BEFORE posting, so the counts
        # flow through the API payload AND the trend snapshot below. Bounded by
        # the GitHub rate limit; COMMIT_STATS_MAX_REPOS caps it per run (0 = all).
        # COMMIT_STATS_ENABLED=0 disables the phase entirely (kill-switch for
        # when the run is over its time budget) while keeping GH_TOKEN available
        # for the rest of ingestion.
        _commit_stats_enabled = os.getenv("COMMIT_STATS_ENABLED", "1") not in ("0", "false", "False")
        # Fix #1 + #4: track how many trailing repos the commit-stats phase
        # deferred at the deadline. Those repos were NOT fully derived this run,
        # so they must NOT receive the COMPLETED checkpoint below -- the next
        # run re-processes them.
        cs_deadline_deferred = 0
        if settings.gh_token and enrich_pairs and _commit_stats_enabled:
            items = [
                (
                    p,
                    f.github_repo.stars or 0,
                    f.github_repo.forks_count or 0,
                    bool(f.github_repo.is_archived),
                )
                for p, f in enrich_pairs
            ]
            max_repos = int(os.getenv("COMMIT_STATS_MAX_REPOS", "0") or "0")
            # Fix #4: reuse the SAME monotonic run deadline captured at the start
            # of the per-repo work (no wall-clock recomputation). The blocking
            # refresh stops the moment time.monotonic() crosses it and reports
            # the rest as deadline_deferred. None => budget disabled (unbounded).
            cs_deadline = run_deadline
            logger.info(
                "phase: commit-stats refresh starting -- repos=%d max_repos=%d "
                "budget_remaining_s=%s",
                len(items), max_repos,
                round(run_deadline - time.monotonic(), 1) if run_deadline is not None else "unbounded",
            )
            with console.status("Refreshing commit stats..."):
                try:
                    cs = await asyncio.to_thread(
                        _refresh_commit_stats_blocking,
                        items, settings.gh_token, max_repos=max_repos,
                        deadline=cs_deadline,
                    )
                    cs_deadline_deferred = cs.get("deadline_deferred", 0)
                    console.print(
                        f"Refreshing commit stats... [green]✓[/green]  "
                        f"{cs['updated']} updated, {cs['skipped']} preserved, "
                        f"{cs['errors']} errors"
                    )
                    logger.info("phase: commit-stats refresh complete — %s", cs)
                except Exception as exc:
                    logger.warning(
                        "commit-stats refresh crashed; continuing run. Error: %s",
                        exc, exc_info=True,
                    )
                    try:
                        import sentry_sdk
                        sentry_sdk.capture_exception(exc)
                    except Exception:
                        pass
        elif enrich_pairs:
            # Phase skipped (kill-switch off or no GH token): the payloads still
            # carry commit counts + activity_score derived from fetched.commits,
            # which are stale for cache-served repos. Posting them would overwrite
            # fresher stored stats. Null them (the API's preserve sentinel) so the
            # DB keeps its existing commit data instead.
            for p, _f in enrich_pairs:
                p["commits_last_7_days"] = None
                p["commits_last_30_days"] = None
                p["commits_last_90_days"] = None
                p["activity_score"] = None
                p["activity_score_breakdown"] = None
            logger.info(
                "phase: commit-stats refresh SKIPPED (enabled=%s, token=%s) — "
                "commit/activity fields nulled to preserve stored values",
                _commit_stats_enabled, bool(settings.gh_token),
            )

        # Post to API
        logger.info("phase: posting to API — payloads=%d", len(payloads))
        with console.status('Posting to API...'):
            result = await api_client.upsert_repos(payloads)
            repos_updated = result.upserted

        console.print(f'Posting to API... [green]✓[/green]  {repos_updated} repos updated')
        logger.info("phase: API post complete — upserted=%d errors=%d", repos_updated, len(result.errors))
        if result.errors:
            for err in result.errors[:5]:
                console.print(f'  [red]⚠ {err}[/red]')

        # Publish event so the API can trigger taxonomy + intelligence refresh
        if repos_updated > 0:
            from .events.pubsub import publish_repo_ingested
            publish_repo_ingested(
                run_mode=mode.value,
                upserted=repos_updated,
                repo_names=[p['name'] for p in payloads],
            )

        # Fix #1 (lost-work checkpoint): set the COMPLETED marker ONLY now, AFTER
        # the repo has been fully posted to the API with all derived fields.
        # `is_checkpointed` keys on this marker (NOT the fetcher's
        # daily_fetched_at), so a run killed earlier in the pipeline does not
        # cause the next run to skip-and-lose the repo. We deliberately exclude:
        #   * repos whose commit-stats were deadline-deferred (the trailing
        #     `cs_deadline_deferred` items -- not fully derived this run), and
        #   * everything when the API post reported errors (we cannot tell which
        #     rows landed, so leave them pending to be re-posted -- idempotent).
        # Excluded repos simply get re-processed next run; nothing is lost.
        if enrich_pairs and not result.errors:
            completable = enrich_pairs[:len(enrich_pairs) - cs_deadline_deferred] \
                if cs_deadline_deferred else enrich_pairs
            # Do NOT checkpoint repos whose enrichment was budget-deferred this
            # run: leave them pending so the next run re-enriches them. They are
            # still posted above, so freshness is preserved -- only enrichment
            # (not ingestion) is deferred.
            if budget_deferred_enrich_names:
                completable = [
                    (_p, _f) for (_p, _f) in completable
                    if _f.github_repo.name not in budget_deferred_enrich_names
                ]
            marked = 0
            for _p, _f in completable:
                try:
                    await db.mark_completed(
                        _f.github_repo.name, _f.github_repo.updated_at
                    )
                    marked += 1
                except Exception as exc:
                    logger.warning(
                        "could not mark %s completed: %s", _f.github_repo.name, exc
                    )
            logger.info(
                "phase: completed-checkpoint set — marked=%d deferred_by_deadline=%d",
                marked, cs_deadline_deferred,
            )
        elif result.errors:
            logger.warning(
                "phase: completed-checkpoint SKIPPED — API post reported %d "
                "errors; leaving repos pending for re-post next run",
                len(result.errors),
            )

        # KAN-199: corpus-wide catch-up — invoke ai_enricher.run_ai_enrichment
        # for any rows that still have NULL readme_summary in the DB. This is
        # a no-op once the corpus is fully enriched (the enricher's WHERE
        # clause filters to the unset population). Runs only on weekly/full
        # to keep `quick` short; the per-payload pass above covers freshly
        # ingested repos for `quick` runs.
        if mode in (RunMode.WEEKLY, RunMode.FULL) and settings.database_url and settings.anthropic_api_key:
            with console.status('AI enrichment catch-up (KAN-199)...'):
                try:
                    catchup_stats = await run_ai_enrichment(
                        db_url=settings.database_url,
                        api_key=settings.anthropic_api_key,
                        model=settings.enrichment_model,
                    )
                    console.print(
                        f'AI catch-up: [green]✓[/green]  '
                        f'{catchup_stats.enriched}/{catchup_stats.total} enriched, '
                        f'{catchup_stats.errors} errors'
                    )
                except Exception as exc:
                    logger.warning(
                        "KAN-199: AI catch-up enrichment crashed; "
                        "continuing run. Error: %s",
                        exc,
                        exc_info=True,
                    )
                    try:
                        import sentry_sdk
                        sentry_sdk.capture_exception(exc)
                    except Exception:
                        pass

        # Post trend snapshot and gap analysis on every successful run.
        #
        # Previously gated to WEEKLY|FULL only (KAN-DRAFT-trends-daily-snapshot,
        # PR #71), which left /trends/report empty in production: the daily
        # Cloud Run Job runs in QUICK mode (see deploy/job.yaml), and the
        # weekly GHA path was the only writer. Each snapshot is timestamped
        # (captured_at = now()) so multiple snapshots per day are fine —
        # they're a time series, not an upsert.
        # Fix #2: trends/gaps are a GLOBAL phase. The tag/category counts are
        # derived from THIS run's payloads (the budgeted slice), but total_repos
        # must report the TRUE corpus size, not the slice size, so the /trends
        # "total" does not collapse to the batch size on a partial run. Each
        # snapshot is a timestamped time-series point, so incremental per-run
        # counts accumulate correctly downstream.
        with console.status('Computing trends & gaps...'):
            snapshot = build_trend_snapshot(payloads, total_repos_override=corpus_size)
            gaps = detect_gaps(snapshot)
            await api_client.post_trend_snapshot(snapshot)
            await api_client.post_gap_analysis(gaps)
        console.print(f'Trends & gaps: [green]✓[/green]  {len(gaps)} gaps detected')

    elapsed = time.time() - start_time
    total_api_calls = rate_limiter.calls_this_run

    # Fix #5: recompute the TRUE run status from checkpoint state AT THE END.
    # The old `_partial = deferred_repos > 0` only knew about the work-selection
    # cap; it reported "completed" whenever nothing was deferred, even if the
    # selected work did NOT fully process (mid-pipeline kill, commit-stats
    # deadline_deferred, API errors leaving repos uncheckpointed). We instead
    # reload the cache (now carrying this run's COMPLETED markers) and count
    # repos that are STILL pending against the FULL corpus. "completed" is
    # reported ONLY when the corpus is actually drained (0 pending).
    try:
        cached_after = {r.name: r for r in await db.get_all_repos()}
    except Exception as _exc:
        logger.warning("could not reload cache for status recompute: %s", _exc)
        cached_after = cached
    _run_status, _pending_after = _compute_run_status(
        full_corpus_repos, cached_after, fix_mode=bool(fix_repos),
    )
    _partial = _run_status != "completed"
    logger.info(
        "phase: run-status recompute -- status=%s pending=%d deferred_at_selection=%d",
        _run_status, _pending_after, deferred_repos,
    )

    await db.finish_run(
        run_id=run_id,
        repos_processed=corpus_size,
        repos_updated=repos_updated,
        api_calls_made=total_api_calls,
        rate_limit_hits=0,
        status=_run_status,
    )

    # Best-effort: record run in reporium-api for the run-history endpoint
    _finished_at = datetime.now(timezone.utc)
    _started_at = datetime.fromtimestamp(start_time, tz=timezone.utc)
    try:
        async with httpx.AsyncClient(timeout=10) as _client:
            _headers = {}
            if settings.reporium_api_key:
                _headers["Authorization"] = f"Bearer {settings.reporium_api_key}"
            if settings.ingest_api_key:
                _headers["X-Admin-Key"] = settings.ingest_api_key
            await _client.post(
                f"{settings.reporium_api_url.rstrip('/')}/admin/runs",
                json={
                    "run_mode": mode.value,
                    # "partial" when more repos remain pending for the next run;
                    # "success" when the corpus was fully drained this run.
                    "status": "partial" if _partial else "success",
                    "repos_upserted": repos_updated,
                    "repos_processed": corpus_size,
                    "errors": [],
                    "started_at": _started_at.isoformat(),
                    "finished_at": _finished_at.isoformat(),
                },
                headers=_headers,
            )
    except Exception as _exc:
        logging.getLogger(__name__).debug("Could not record run in API: %s", _exc)

    logger.info(
        "run complete: status=%s elapsed_s=%.0f corpus=%d processed_this_run=%d "
        "upserted=%d pending_after=%d deferred_at_selection=%d api_calls=%d",
        _run_status, elapsed, corpus_size, len(selected_repos),
        repos_updated, _pending_after, deferred_repos, total_api_calls,
    )

    console.rule()
    if _partial:
        console.print(
            f'[yellow bold]Partial run complete in {elapsed:.0f}s[/yellow bold] '
            f'[dim](re-run to continue: {_pending_after} repos still pending)[/dim]'
        )
    else:
        console.print(f'[green bold]Complete in {elapsed:.0f}s[/green bold]')
    console.print(f'  API calls: {total_api_calls} (saved ~{max(0, corpus_size*5 - total_api_calls)} with cache)')
    console.print(f'  Repos processed this run: {len(selected_repos)} / {corpus_size} corpus')
    console.print(f'  Repos updated: {repos_updated}')
    if _pending_after:
        console.print(f'  Still pending (re-run to continue): {_pending_after}')
    console.print(f'  Rate limit remaining: {rate_limiter.remaining:,}')


async def show_status() -> None:
    settings = get_settings()
    console.rule('[bold]Reporium Ingestion — Status[/bold]')

    db = _make_cache(settings)
    await db.init()

    stats = await db.get_cache_stats()
    last_run = await db.get_last_run()

    table = Table(show_header=False)
    table.add_row('Total repos cached', str(stats['total_repos']))
    table.add_row('Permanent cached', str(stats['permanent_cached']))
    table.add_row('Daily cached', str(stats['daily_cached']))
    table.add_row('Total runs', str(stats['total_runs']))
    table.add_row('Total API calls logged', str(stats['total_api_calls_logged']))
    if last_run:
        table.add_row('Last run', f"{last_run.mode} at {last_run.started_at} [{last_run.status}]")
    console.print(table)

    # Check GitHub rate limit
    rate_limiter = RateLimitManager()
    async with GitHubClient(rate_limiter, db) as gh:
        await gh.get_rate_limit()
    console.print(f'\nGitHub rate limit remaining: [cyan]{rate_limiter.remaining:,}[/cyan]')


async def show_cache_stats() -> None:
    settings = get_settings()
    db = _make_cache(settings)
    await db.init()
    stats = await db.get_cache_stats()

    table = Table(title='Cache Statistics')
    for k, v in stats.items():
        table.add_row(k.replace('_', ' ').title(), str(v))
    console.print(table)


async def clean_cache(days: int = 90) -> None:
    settings = get_settings()
    db = _make_cache(settings)
    await db.init()
    removed = await db.clean_stale(days)
    console.print(f'[green]Removed {removed} stale cache entries (older than {days} days)[/green]')


def main() -> None:
    args = sys.argv[1:]

    if not args:
        console.print('Usage: python -m ingestion [run|fix|status|cache|schedule|bootstrap]')
        sys.exit(1)

    command = args[0]

    if command == 'run':
        mode_str = 'quick'
        for i, arg in enumerate(args):
            if arg == '--mode' and i + 1 < len(args):
                mode_str = args[i + 1]
        try:
            mode = RunMode(mode_str)
        except ValueError:
            console.print(f'[red]Unknown mode: {mode_str}. Use quick, weekly, or full.[/red]')
            sys.exit(1)
        asyncio.run(run_ingestion(mode))

    elif command == 'fix':
        repos = []
        for i, arg in enumerate(args):
            if arg == '--repos':
                # Accept either space-separated (`--repos a b c`) or
                # comma-separated (`--repos "a,b,c"`). The workflow passes a
                # single quoted CSV; interactive callers typically use spaces.
                raw = args[i + 1:]
                for token in raw:
                    for name in token.split(','):
                        name = name.strip()
                        if name:
                            repos.append(name)
                break
        if not repos:
            console.print('[red]Usage: python -m ingestion fix --repos repo1 repo2  (or --repos "repo1,repo2")[/red]')
            sys.exit(1)
        asyncio.run(run_ingestion(RunMode.QUICK, fix_repos=repos))

    elif command == 'status':
        asyncio.run(show_status())

    elif command == 'cache':
        sub = args[1] if len(args) > 1 else ''
        if sub == 'stats':
            asyncio.run(show_cache_stats())
        elif sub == 'clean':
            asyncio.run(clean_cache())
        else:
            console.print('Usage: python -m ingestion cache [stats|clean]')

    elif command == 'schedule':
        from .scheduler import start_scheduler
        asyncio.run(start_scheduler())

    elif command == 'bootstrap':
        from scripts.bootstrap import run_bootstrap
        asyncio.run(run_bootstrap())

    else:
        console.print(f'[red]Unknown command: {command}[/red]')
        sys.exit(1)
