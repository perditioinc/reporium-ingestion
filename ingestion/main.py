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

    client = anthropic.Anthropic(api_key=api_key)

    for payload in payloads:
        repo_label = f"{payload.get('owner')}/{payload.get('name')}"
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

            def _call_claude():
                return client.messages.create(
                    model=model,
                    max_tokens=800,
                    messages=[{"role": "user", "content": prompt}],
                )

            response = await asyncio.to_thread(_call_claude)
            stats["input_tokens"] += response.usage.input_tokens
            stats["output_tokens"] += response.usage.output_tokens

            data = _parse_enrichment_response(response.content[0].text)
            _merge_ai_fields_into_payload(payload, data)
            stats["enriched"] += 1

        except Exception as exc:
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

        # Modest courtesy delay to avoid Anthropic rate-limit headwinds on
        # weekly/full runs. Mirrors the 0.3s sleep in run_ai_enrichment.
        await asyncio.sleep(0.3)

    return stats


async def run_ingestion(mode: RunMode, fix_repos: list[str] | None = None) -> None:
    settings = get_settings()
    start_time = time.time()

    console.rule(f'[bold blue]Reporium Ingestion — {mode.value.capitalize()} Mode[/bold blue]')

    db = CacheDatabase(settings.cache_db_path)
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

        # Hydrate forked_from via secondary fetch. The list endpoint returns
        # the minimal-repository schema which omits `parent`, so forks come
        # back with forked_from=None. Done after the fix_repos filter so a
        # fix-mode run for 6 repos only makes ~6 extra API calls instead of
        # one per fork in the entire account (~900, 7+ min at rate-limit).
        with console.status('Hydrating fork parents...'):
            await gh_client.hydrate_fork_parents(all_repos)

        api_calls_after_list = rate_limiter.calls_this_run
        console.print(f'Fetching repo list... [green]✓[/green]  {len(all_repos)} repos ({api_calls_after_list} API calls)')

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

        # Check cache
        with console.status('Checking cache...'):
            cached = {r.name: r for r in await db.get_all_repos()}
            unchanged = sum(
                1 for repo in all_repos
                if repo.name in cached and cached[repo.name].github_updated_at == repo.updated_at
                and cached[repo.name].daily_fetched_at is not None
            )
            changed = len(all_repos) - unchanged

        console.print(f'Checking cache... [green]✓[/green]  {unchanged} unchanged, {changed} updated')

        # Fetch updated repos
        fetcher = RepoFetcher(gh_client, db)
        payloads: list[dict] = []
        repos_updated = 0

        with Progress(
            SpinnerColumn(),
            TextColumn('[progress.description]{task.description}'),
            BarColumn(),
            TextColumn('{task.completed}/{task.total}'),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task('Fetching updated repos...', total=len(all_repos))
            fetched_repos = await fetcher.fetch_changed_repos(all_repos, mode)
            progress.update(task, completed=len(all_repos))

        api_calls_fetch = rate_limiter.calls_this_run

        # Enrich with AI
        with console.status('Enriching with AI...'):
            for fetched in fetched_repos:
                payload = await _to_api_payload(fetched, summarizer)
                payloads.append(payload)

        enriched_count = len(payloads)
        console.print(f'Enriching with AI... [green]✓[/green]  {enriched_count} repos enriched')

        # KAN-199: AI enrichment on the just-built payloads. Populates
        # integration_tags + open-taxonomy dimensions BEFORE the API post,
        # so they flow through /ingest/repos like every other field. Failure
        # here logs and returns; it MUST NOT abort the structural run.
        if payloads:
            with console.status('AI enrichment (KAN-199)...'):
                try:
                    ai_stats = await _enrich_payloads_with_ai(
                        payloads,
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
                        "attempted": len(payloads),
                        "enriched": 0,
                        "errors": len(payloads),
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

        # Post to API
        with console.status('Posting to API...'):
            result = await api_client.upsert_repos(payloads)
            repos_updated = result.upserted

        console.print(f'Posting to API... [green]✓[/green]  {repos_updated} repos updated')
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
        with console.status('Computing trends & gaps...'):
            snapshot = build_trend_snapshot(payloads)
            gaps = detect_gaps(snapshot)
            await api_client.post_trend_snapshot(snapshot)
            await api_client.post_gap_analysis(gaps)
        console.print(f'Trends & gaps: [green]✓[/green]  {len(gaps)} gaps detected')

    elapsed = time.time() - start_time
    total_api_calls = rate_limiter.calls_this_run

    await db.finish_run(
        run_id=run_id,
        repos_processed=len(all_repos),
        repos_updated=repos_updated,
        api_calls_made=total_api_calls,
        rate_limit_hits=0,
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
                    "status": "success",
                    "repos_upserted": repos_updated,
                    "repos_processed": len(all_repos),
                    "errors": [],
                    "started_at": _started_at.isoformat(),
                    "finished_at": _finished_at.isoformat(),
                },
                headers=_headers,
            )
    except Exception as _exc:
        logging.getLogger(__name__).debug("Could not record run in API: %s", _exc)

    console.rule()
    console.print(f'[green bold]✓ Complete in {elapsed:.0f}s[/green bold]')
    console.print(f'  API calls: {total_api_calls} (saved ~{max(0, len(all_repos)*5 - total_api_calls)} with cache)')
    console.print(f'  Repos updated: {repos_updated}')
    console.print(f'  Rate limit remaining: {rate_limiter.remaining:,}')


async def show_status() -> None:
    settings = get_settings()
    console.rule('[bold]Reporium Ingestion — Status[/bold]')

    db = CacheDatabase(settings.cache_db_path)
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
    db = CacheDatabase(settings.cache_db_path)
    await db.init()
    stats = await db.get_cache_stats()

    table = Table(title='Cache Statistics')
    for k, v in stats.items():
        table.add_row(k.replace('_', ' ').title(), str(v))
    console.print(table)


async def clean_cache(days: int = 90) -> None:
    settings = get_settings()
    db = CacheDatabase(settings.cache_db_path)
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
