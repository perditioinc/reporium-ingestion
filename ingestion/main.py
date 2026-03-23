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
    assign_dimension, build_builder, AI_DEV_SKILLS, PM_SKILLS,
)
from .enrichment.summarizer import RepoSummarizer
from .enrichment.embeddings import EmbeddingGenerator
from .api.client import ReporiumAPIClient
from .analysis.trends import build_trend_snapshot
from .analysis.gaps import detect_gaps

console = Console()


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
            pass
    return stats


def _build_language_percentages(breakdown: dict[str, int]) -> dict[str, float]:
    total = sum(breakdown.values())
    if not total:
        return {}
    return {lang: round(bytes_ / total * 100, 1) for lang, bytes_ in breakdown.items()}


async def _to_api_payload(
    fetched: FetchedRepo,
    summarizer: RepoSummarizer,
    embedder: EmbeddingGenerator,
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
    ai_dev_skills = assign_dimension(tags, AI_DEV_SKILLS)
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
        'fork_sync_state': fetched.fork_sync_state,
        'behind_by': fetched.behind_by,
        'ahead_by': fetched.ahead_by,
        'commits_last_7_days': commit_stats['last7Days'],
        'commits_last_30_days': commit_stats['last30Days'],
        'commits_last_90_days': commit_stats['last90Days'],
        'readme_summary': summary,
        'activity_score': min(100, commit_stats['last30Days'] * 5 + commit_stats['last7Days'] * 10),
        'tags': tags,
        'categories': categories_list,
        'builders': [builder],
        'ai_dev_skills': ai_dev_skills,
        'pm_skills': pm_skills,
        'languages': languages_list,
        'commits': commits_list,
    }


async def run_ingestion(mode: RunMode, fix_repos: list[str] | None = None) -> None:
    settings = get_settings()
    start_time = time.time()

    console.rule(f'[bold blue]Reporium Ingestion — {mode.value.capitalize()} Mode[/bold blue]')

    db = CacheDatabase(settings.cache_db_path)
    await db.init()

    rate_limiter = RateLimitManager(min_buffer=settings.min_rate_limit_buffer)
    summarizer = RepoSummarizer()
    embedder = EmbeddingGenerator()
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
                payload = await _to_api_payload(fetched, summarizer, embedder)
                payloads.append(payload)

        enriched_count = len(payloads)
        console.print(f'Enriching with AI... [green]✓[/green]  {enriched_count} repos enriched')

        # Post to API
        with console.status('Posting to API...'):
            result = await api_client.upsert_repos(payloads)
            repos_updated = result.upserted

        console.print(f'Posting to API... [green]✓[/green]  {repos_updated} repos updated')
        if result.errors:
            for err in result.errors[:5]:
                console.print(f'  [red]⚠ {err}[/red]')

        # Post trend snapshot and gap analysis on weekly/full runs
        if mode in (RunMode.WEEKLY, RunMode.FULL):
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
                repos = args[i + 1:]
                break
        if not repos:
            console.print('[red]Usage: python -m ingestion fix --repos repo1 repo2[/red]')
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
