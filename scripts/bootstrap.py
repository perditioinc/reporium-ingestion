"""
Bootstrap script — first-run setup and initial data load.
Checks all connections, then runs a full ingestion.

Usage:
    python scripts/bootstrap.py
    python -m ingestion bootstrap
"""
import asyncio
import sys
import os

# Allow running as a script from repo root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rich.console import Console
from rich.table import Table

console = Console()


async def run_bootstrap() -> None:
    from ingestion.config import get_settings, RunMode
    from ingestion.cache.database import CacheDatabase
    from ingestion.github.rate_limit import RateLimitManager
    from ingestion.github.client import GitHubClient
    from ingestion.enrichment.summarizer import RepoSummarizer
    from ingestion.api.client import ReporiumAPIClient
    from ingestion.main import run_ingestion

    console.rule('[bold blue]Reporium Ingestion — Bootstrap[/bold blue]')
    console.print()

    settings = get_settings()
    db = CacheDatabase(settings.cache_db_path)
    await db.init()

    # Check connections
    console.print('[bold]Checking connections...[/bold]')

    results: dict[str, tuple[bool, str]] = {}

    # GitHub
    rate_limiter = RateLimitManager()
    try:
        async with GitHubClient(rate_limiter, db) as gh:
            await gh.get_rate_limit()
        remaining = rate_limiter.remaining
        results['GitHub API'] = (True, f'connected ({remaining:,} calls remaining)')
    except Exception as e:
        results['GitHub API'] = (False, str(e))

    # reporium-api
    api_client = ReporiumAPIClient()
    try:
        ok = await api_client.check_connection()
        if ok:
            results['reporium-api'] = (True, f'connected ({settings.reporium_api_url})')
        else:
            results['reporium-api'] = (False, f'not reachable at {settings.reporium_api_url}')
    except Exception as e:
        results['reporium-api'] = (False, str(e))

    # Ollama
    summarizer = RepoSummarizer()
    try:
        available = await summarizer.check_available()
        if available:
            results['Ollama'] = (True, f'connected ({settings.ollama_url})')
        else:
            results['Ollama'] = (None, 'not available (AI summaries disabled)')
    except Exception as e:
        results['Ollama'] = (None, 'not available (AI summaries disabled)')

    # Print connection table
    for service, (ok, msg) in results.items():
        if ok is True:
            icon = '[green]✓[/green]'
        elif ok is False:
            icon = '[red]✗[/red]'
        else:
            icon = '[yellow]⚠[/yellow]'
        console.print(f'  {service:<20} {icon} {msg}')

    console.print()

    # Abort if critical connections fail
    if not results['GitHub API'][0]:
        console.print('[red bold]✗ Cannot proceed: GitHub API connection failed.[/red bold]')
        console.print('[dim]Check your GH_TOKEN in .env[/dim]')
        return

    if not results['reporium-api'][0]:
        console.print('[red bold]✗ Cannot proceed: reporium-api connection failed.[/red bold]')
        console.print('[dim]Check REPORIUM_API_URL and REPORIUM_API_KEY in .env[/dim]')
        console.print('[dim]Make sure reporium-api is running: cd ../reporium-api && uvicorn app.main:app[/dim]')
        return

    # Run full ingestion
    console.print('[bold]Starting full ingestion...[/bold]')
    console.print()
    await run_ingestion(RunMode.FULL)

    console.print()
    console.rule('[bold green]Bootstrap complete![/bold green]')
    console.print()

    stats = await db.get_cache_stats()
    console.print(f'  [green]{stats["total_repos"]}[/green] repos ingested')

    if results['Ollama'][0] is not True:
        console.print('  AI summaries: [yellow]disabled[/yellow] (start Ollama to enable)')
    else:
        console.print('  AI summaries: [green]enabled[/green]')

    console.print(f'  Next run: tomorrow at 9am (cron: {get_settings().quick_schedule})')
    console.print()
    console.print('[dim]To start scheduled runs: python -m ingestion schedule[/dim]')


if __name__ == '__main__':
    asyncio.run(run_bootstrap())
