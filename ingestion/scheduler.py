import asyncio
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from .config import get_settings, RunMode

logger = logging.getLogger(__name__)


async def _run_mode(mode: RunMode) -> None:
    """Import here to avoid circular imports."""
    from .main import run_ingestion
    try:
        await run_ingestion(mode)
    except Exception as e:
        logger.error(f'Scheduled {mode.value} run failed: {e}')


async def run_quick() -> None:
    await _run_mode(RunMode.QUICK)


async def run_weekly() -> None:
    await _run_mode(RunMode.WEEKLY)


async def run_full() -> None:
    await _run_mode(RunMode.FULL)


def create_scheduler() -> AsyncIOScheduler:
    settings = get_settings()
    scheduler = AsyncIOScheduler()

    scheduler.add_job(
        run_quick,
        CronTrigger.from_crontab(settings.quick_schedule),
        id='quick',
        name='Quick incremental run',
        misfire_grace_time=3600,
    )
    scheduler.add_job(
        run_weekly,
        CronTrigger.from_crontab(settings.weekly_schedule),
        id='weekly',
        name='Weekly refresh run',
        misfire_grace_time=7200,
    )
    scheduler.add_job(
        run_full,
        CronTrigger.from_crontab(settings.full_schedule),
        id='full',
        name='Full monthly run',
        misfire_grace_time=14400,
    )

    return scheduler


async def start_scheduler() -> None:
    scheduler = create_scheduler()
    scheduler.start()
    settings = get_settings()

    from rich.console import Console
    from rich.table import Table
    console = Console()

    table = Table(title='Scheduled Jobs', show_header=True)
    table.add_column('Job')
    table.add_column('Schedule')
    table.add_row('Quick', settings.quick_schedule)
    table.add_row('Weekly', settings.weekly_schedule)
    table.add_row('Full', settings.full_schedule)
    console.print(table)
    console.print('[green]Scheduler running. Press Ctrl+C to stop.[/green]')

    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, asyncio.CancelledError):
        scheduler.shutdown()
        console.print('\n[yellow]Scheduler stopped.[/yellow]')
