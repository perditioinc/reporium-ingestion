"""Cron-trigger logic for the APScheduler-driven runner (`python -m ingestion
schedule`), with all live infra mocked.

Root cause this guards against (live-reproduced 2026-06-13):
``ingestion/scheduler.py`` reads ``settings.quick_schedule`` /
``weekly_schedule`` / ``full_schedule`` and ``scripts/bootstrap.py`` prints
``get_settings().quick_schedule``, but those three fields were dropped from
``Settings`` in commit 86a5d0a (they existed in the original e286c87 release).
As shipped, ``create_scheduler()`` and ``bootstrap`` raise
``AttributeError: 'Settings' object has no attribute 'quick_schedule'`` the
moment they touch the schedule, so the entire scheduled-runner path is dead.

These are pure unit tests:

  * no GCP / Cloud Run / Cloud Scheduler - the scheduler is the in-process
    APScheduler runner, and ``run_ingestion`` (which would hit GitHub + the
    API) is monkeypatched to a no-op so the cron WIRING is exercised without
    any live ingest;
  * the three cron strings are validated as real 5-field crontabs via the same
    ``CronTrigger.from_crontab`` the production scheduler uses.
"""

from __future__ import annotations

import pytest

from ingestion.config import RunMode, Settings

# Pure unit tests: no PostgreSQL fixture.
pytestmark = pytest.mark.no_db


# -- Settings carry the three cron schedules the scheduler/bootstrap read ------


def _settings() -> Settings:
    # gh_token is the one required field; everything else defaults.
    return Settings(gh_token="dummy-token")


def test_settings_expose_three_schedule_fields():
    """The regression: scheduler.py + bootstrap.py read these three fields, so
    Settings MUST define them. Each default must be a non-empty string."""
    s = _settings()
    for attr in ("quick_schedule", "weekly_schedule", "full_schedule"):
        assert hasattr(s, attr), f"Settings is missing {attr}"
        val = getattr(s, attr)
        assert isinstance(val, str) and val.strip(), f"{attr} must be a non-empty crontab string"


def test_schedule_defaults_are_valid_crontabs():
    """Every default schedule must parse as a real 5-field crontab - otherwise
    create_scheduler() raises at startup. Uses the exact APScheduler parser the
    production scheduler uses."""
    from apscheduler.triggers.cron import CronTrigger

    s = _settings()
    for cron in (s.quick_schedule, s.weekly_schedule, s.full_schedule):
        # Raises ValueError on a malformed crontab; 5 space-separated fields.
        CronTrigger.from_crontab(cron)
        assert len(cron.split()) == 5, f"{cron!r} is not a 5-field crontab"


def test_schedule_fields_are_env_overridable(monkeypatch):
    """Ops override schedules via env vars (QUICK_SCHEDULE etc.). A custom
    crontab must flow through to the setting AND still parse."""
    from apscheduler.triggers.cron import CronTrigger

    monkeypatch.setenv("QUICK_SCHEDULE", "30 6 * * *")
    monkeypatch.setenv("WEEKLY_SCHEDULE", "15 1 * * 1")
    monkeypatch.setenv("FULL_SCHEDULE", "0 4 2 * *")

    s = Settings(gh_token="dummy-token")
    assert s.quick_schedule == "30 6 * * *"
    assert s.weekly_schedule == "15 1 * * 1"
    assert s.full_schedule == "0 4 2 * *"
    for cron in (s.quick_schedule, s.weekly_schedule, s.full_schedule):
        CronTrigger.from_crontab(cron)


# -- create_scheduler builds the three cron jobs without touching live infra ---


def _patch_settings(monkeypatch) -> Settings:
    """Force ingestion.scheduler.get_settings() to return a deterministic
    Settings, so the test never reads a real .env or GCP secret."""
    import ingestion.scheduler as sched_mod

    s = _settings()
    monkeypatch.setattr(sched_mod, "get_settings", lambda: s)
    return s


def test_create_scheduler_registers_quick_weekly_full(monkeypatch):
    """create_scheduler() wires exactly the quick/weekly/full cron jobs, each
    bound to its configured schedule. This is the call that used to die on the
    missing attribute."""
    s = _patch_settings(monkeypatch)

    from ingestion.scheduler import create_scheduler

    # create_scheduler() registers the jobs but does NOT start the event loop,
    # so we inspect the registered jobs directly (no shutdown needed - the
    # scheduler is never started, and shutting down an unstarted AsyncIOScheduler
    # raises SchedulerNotRunningError).
    scheduler = create_scheduler()
    jobs = {job.id: job for job in scheduler.get_jobs()}
    assert set(jobs) == {"quick", "weekly", "full"}

    # Each job's trigger must reflect the configured crontab. APScheduler
    # renders the cron fields, so compare on the resolved fields rather
    # than the raw string.
    from apscheduler.triggers.cron import CronTrigger

    expected = {
        "quick": s.quick_schedule,
        "weekly": s.weekly_schedule,
        "full": s.full_schedule,
    }
    for job_id, cron in expected.items():
        ref = CronTrigger.from_crontab(cron)
        got = jobs[job_id].trigger
        assert str(got) == str(ref), (
            f"{job_id} trigger {got} != expected {ref}"
        )


def test_create_scheduler_misfire_grace_widens_with_cadence(monkeypatch):
    """The rarer the run, the more misfire slack it gets (a missed monthly run
    is costlier to skip than a missed nightly). Lock the ordering so a refactor
    can't silently flatten it."""
    _patch_settings(monkeypatch)

    from ingestion.scheduler import create_scheduler

    scheduler = create_scheduler()
    grace = {job.id: job.misfire_grace_time for job in scheduler.get_jobs()}
    assert grace["quick"] < grace["weekly"] < grace["full"]


# -- Mode dispatch: each cron job routes to the right RunMode, no live ingest --


@pytest.mark.asyncio
async def test_run_weekly_dispatches_weekly_mode(monkeypatch):
    """run_weekly() must invoke run_ingestion with RunMode.WEEKLY - the
    Cloud Run job runs QUICK on its own cadence, so the runner's weekly path is
    the only thing that triggers the corpus-wide weekly refresh."""
    import ingestion.scheduler as sched_mod

    seen: list[RunMode] = []

    async def fake_run_ingestion(mode):
        seen.append(mode)

    # run_ingestion is imported lazily inside _run_mode from ingestion.main,
    # so patch it on the source module.
    import ingestion.main as main_mod
    monkeypatch.setattr(main_mod, "run_ingestion", fake_run_ingestion)

    await sched_mod.run_weekly()
    assert seen == [RunMode.WEEKLY]


@pytest.mark.asyncio
async def test_run_quick_and_full_dispatch_their_modes(monkeypatch):
    """run_quick -> QUICK, run_full -> FULL. Confirms the three cron entry points
    don't collapse to a single mode."""
    import ingestion.scheduler as sched_mod
    import ingestion.main as main_mod

    seen: list[RunMode] = []

    async def fake_run_ingestion(mode):
        seen.append(mode)

    monkeypatch.setattr(main_mod, "run_ingestion", fake_run_ingestion)

    await sched_mod.run_quick()
    await sched_mod.run_full()
    assert seen == [RunMode.QUICK, RunMode.FULL]


@pytest.mark.asyncio
async def test_scheduled_run_swallows_ingest_failure(monkeypatch):
    """A failing scheduled run must be caught and logged, not propagated - a
    raised exception inside an APScheduler job would otherwise be swallowed by
    the loop anyway, but the code explicitly guards it so the scheduler keeps
    running the other jobs. Verify the guard actually contains the error."""
    import ingestion.scheduler as sched_mod
    import ingestion.main as main_mod

    async def boom(mode):
        raise RuntimeError("simulated ingest blowup")

    monkeypatch.setattr(main_mod, "run_ingestion", boom)

    # Must NOT raise out of the scheduled entry point.
    await sched_mod.run_weekly()
