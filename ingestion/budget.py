"""Resumable-run budgeting + work selection for `python -m ingestion run`.

Root cause this addresses (live-verified; failed runs 2026-05-25 / 06-01 /
06-08 / 06-15, last clean ingestion 2026-05-18):

    The weekly "Manual Ingestion Run" workflow executes the
    ``reporium-enrichment`` Cloud Run JOB (``python -m ingestion run``). The job
    builds a payload for, and iterates over, the ENTIRE corpus (~1900 repos)
    every invocation. Even after the KAN-230 enrichment gate trims the Claude
    pass, the structural phases still walk all repos: the daily fetch, the
    per-payload summarizer pass, and especially the commit-stats refresh, which
    sleeps 0.5s per repo (`time.sleep(0.5)` in `_refresh_commit_stats_blocking`)
    => ~16 min of pure sleeping alone before any I/O. A single run therefore
    cannot finish inside the 3600s Cloud Run task timeout, gets KILLED, and the
    newly-forked repos at the back of the list never reach the live DB. The DB
    goes stale relative to GitHub.

Fix shape (NOT "raise the timeout"):

  * Make each invocation process only a BOUNDED, PRIORITISED slice of the
    pending set, sized to finish well under the timeout
    (``MAX_REPOS_PER_RUN`` + a wall-clock ``RUN_TIME_BUDGET_SECONDS``).
  * The durable PostgreSQL cache (KAN-230, PR #96) already records which repos
    have been fully processed (a ``repo_cache`` row with ``daily_fetched_at``
    set and a matching ``github_updated_at``). That row is the CHECKPOINT: a
    repo processed on a prior run is "done" and is skipped, so the NEXT
    scheduled (or immediately re-triggered) run continues with whatever is
    still pending. No new state to corrupt mid-batch: the cache is upserted
    per-repo inside the fetcher, so a crash after N repos leaves N checkpoints
    durably committed and the remainder pending.
  * Prioritise genuinely NEW repos (recent forks with no cache row) FIRST so
    freshness recovers in a single run even when a large backlog exists.
  * Emit a FRESHNESS SLO metric (age of the most-recently-updated repo on
    GitHub that is not yet checkpointed) so staleness is observable in logs.

This module is pure and synchronous (no network, no DB, no asyncio) so it is
trivially unit-testable with fakes and deterministic offline.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable


# -- Tunables (env-overridable; read at call time, not import) -----------------

# How many repos a single invocation is allowed to FULLY process (fetch +
# summarise + enrich + commit-stats + post). 0 disables the cap (legacy
# whole-corpus behaviour). The default is sized so the dominant cost -- the
# commit-stats refresh's 0.5s/repo sleep plus GitHub I/O -- stays well under the
# 3600s task timeout: 400 repos * ~0.5s sleep ~= 200s of sleeping, leaving
# ample headroom for fetch + Claude + API I/O. Newly-forked repos are
# prioritised, so freshness recovers in the first run even with a big backlog.
DEFAULT_MAX_REPOS_PER_RUN = 400

# Wall-clock budget for the per-repo work phase. When the elapsed run time
# crosses this, the pipeline stops queuing more repos for THIS invocation and
# exits cleanly (partial completion), leaving the rest pending for the next
# run. Defaults to 2700s = 45 min, a 900s (15 min) safety margin under the
# 3600s Cloud Run task timeout for the list/post/teardown phases.
DEFAULT_RUN_TIME_BUDGET_SECONDS = 2700

# Freshness SLO: how old (hours) the newest not-yet-checkpointed repo may be
# before we flag the corpus as stale. Observability only -- it never fails the
# run; it emits a metric/log the operator (or a probe) can alert on.
DEFAULT_FRESHNESS_SLO_HOURS = 48


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def max_repos_per_run() -> int:
    return _env_int("MAX_REPOS_PER_RUN", DEFAULT_MAX_REPOS_PER_RUN)


def run_time_budget_seconds() -> int:
    return _env_int("RUN_TIME_BUDGET_SECONDS", DEFAULT_RUN_TIME_BUDGET_SECONDS)


def freshness_slo_hours() -> int:
    return _env_int("FRESHNESS_SLO_HOURS", DEFAULT_FRESHNESS_SLO_HOURS)


# -- Helpers -------------------------------------------------------------------


def _parse_iso(value: str | None) -> datetime | None:
    """Best-effort parse of a GitHub ISO-8601 timestamp to an aware datetime.

    Returns None on missing/malformed input so callers can treat it as
    "unknown" rather than crash. Accepts the trailing-Z form GitHub emits.
    """
    if not value or not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def is_checkpointed(repo: Any, cached: dict[str, Any]) -> bool:
    """Has this repo already been fully processed on a prior run (the durable
    CHECKPOINT signal)?

    Mirrors ``main._needs_ai_enrichment`` / ``CacheDatabase.needs_daily_fetch``
    so the budget cannot drift from the fetch/enrich change-detection logic.
    A repo is checkpointed iff ALL of:

      * a durable cache row exists, AND
      * ``daily_fetched_at`` is set (it went through a full fetch at least
        once), AND
      * the cached ``github_updated_at`` equals the repo's CURRENT GitHub
        ``updated_at`` (GitHub has not changed it since).
    """
    row = cached.get(getattr(repo, "name", None))
    if row is None:
        return False
    if getattr(row, "daily_fetched_at", None) is None:
        return False
    return getattr(row, "github_updated_at", None) == getattr(repo, "updated_at", None)


def _is_brand_new(repo: Any, cached: dict[str, Any]) -> bool:
    """True when GitHub knows this repo but the durable cache has never seen it
    (no row at all) -- i.e. a freshly-added repo / recent fork."""
    return getattr(repo, "name", None) not in cached


# -- Public surface ------------------------------------------------------------


@dataclass
class WorkSelection:
    """The bounded, prioritised slice one invocation will fully process."""

    selected: list = field(default_factory=list)   # repos to process THIS run
    pending_total: int = 0                          # all repos needing work
    deferred: int = 0                               # pending - selected (next run)
    new_count: int = 0                              # brand-new repos in `selected`
    changed_count: int = 0                          # changed repos in `selected`
    capped: bool = False                            # was the cap actually hit?

    @property
    def is_complete(self) -> bool:
        """True when this run drains the entire pending set (nothing deferred)."""
        return self.deferred == 0


def select_work_for_run(
    all_repos: Iterable[Any],
    cached: dict[str, Any],
    *,
    max_repos: int | None = None,
) -> WorkSelection:
    """Partition the corpus into done/pending and return the prioritised slice
    this invocation should process.

    Pending = NOT ``is_checkpointed`` (brand-new OR changed-since-checkpoint).
    Priority order, so freshness recovers fastest:

      1. Brand-new repos (no cache row) -- newest GitHub ``updated_at`` first.
      2. Changed repos (cache row exists but GitHub moved) -- newest first.

    ``max_repos`` (env ``MAX_REPOS_PER_RUN`` by default) caps how many enter
    THIS run; the remainder are reported as ``deferred`` and picked up by the
    next run because their checkpoint is still absent/stale. ``max_repos <= 0``
    means "no cap" (process all pending -- legacy behaviour, opt-in).

    Pure function: no mutation of ``all_repos`` ordering, no I/O.
    """
    if max_repos is None:
        max_repos = max_repos_per_run()

    new_repos: list = []
    changed_repos: list = []
    for repo in all_repos:
        if is_checkpointed(repo, cached):
            continue
        if _is_brand_new(repo, cached):
            new_repos.append(repo)
        else:
            changed_repos.append(repo)

    # Newest-first within each tier; epoch-floor keeps unparseable timestamps
    # last (deterministic) instead of raising.
    _epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    key = lambda r: _parse_iso(getattr(r, "updated_at", None)) or _epoch
    new_repos.sort(key=key, reverse=True)
    changed_repos.sort(key=key, reverse=True)

    ordered = new_repos + changed_repos
    pending_total = len(ordered)

    if max_repos and max_repos > 0:
        selected = ordered[:max_repos]
    else:
        selected = ordered

    selected_set = {id(r) for r in selected}
    new_in_selected = sum(1 for r in new_repos if id(r) in selected_set)
    changed_in_selected = sum(1 for r in changed_repos if id(r) in selected_set)

    return WorkSelection(
        selected=selected,
        pending_total=pending_total,
        deferred=pending_total - len(selected),
        new_count=new_in_selected,
        changed_count=changed_in_selected,
        capped=bool(max_repos and max_repos > 0 and pending_total > max_repos),
    )


@dataclass
class FreshnessReport:
    """How stale is the live corpus relative to GitHub?

    ``lag_hours`` is the age (now - GitHub ``updated_at``) of the most-recently
    updated repo that is NOT yet checkpointed. None when nothing is pending
    (corpus fully fresh). ``breached`` compares ``lag_hours`` against the SLO.
    """

    pending_total: int = 0
    newest_pending_repo: str | None = None
    newest_pending_updated_at: str | None = None
    lag_hours: float | None = None
    slo_hours: int = DEFAULT_FRESHNESS_SLO_HOURS
    breached: bool = False

    def as_metrics(self) -> dict[str, Any]:
        """Flat dict for structured logging / metric emission."""
        return {
            "freshness_pending_total": self.pending_total,
            "freshness_newest_pending_repo": self.newest_pending_repo,
            "freshness_lag_hours": (
                round(self.lag_hours, 2) if self.lag_hours is not None else None
            ),
            "freshness_slo_hours": self.slo_hours,
            "freshness_slo_breached": self.breached,
        }


def compute_freshness(
    all_repos: Iterable[Any],
    cached: dict[str, Any],
    *,
    now: datetime | None = None,
    slo_hours: int | None = None,
) -> FreshnessReport:
    """Compute the freshness SLO from the not-yet-checkpointed repos.

    The "newest pending" repo is the freshest GitHub change we have NOT yet
    landed in the DB; its age is the freshness lag. If it exceeds ``slo_hours``
    the corpus is stale beyond budget and ``breached`` is True. Pure function;
    inject ``now`` for deterministic tests.
    """
    if now is None:
        now = datetime.now(timezone.utc)
    if slo_hours is None:
        slo_hours = freshness_slo_hours()

    pending = [r for r in all_repos if not is_checkpointed(r, cached)]
    report = FreshnessReport(pending_total=len(pending), slo_hours=slo_hours)
    if not pending:
        return report

    def _updated(r: Any) -> datetime:
        return _parse_iso(getattr(r, "updated_at", None)) or datetime(
            1970, 1, 1, tzinfo=timezone.utc
        )

    newest = max(pending, key=_updated)
    newest_dt = _parse_iso(getattr(newest, "updated_at", None))
    report.newest_pending_repo = getattr(newest, "name", None)
    report.newest_pending_updated_at = getattr(newest, "updated_at", None)
    if newest_dt is not None:
        report.lag_hours = max(0.0, (now - newest_dt).total_seconds() / 3600.0)
        report.breached = report.lag_hours > slo_hours
    return report
