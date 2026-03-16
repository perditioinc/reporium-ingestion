import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone

from ..config import RunMode


@dataclass
class RateLimitStatus:
    remaining: int
    limit: int
    reset_at: datetime
    used: int = 0


@dataclass
class BudgetCheck:
    ok: bool
    available: int
    wait_seconds: int = 0
    message: str = ''


class RateLimitManager:
    """
    Tracks GitHub API rate limit and prevents hitting limits.

    Strategy:
    - Always check remaining before batch operations
    - If remaining < buffer: pause until reset
    - If remaining < 500: switch to sequential (no concurrency)
    - Track calls per run in SQLite
    - Estimate calls needed before starting
    - Warn if estimated calls > available budget
    """

    def __init__(self, min_buffer: int = 100):
        self.min_buffer = min_buffer
        self._status: RateLimitStatus | None = None
        self._calls_this_run: int = 0

    def update(self, remaining: int, limit: int, reset_at: datetime) -> None:
        self._status = RateLimitStatus(
            remaining=remaining,
            limit=limit,
            reset_at=reset_at,
        )

    def record_call(self) -> None:
        self._calls_this_run += 1
        if self._status:
            self._status.remaining = max(0, self._status.remaining - 1)

    def reset_run_counter(self) -> None:
        self._calls_this_run = 0

    @property
    def calls_this_run(self) -> int:
        return self._calls_this_run

    @property
    def remaining(self) -> int:
        return self._status.remaining if self._status else 5000

    async def check_budget(self, estimated_calls: int) -> BudgetCheck:
        if self._status is None:
            return BudgetCheck(ok=True, available=5000, message='No rate limit data yet')

        available = self._status.remaining
        if available >= estimated_calls + self.min_buffer:
            return BudgetCheck(ok=True, available=available)

        if available < self.min_buffer:
            wait = self._seconds_until_reset()
            return BudgetCheck(
                ok=False,
                available=available,
                wait_seconds=wait,
                message=f'Rate limit critically low ({available} remaining). Reset in {wait}s.'
            )

        return BudgetCheck(
            ok=True,
            available=available,
            message=f'Warning: estimated {estimated_calls} calls but only {available} remaining'
        )

    async def should_pause(self) -> tuple[bool, int]:
        if self._status is None:
            return False, 0
        if self._status.remaining < self.min_buffer:
            wait = self._seconds_until_reset()
            return True, max(wait, 0)
        return False, 0

    @property
    def use_sequential(self) -> bool:
        return self._status is not None and self._status.remaining < 500

    def _seconds_until_reset(self) -> int:
        if self._status is None:
            return 0
        now = datetime.now(timezone.utc)
        delta = (self._status.reset_at - now).total_seconds()
        return max(int(delta) + 5, 0)

    def estimate_calls(self, repo_count: int, mode: RunMode) -> int:
        if mode == RunMode.QUICK:
            # ~7 for repo list + ~3 per updated repo (assume 10% changed)
            changed = max(1, repo_count // 10)
            return 7 + (changed * 3)
        elif mode == RunMode.WEEKLY:
            # Quick + parent stats + languages + fork sync for recent repos
            changed = max(1, repo_count // 10)
            recent = max(1, repo_count // 3)
            return 7 + (changed * 3) + (repo_count * 2) + recent
        elif mode == RunMode.FULL:
            # Everything for all repos
            return 7 + (repo_count * 5)
        return 7
