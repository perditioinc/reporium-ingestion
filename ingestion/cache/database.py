import aiosqlite
import json
import os
from datetime import datetime, timezone, timedelta
from typing import Any

from .models import RepoCacheRow, IngestionRun, CREATE_TABLES_SQL


class CacheDatabase:
    def __init__(self, db_path: str):
        self.db_path = db_path

    async def init(self) -> None:
        os.makedirs(os.path.dirname(self.db_path) if os.path.dirname(self.db_path) else '.', exist_ok=True)
        async with aiosqlite.connect(self.db_path) as db:
            await db.executescript(CREATE_TABLES_SQL)
            await db.commit()

    # ── repo_cache ──────────────────────────────────────────────────────────

    async def get_repo(self, name: str) -> RepoCacheRow | None:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute('SELECT * FROM repo_cache WHERE name = ?', (name,)) as cur:
                row = await cur.fetchone()
                return RepoCacheRow(**dict(row)) if row else None

    async def get_all_repos(self) -> list[RepoCacheRow]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute('SELECT * FROM repo_cache') as cur:
                rows = await cur.fetchall()
                return [RepoCacheRow(**dict(r)) for r in rows]

    async def upsert_repo(self, row: RepoCacheRow) -> None:
        data = row.model_dump()
        cols = ', '.join(data.keys())
        placeholders = ', '.join(['?' for _ in data])
        updates = ', '.join([f'{k} = excluded.{k}' for k in data if k != 'name'])
        sql = f"""
            INSERT INTO repo_cache ({cols}) VALUES ({placeholders})
            ON CONFLICT(name) DO UPDATE SET {updates}
        """
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(sql, list(data.values()))
            await db.commit()

    async def needs_permanent_fetch(self, name: str) -> bool:
        row = await self.get_repo(name)
        return row is None or row.permanent_fetched_at is None

    async def needs_weekly_fetch(self, name: str) -> bool:
        row = await self.get_repo(name)
        if row is None or row.weekly_fetched_at is None:
            return True
        fetched = datetime.fromisoformat(row.weekly_fetched_at)
        return (datetime.now(timezone.utc) - fetched).days >= 7

    async def needs_daily_fetch(self, name: str, current_github_updated_at: str) -> bool:
        row = await self.get_repo(name)
        if row is None or row.daily_fetched_at is None:
            return True
        return row.github_updated_at != current_github_updated_at

    async def needs_sync_fetch(self, name: str) -> bool:
        row = await self.get_repo(name)
        if row is None or row.sync_fetched_at is None:
            return True
        fetched = datetime.fromisoformat(row.sync_fetched_at)
        return (datetime.now(timezone.utc) - fetched).total_seconds() < 3600

    # ── ingestion_runs ───────────────────────────────────────────────────────

    async def start_run(self, mode: str) -> int:
        now = datetime.now(timezone.utc).isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                'INSERT INTO ingestion_runs (started_at, mode, status) VALUES (?, ?, ?)',
                (now, mode, 'running')
            )
            await db.commit()
            return cur.lastrowid

    async def finish_run(self, run_id: int, repos_processed: int, repos_updated: int,
                         api_calls_made: int, rate_limit_hits: int, status: str = 'completed') -> None:
        now = datetime.now(timezone.utc).isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                '''UPDATE ingestion_runs
                   SET completed_at=?, repos_processed=?, repos_updated=?,
                       api_calls_made=?, rate_limit_hits=?, status=?
                   WHERE id=?''',
                (now, repos_processed, repos_updated, api_calls_made, rate_limit_hits, status, run_id)
            )
            await db.commit()

    async def get_last_run(self, mode: str | None = None) -> IngestionRun | None:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            if mode:
                sql = 'SELECT * FROM ingestion_runs WHERE mode=? ORDER BY id DESC LIMIT 1'
                args = (mode,)
            else:
                sql = 'SELECT * FROM ingestion_runs ORDER BY id DESC LIMIT 1'
                args = ()
            async with db.execute(sql, args) as cur:
                row = await cur.fetchone()
                return IngestionRun(**dict(row)) if row else None

    # ── api_call_log ─────────────────────────────────────────────────────────

    async def log_api_call(self, endpoint: str, status_code: int, rate_limit_remaining: int | None) -> None:
        now = datetime.now(timezone.utc).isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                'INSERT INTO api_call_log (timestamp, endpoint, status_code, rate_limit_remaining) VALUES (?,?,?,?)',
                (now, endpoint, status_code, rate_limit_remaining)
            )
            await db.commit()

    async def get_cache_stats(self) -> dict[str, Any]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute('SELECT COUNT(*) FROM repo_cache') as cur:
                total = (await cur.fetchone())[0]
            async with db.execute('SELECT COUNT(*) FROM repo_cache WHERE permanent_fetched_at IS NOT NULL') as cur:
                permanent = (await cur.fetchone())[0]
            async with db.execute('SELECT COUNT(*) FROM repo_cache WHERE daily_fetched_at IS NOT NULL') as cur:
                daily = (await cur.fetchone())[0]
            async with db.execute('SELECT COUNT(*) FROM ingestion_runs') as cur:
                runs = (await cur.fetchone())[0]
            async with db.execute('SELECT COUNT(*) FROM api_call_log') as cur:
                calls = (await cur.fetchone())[0]
        return {
            'total_repos': total,
            'permanent_cached': permanent,
            'daily_cached': daily,
            'total_runs': runs,
            'total_api_calls_logged': calls,
        }

    async def clean_stale(self, days: int = 90) -> int:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                'DELETE FROM repo_cache WHERE daily_fetched_at < ? AND daily_fetched_at IS NOT NULL',
                (cutoff,)
            )
            await db.commit()
            return cur.rowcount
