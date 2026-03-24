import logging
from typing import Any

import httpx
from pydantic import BaseModel

from ..config import get_settings
from ..analysis.trends import TrendSnapshot
from ..analysis.gaps import Gap


logger = logging.getLogger(__name__)


class UpsertResult(BaseModel):
    upserted: int
    errors: list[str] = []


class ReporiumAPIClient:
    """
    Client for writing data to reporium-api.
    All writes are batched and retried on failure.
    Auth: Bearer token in Authorization header.
    """

    BATCH_SIZE = 50

    def __init__(self):
        self.settings = get_settings()
        self._base_url = self.settings.reporium_api_url.rstrip('/')
        self._headers = {
            'Authorization': f'Bearer {self.settings.reporium_api_key}',
            'Content-Type': 'application/json',
        }

    async def check_connection(self) -> bool:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(f'{self._base_url}/health')
                return resp.status_code == 200
        except Exception:
            return False

    async def upsert_repos(self, repos: list[dict]) -> UpsertResult:
        """Batch upsert repos. Splits into chunks of BATCH_SIZE."""
        total_upserted = 0
        all_errors: list[str] = []

        for i in range(0, len(repos), self.BATCH_SIZE):
            batch = repos[i:i + self.BATCH_SIZE]
            result = await self._upsert_batch(batch)
            total_upserted += result.upserted
            all_errors.extend(result.errors)

        return UpsertResult(upserted=total_upserted, errors=all_errors)

    async def _upsert_batch(self, repos: list[dict]) -> UpsertResult:
        for attempt in range(3):
            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    resp = await client.post(
                        f'{self._base_url}/ingest/repos',
                        json=repos,
                        headers=self._headers,
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        return UpsertResult(
                            upserted=data.get('upserted', len(repos)),
                            errors=data.get('errors', []),
                        )
                    elif resp.status_code == 401:
                        return UpsertResult(upserted=0, errors=['Unauthorized — check REPORIUM_API_KEY'])
                    else:
                        if attempt < 2:
                            import asyncio
                            await asyncio.sleep(2 ** attempt)
                            continue
                        return UpsertResult(upserted=0, errors=[f'HTTP {resp.status_code}: {resp.text[:200]}'])
            except Exception as e:
                if attempt < 2:
                    import asyncio
                    await asyncio.sleep(2 ** attempt)
                    continue
                return UpsertResult(upserted=0, errors=[str(e)])
        return UpsertResult(upserted=0, errors=['Max retries exceeded'])

    async def post_trend_snapshot(self, snapshot: TrendSnapshot) -> None:
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                await client.post(
                    f'{self._base_url}/ingest/trends/snapshot',
                    json=snapshot.model_dump(),
                    headers=self._headers,
                )
        except Exception:
            logger.warning(
                "Failed to post trend snapshot to reporium-api",
                extra={"snapshot_captured_at": snapshot.captured_at},
                exc_info=True,
            )

    async def post_gap_analysis(self, gaps: list[Gap]) -> None:
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                await client.post(
                    f'{self._base_url}/ingest/gaps',
                    json=[g.model_dump() for g in gaps],
                    headers=self._headers,
                )
        except Exception:
            logger.warning(
                "Failed to post gap analysis to reporium-api",
                extra={"gap_count": len(gaps)},
                exc_info=True,
            )

    async def log_run(self, run_data: dict) -> None:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                await client.post(
                    f'{self._base_url}/ingest/log',
                    json=run_data,
                    headers=self._headers,
                )
        except Exception:
            logger.warning(
                "Failed to log ingestion run to reporium-api",
                extra={"run_status": run_data.get("status"), "run_mode": run_data.get("mode")},
                exc_info=True,
            )
