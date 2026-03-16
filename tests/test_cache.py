import asyncio
import os
import tempfile
import pytest
import pytest_asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

from ingestion.cache.database import CacheDatabase
from ingestion.cache.models import RepoCacheRow


@pytest.fixture
def tmp_db(tmp_path):
    return str(tmp_path / 'test_cache.db')


@pytest.fixture
def db(tmp_db):
    return CacheDatabase(tmp_db)


@pytest.mark.asyncio
async def test_init_creates_tables(db):
    await db.init()
    stats = await db.get_cache_stats()
    assert stats['total_repos'] == 0


@pytest.mark.asyncio
async def test_unchanged_repo_uses_cache(db):
    """Repo with same github_updated_at should NOT trigger daily fetch."""
    await db.init()
    now = datetime.now(timezone.utc).isoformat()
    row = RepoCacheRow(
        name='my-repo',
        github_updated_at='2024-01-15T10:00:00Z',
        daily_fetched_at=now,
        readme_content='# Hello',
    )
    await db.upsert_repo(row)

    # Same updated_at — should NOT need daily fetch
    needs = await db.needs_daily_fetch('my-repo', '2024-01-15T10:00:00Z')
    assert needs is False


@pytest.mark.asyncio
async def test_changed_repo_triggers_fetch(db):
    """Repo with new github_updated_at should trigger daily fetch."""
    await db.init()
    now = datetime.now(timezone.utc).isoformat()
    row = RepoCacheRow(
        name='my-repo',
        github_updated_at='2024-01-15T10:00:00Z',
        daily_fetched_at=now,
        readme_content='# Hello',
    )
    await db.upsert_repo(row)

    # Different updated_at — should need daily fetch
    needs = await db.needs_daily_fetch('my-repo', '2024-02-01T10:00:00Z')
    assert needs is True


@pytest.mark.asyncio
async def test_new_repo_triggers_fetch(db):
    """Repo not in cache should trigger fetch."""
    await db.init()
    needs = await db.needs_daily_fetch('unknown-repo', '2024-01-01T00:00:00Z')
    assert needs is True


@pytest.mark.asyncio
async def test_permanent_fetched_not_refetched(db):
    """Repo with permanent_fetched_at set should not need permanent fetch."""
    await db.init()
    row = RepoCacheRow(
        name='my-repo',
        permanent_fetched_at=datetime.now(timezone.utc).isoformat(),
        original_owner='some-org',
    )
    await db.upsert_repo(row)
    needs = await db.needs_permanent_fetch('my-repo')
    assert needs is False


@pytest.mark.asyncio
async def test_weekly_fetch_needed_after_7_days(db):
    """Weekly fetch needed if last fetch was > 7 days ago."""
    await db.init()
    old_date = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
    row = RepoCacheRow(name='my-repo', weekly_fetched_at=old_date)
    await db.upsert_repo(row)
    needs = await db.needs_weekly_fetch('my-repo')
    assert needs is True


@pytest.mark.asyncio
async def test_weekly_fetch_not_needed_within_7_days(db):
    """Weekly fetch NOT needed if last fetch was < 7 days ago."""
    await db.init()
    recent = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
    row = RepoCacheRow(name='my-repo', weekly_fetched_at=recent)
    await db.upsert_repo(row)
    needs = await db.needs_weekly_fetch('my-repo')
    assert needs is False


@pytest.mark.asyncio
async def test_rate_limit_pause(monkeypatch):
    """Should pause when remaining < buffer."""
    from ingestion.github.rate_limit import RateLimitManager
    mgr = RateLimitManager(min_buffer=100)
    reset_at = datetime.now(timezone.utc) + timedelta(seconds=10)
    mgr.update(remaining=50, limit=5000, reset_at=reset_at)

    should, wait = await mgr.should_pause()
    assert should is True
    assert wait > 0


@pytest.mark.asyncio
async def test_rate_limit_no_pause_when_sufficient():
    """Should NOT pause when remaining > buffer."""
    from ingestion.github.rate_limit import RateLimitManager
    mgr = RateLimitManager(min_buffer=100)
    reset_at = datetime.now(timezone.utc) + timedelta(hours=1)
    mgr.update(remaining=1000, limit=5000, reset_at=reset_at)

    should, wait = await mgr.should_pause()
    assert should is False


@pytest.mark.asyncio
async def test_abuse_detection_backoff(monkeypatch):
    """429 response should trigger 30s wait and retry."""
    from ingestion.github.client import GitHubClient
    from ingestion.github.rate_limit import RateLimitManager

    sleep_calls = []

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr(asyncio, 'sleep', fake_sleep)

    db = MagicMock()
    db.log_api_call = AsyncMock()
    rate_limiter = RateLimitManager()
    rate_limiter.update(1000, 5000, datetime.now(timezone.utc) + timedelta(hours=1))

    # Mock httpx client
    mock_response_429 = MagicMock()
    mock_response_429.status_code = 429
    mock_response_429.headers = {
        'x-ratelimit-remaining': '0',
        'x-ratelimit-limit': '5000',
        'x-ratelimit-reset': str(int((datetime.now(timezone.utc) + timedelta(hours=1)).timestamp())),
    }

    mock_response_200 = MagicMock()
    mock_response_200.status_code = 200
    mock_response_200.headers = mock_response_429.headers
    mock_response_200.json = lambda: []

    call_count = [0]

    async def fake_request(method, url, **kwargs):
        if call_count[0] == 0:
            call_count[0] += 1
            return mock_response_429
        return mock_response_200

    client = GitHubClient(rate_limiter, db)
    client._client = MagicMock()
    client._client.request = fake_request

    result = await client._request('GET', '/repos/owner/repo')

    # Should have slept 30s for abuse detection
    assert 30 in sleep_calls


@pytest.mark.asyncio
async def test_upsert_and_retrieve(db):
    """Can upsert and retrieve a cache row."""
    await db.init()
    row = RepoCacheRow(
        name='test-repo',
        github_updated_at='2024-06-01T00:00:00Z',
        readme_content='# Test README',
        original_owner='testuser',
    )
    await db.upsert_repo(row)

    retrieved = await db.get_repo('test-repo')
    assert retrieved is not None
    assert retrieved.name == 'test-repo'
    assert retrieved.readme_content == '# Test README'
    assert retrieved.original_owner == 'testuser'


@pytest.mark.asyncio
async def test_run_tracking(db):
    """Can start and finish ingestion runs."""
    await db.init()
    run_id = await db.start_run('quick')
    assert run_id > 0

    await db.finish_run(run_id, repos_processed=100, repos_updated=10,
                        api_calls_made=50, rate_limit_hits=0)

    last = await db.get_last_run('quick')
    assert last is not None
    assert last.repos_processed == 100
    assert last.status == 'completed'
