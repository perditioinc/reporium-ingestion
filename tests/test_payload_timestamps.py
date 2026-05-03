"""
Tests for ingestion payload timestamp fields.

KAN-DRAFT-trends-payload-timestamps: `_to_api_payload` was omitting
`github_created_at` and `forked_at`, which made the API write empty strings
into the corresponding columns.  The /trends "New This Week" filter, which
uses `github_created_at > NOW() - INTERVAL '7 days'`, then matched nothing,
even though the daily Cloud Run Job WAS topping the corpus up.

These tests pin the contract:
  - non-fork  → github_created_at present (ISO), forked_at is None
  - fork      → both fields present, equal (forked_at == created_at)

NOTE: `ingestion.github.client` and `ingestion.main` import
`ingestion.cache.database`, which is missing from `origin/dev` (the cache
package lives on `main` only — dev/main drift, not this ticket's scope).
We stub `ingestion.cache.*` in sys.modules before importing so this test
exercises the real `_to_api_payload` without depending on the missing pkg.
"""

import sys
import types

import pytest


def _install_cache_stub() -> None:
    """Provide minimal stand-ins for `ingestion.cache.database` / `models`.

    The real classes have rich state; we only need the names to satisfy
    `from ..cache.database import CacheDatabase` and
    `from ..cache.models import RepoCacheRow` at import time. Tests
    construct a `RepoCacheRow` via `FetchedRepo`'s `cache=None` fallback,
    which only ever calls `RepoCacheRow(name=...)`.
    """
    if 'ingestion.cache' not in sys.modules:
        cache_pkg = types.ModuleType('ingestion.cache')
        cache_pkg.__path__ = []  # mark as a package so submodule imports work
        sys.modules['ingestion.cache'] = cache_pkg

    if 'ingestion.cache.database' not in sys.modules:
        db_mod = types.ModuleType('ingestion.cache.database')

        class _StubCacheDatabase:
            def __init__(self, *a, **kw): ...
        db_mod.CacheDatabase = _StubCacheDatabase
        sys.modules['ingestion.cache.database'] = db_mod

    if 'ingestion.cache.models' not in sys.modules:
        models_mod = types.ModuleType('ingestion.cache.models')

        class _StubRepoCacheRow:
            def __init__(self, name=None, **kw):
                self.name = name
                self.github_updated_at = None
                self.daily_fetched_at = None
                self.has_tests = False
                self.has_ci = False
                self.dependencies = []
                self.dep_source_file = None
        models_mod.RepoCacheRow = _StubRepoCacheRow
        sys.modules['ingestion.cache.models'] = models_mod


_install_cache_stub()

from ingestion.github.client import GitHubRepo  # noqa: E402
from ingestion.github.fetcher import FetchedRepo  # noqa: E402
from ingestion.main import _to_api_payload  # noqa: E402


class _NoOpSummarizer:
    """Minimal stand-in for RepoSummarizer that doesn't touch Ollama."""

    async def summarize(self, repo_name, readme, tags):
        return None


def _make_github_repo(*, is_fork: bool, created_at: str = '2026-04-15T10:00:00Z') -> GitHubRepo:
    return GitHubRepo(
        name='example-repo',
        full_name='perditioinc/example-repo' if not is_fork else 'perditioinc/forked-repo',
        owner='perditioinc',
        description='example',
        is_fork=is_fork,
        is_private=False,
        forked_from='upstream/source-repo' if is_fork else None,
        primary_language='Python',
        github_url='https://github.com/perditioinc/example-repo',
        stars=10,
        forks_count=2,
        open_issues_count=0,
        is_archived=False,
        topics=[],
        updated_at='2026-04-25T10:00:00Z',
        created_at=created_at,
        pushed_at='2026-04-24T10:00:00Z',
        default_branch='main',
        license_spdx='MIT',
    )


@pytest.mark.no_db
@pytest.mark.asyncio
async def test_non_fork_payload_has_github_created_at_and_no_forked_at():
    """A non-fork repo should set github_created_at and leave forked_at as None.

    GitHub does not give a separate "forked_at" timestamp; for non-forks the
    field is meaningless, so we send None (not the empty string).
    """
    gh_repo = _make_github_repo(is_fork=False, created_at='2026-04-15T10:00:00Z')
    fetched = FetchedRepo(github_repo=gh_repo, cache=None)

    payload = await _to_api_payload(fetched, _NoOpSummarizer())

    assert 'github_created_at' in payload, 'payload must include github_created_at'
    assert payload['github_created_at'] == '2026-04-15T10:00:00Z'
    # forked_at must be present-as-None (never empty string), so the API
    # schema's `datetime | None` accepts it and the DB stores NULL.
    assert payload.get('forked_at') is None


@pytest.mark.no_db
@pytest.mark.asyncio
async def test_fork_payload_sets_both_timestamps_to_created_at():
    """A fork should set both github_created_at AND forked_at, equal to created_at.

    Convention: GitHub doesn't expose a "forked_at" timestamp separately.
    For forks, the repo's created_at IS when the fork was created, so we
    use it for both fields.  reporium-api/library_full.py reads `forked_at`
    to populate the outgoing `forkedAt` JSON field, which the trends page
    uses for its `forkedAt ?? createdAt` "added recently" filter.
    """
    gh_repo = _make_github_repo(is_fork=True, created_at='2026-04-20T08:30:00Z')
    fetched = FetchedRepo(github_repo=gh_repo, cache=None)

    payload = await _to_api_payload(fetched, _NoOpSummarizer())

    assert payload.get('github_created_at') == '2026-04-20T08:30:00Z'
    assert payload.get('forked_at') == '2026-04-20T08:30:00Z'
    assert payload['forked_at'] == payload['github_created_at']
