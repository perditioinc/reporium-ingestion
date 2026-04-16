import asyncio
import json
from datetime import datetime, timezone, timedelta
from typing import Any

from .client import GitHubClient, GitHubRepo
from ..cache.database import CacheDatabase
from ..cache.models import RepoCacheRow
from ..config import RunMode, get_settings


class FetchedRepo:
    """All data collected for a single repo across all cache tiers."""

    def __init__(self, github_repo: GitHubRepo, cache: RepoCacheRow | None):
        self.github_repo = github_repo
        self.cache = cache or RepoCacheRow(name=github_repo.name)

        # Populated during fetch
        self.readme: str | None = None
        self.commits: list[dict] = []
        self.latest_release: dict | None = None
        self.languages: dict[str, int] = {}
        self.fork_sync_state: str | None = None
        self.behind_by: int = 0
        self.ahead_by: int = 0
        self.upstream_created_at: str | None = None
        self.upstream_last_push_at: str | None = None
        self.original_owner: str | None = None
        self.parent_stars: int = 0
        self.parent_forks: int = 0
        self.parent_archived: bool = False
        self.dependencies: list[str] = []
        self.dep_source_file: str | None = None
        self.has_tests: bool = False
        self.has_ci: bool = False


class RepoFetcher:
    """Orchestrates fetching across cache tiers based on run mode."""

    def __init__(self, client: GitHubClient, db: CacheDatabase):
        self.client = client
        self.db = db
        self.settings = get_settings()

    async def fetch_repo_list(self) -> list[GitHubRepo]:
        return await self.client.get_repos(self.settings.gh_username)

    async def fetch_changed_repos(
        self,
        repos: list[GitHubRepo],
        mode: RunMode,
    ) -> list[FetchedRepo]:
        """
        Determine which repos need updating and fetch their data.
        Returns enriched FetchedRepo objects.
        """
        results: list[FetchedRepo] = []

        # Split into concurrent batches respecting rate limit
        concurrency = 1 if self.client.rate_limiter.use_sequential else self.settings.max_concurrency
        semaphore = asyncio.Semaphore(concurrency)

        tasks = [self._fetch_single(repo, mode, semaphore) for repo in repos]
        fetched = await asyncio.gather(*tasks)
        return [f for f in fetched if f is not None]

    async def _fetch_single(
        self, repo: GitHubRepo, mode: RunMode, semaphore: asyncio.Semaphore
    ) -> FetchedRepo | None:
        async with semaphore:
            cache = await self.db.get_repo(repo.name)
            fetched = FetchedRepo(repo, cache)

            # PERMANENT tier — fetch once, never again
            if await self.db.needs_permanent_fetch(repo.name):
                await self._fetch_permanent(fetched)

            # Determine if DAILY tier is needed
            daily_needed = await self.db.needs_daily_fetch(repo.name, repo.updated_at)

            if daily_needed:
                await self._fetch_daily(fetched)

            # WEEKLY tier
            if mode in (RunMode.WEEKLY, RunMode.FULL) and await self.db.needs_weekly_fetch(repo.name):
                await self._fetch_weekly(fetched)
            elif cache and cache.language_breakdown:
                try:
                    fetched.languages = json.loads(cache.language_breakdown)
                except Exception:
                    pass

            # REALTIME tier (fork sync)
            if mode == RunMode.FULL and repo.is_fork:
                await self._fetch_fork_sync(fetched)
            elif mode == RunMode.WEEKLY and repo.is_fork:
                # Only for repos updated in last 30 days
                cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
                if repo.updated_at >= cutoff:
                    await self._fetch_fork_sync(fetched)

            # Load remaining from cache for unchanged tiers
            if not daily_needed and cache:
                if cache.readme_content:
                    fetched.readme = cache.readme_content
                if cache.recent_commits:
                    try:
                        fetched.commits = json.loads(cache.recent_commits)
                    except Exception:
                        pass
                if cache.latest_release:
                    try:
                        fetched.latest_release = json.loads(cache.latest_release)
                    except Exception:
                        pass

            if cache:
                fetched.fork_sync_state = cache.fork_sync_state
                fetched.behind_by = cache.behind_by or 0
                fetched.ahead_by = cache.ahead_by or 0
                fetched.parent_stars = cache.parent_stars or 0
                fetched.parent_forks = cache.parent_forks or 0
                fetched.parent_archived = bool(cache.parent_archived)
                fetched.upstream_created_at = cache.upstream_created_at
                fetched.original_owner = cache.original_owner

            # Persist updated cache
            await self._persist_cache(fetched, daily_needed)
            return fetched

    async def _fetch_permanent(self, fetched: FetchedRepo) -> None:
        repo = fetched.github_repo
        if repo.is_fork:
            fork_info = await self.client.get_fork_info(repo.owner, repo.name)
            if fork_info:
                fetched.upstream_created_at = fork_info.upstream_created_at
                fetched.upstream_last_push_at = fork_info.upstream_pushed_at
                fetched.original_owner = fork_info.upstream_owner
                fetched.parent_stars = fork_info.parent_stars
                fetched.parent_forks = fork_info.parent_forks
                fetched.parent_archived = fork_info.parent_archived
        else:
            fetched.original_owner = repo.owner
            fetched.upstream_created_at = repo.created_at
            # For repos you own outright, "upstream" IS you, so both push timestamps
            # coincide. Setting this keeps the timeline card populated for built repos.
            fetched.upstream_last_push_at = repo.pushed_at

    async def _fetch_daily(self, fetched: FetchedRepo) -> None:
        repo = fetched.github_repo
        since = datetime.now(timezone.utc) - timedelta(days=90)

        readme, commits, release = await asyncio.gather(
            self.client.get_readme(repo.owner, repo.name),
            self.client.get_commits_since(repo.owner, repo.name, since),
            self.client.get_latest_release(repo.owner, repo.name),
        )
        fetched.readme = readme
        fetched.commits = [c.model_dump() for c in commits]
        fetched.latest_release = release.model_dump() if release else None

        # Fetch dependency file (bundled with daily fetch — no extra API calls budget)
        from ..extractors.dependencies import DEPENDENCY_FILES, PARSERS
        target_owner = repo.owner
        target_repo = repo.name
        # For forks, try upstream first
        if repo.is_fork and repo.forked_from:
            parts = repo.forked_from.split('/')
            if len(parts) == 2:
                target_owner, target_repo = parts[0], parts[1]

        for filepath in DEPENDENCY_FILES:
            content = await self.client.get_file(target_owner, target_repo, filepath)
            if content:
                parser = PARSERS.get(filepath)
                if parser:
                    fetched.dependencies = parser(content)
                    fetched.dep_source_file = f'{target_owner}/{target_repo}/{filepath}'
                break

        # Detect has_tests and has_ci from repo tree (top-level paths only)
        try:
            branch = repo.default_branch or 'main'
            paths = await self.client.get_tree_paths(repo.owner, repo.name, branch)
            if not paths and branch != 'master':
                paths = await self.client.get_tree_paths(repo.owner, repo.name, 'master')

            test_indicators = {'test', 'tests', 'spec', 'specs', '__tests__', 'test_', 'pytest.ini', 'jest.config.js', 'jest.config.ts'}
            ci_indicators = {'.github', '.circleci', '.travis.yml', 'Jenkinsfile', '.gitlab-ci.yml', 'azure-pipelines.yml'}

            fetched.has_tests = any(
                p.lower() in test_indicators or p.lower().startswith('test_')
                for p in paths
            )
            fetched.has_ci = any(p in ci_indicators for p in paths)
        except Exception:
            pass  # Non-critical — has_tests/has_ci default to False

    async def _fetch_weekly(self, fetched: FetchedRepo) -> None:
        repo = fetched.github_repo
        languages = await self.client.get_languages(repo.owner, repo.name)
        fetched.languages = languages

        if repo.is_fork and not fetched.upstream_created_at:
            await self._fetch_permanent(fetched)

    async def _fetch_fork_sync(self, fetched: FetchedRepo) -> None:
        repo = fetched.github_repo
        if not repo.is_fork or not fetched.original_owner:
            return

        # Fork sync uses max 1 concurrent request, 1000ms delay
        await asyncio.sleep(1.0)

        forked_from = repo.forked_from or f'{fetched.original_owner}/{repo.name}'
        parts = forked_from.split('/')
        if len(parts) != 2:
            return

        sync = await self.client.get_fork_sync(
            fork_owner=repo.owner,
            fork_repo=repo.name,
            upstream_owner=parts[0],
            upstream_repo=parts[1],
            branch=repo.default_branch,
        )
        fetched.fork_sync_state = sync.state
        fetched.behind_by = sync.behind_by
        fetched.ahead_by = sync.ahead_by

    async def _persist_cache(self, fetched: FetchedRepo, daily_updated: bool) -> None:
        now = datetime.now(timezone.utc).isoformat()
        repo = fetched.github_repo

        existing = await self.db.get_repo(repo.name) or RepoCacheRow(name=repo.name)

        updated = RepoCacheRow(
            name=repo.name,
            github_updated_at=repo.updated_at,

            # PERMANENT
            upstream_created_at=fetched.upstream_created_at or existing.upstream_created_at,
            original_owner=fetched.original_owner or existing.original_owner,
            forked_from=repo.forked_from or existing.forked_from,
            permanent_fetched_at=now if fetched.upstream_created_at else existing.permanent_fetched_at,

            # WEEKLY
            parent_stars=fetched.parent_stars if fetched.languages else existing.parent_stars,
            parent_forks=fetched.parent_forks if fetched.languages else existing.parent_forks,
            parent_archived=fetched.parent_archived if fetched.languages else existing.parent_archived,
            language_breakdown=json.dumps(fetched.languages) if fetched.languages else existing.language_breakdown,
            weekly_fetched_at=now if fetched.languages else existing.weekly_fetched_at,

            # DAILY
            readme_content=fetched.readme if daily_updated else existing.readme_content,
            recent_commits=json.dumps(fetched.commits) if daily_updated else existing.recent_commits,
            latest_release=json.dumps(fetched.latest_release) if daily_updated else existing.latest_release,
            daily_fetched_at=now if daily_updated else existing.daily_fetched_at,

            # REALTIME
            fork_sync_state=fetched.fork_sync_state or existing.fork_sync_state,
            behind_by=fetched.behind_by,
            ahead_by=fetched.ahead_by,
            sync_fetched_at=now if fetched.fork_sync_state else existing.sync_fetched_at,
        )
        await self.db.upsert_repo(updated)
