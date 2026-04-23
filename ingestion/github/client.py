import asyncio
import httpx
from datetime import datetime, timezone
from pydantic import BaseModel
from typing import Any

from .rate_limit import RateLimitManager
from ..config import get_settings
from ..cache.database import CacheDatabase


class GitHubRepo(BaseModel):
    name: str
    full_name: str
    owner: str
    description: str | None = None
    is_fork: bool = False
    is_private: bool = False
    forked_from: str | None = None
    primary_language: str | None = None
    github_url: str
    stars: int = 0
    forks_count: int = 0
    open_issues_count: int = 0
    is_archived: bool = False
    topics: list[str] = []
    updated_at: str
    created_at: str
    pushed_at: str | None = None
    default_branch: str = 'main'
    license_spdx: str | None = None


class ForkInfo(BaseModel):
    upstream_owner: str
    upstream_repo: str
    upstream_created_at: str
    # Upstream's last push (parent repo's pushed_at) — powers the
    # "upstream activity" row of the timeline on the repo detail page.
    upstream_pushed_at: str | None = None
    parent_stars: int = 0
    parent_forks: int = 0
    parent_archived: bool = False
    default_branch: str = 'main'


class ForkSyncStatus(BaseModel):
    state: str  # 'up-to-date', 'behind', 'ahead', 'diverged', 'unknown'
    behind_by: int = 0
    ahead_by: int = 0


class Commit(BaseModel):
    sha: str
    message: str
    author: str
    committed_at: str
    url: str


class Release(BaseModel):
    tag_name: str
    name: str | None = None
    published_at: str
    url: str


class GitHubClient:
    """
    Rate-limit aware GitHub API client.

    - Tracks rate limit remaining from response headers
    - Automatic exponential backoff on 429/403
    - Concurrency: max 2 simultaneous requests
    - Delay: 500ms between requests minimum
    - On abuse detection (429): wait 30s, retry once, then skip with 'unknown'
    - Logs every API call to SQLite for debugging
    """

    BASE_URL = 'https://api.github.com'

    def __init__(self, rate_limiter: RateLimitManager, db: CacheDatabase):
        self.rate_limiter = rate_limiter
        self.db = db
        settings = get_settings()
        self._token = settings.gh_token
        self._delay_s = settings.request_delay_ms / 1000.0
        self._semaphore = asyncio.Semaphore(settings.max_concurrency)
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            headers={
                'Authorization': f'Bearer {self._token}',
                'Accept': 'application/vnd.github+json',
                'X-GitHub-Api-Version': '2022-11-28',
            },
            timeout=30.0,
        )
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()

    async def _request(self, method: str, path: str, **kwargs) -> dict | list | None:
        url = f'{self.BASE_URL}{path}'
        async with self._semaphore:
            await asyncio.sleep(self._delay_s)

            # Pause if rate limit critically low
            should_pause, wait_secs = await self.rate_limiter.should_pause()
            if should_pause:
                await asyncio.sleep(wait_secs)

            for attempt in range(2):
                try:
                    resp = await self._client.request(method, url, **kwargs)
                    self._update_rate_limit(resp)
                    self.rate_limiter.record_call()

                    if self.db is not None:
                        await self.db.log_api_call(
                            endpoint=path,
                            status_code=resp.status_code,
                            rate_limit_remaining=self._parse_remaining(resp),
                        )

                    if resp.status_code == 200:
                        return resp.json()
                    elif resp.status_code == 404:
                        return None
                    elif resp.status_code in (429, 403):
                        if attempt == 0:
                            await asyncio.sleep(30)
                            continue
                        return None
                    elif resp.status_code == 204:
                        return {}
                    else:
                        return None
                except httpx.RequestError:
                    if attempt == 0:
                        await asyncio.sleep(5)
                        continue
                    return None
        return None

    def _parse_remaining(self, resp: httpx.Response) -> int | None:
        try:
            return int(resp.headers.get('x-ratelimit-remaining', 0))
        except (ValueError, TypeError):
            return None

    def _update_rate_limit(self, resp: httpx.Response) -> None:
        try:
            remaining = int(resp.headers.get('x-ratelimit-remaining', 5000))
            limit = int(resp.headers.get('x-ratelimit-limit', 5000))
            reset_ts = int(resp.headers.get('x-ratelimit-reset', 0))
            reset_at = datetime.fromtimestamp(reset_ts, tz=timezone.utc)
            self.rate_limiter.update(remaining, limit, reset_at)
        except (ValueError, TypeError):
            pass

    # ── Public API ────────────────────────────────────────────────────────────

    async def get_repos(self, username: str) -> list[GitHubRepo]:
        repos: list[GitHubRepo] = []
        page = 1
        while True:
            data = await self._request('GET', f'/users/{username}/repos', params={
                'per_page': 100,
                'page': page,
                'type': 'all',
                'sort': 'updated',
            })
            if not data:
                break
            for r in data:
                # Skip private repos — only public repos belong in Reporium
                if r.get('private', False):
                    continue
                _raw_spdx = r.get('license', {}).get('spdx_id') if r.get('license') else None
                license_spdx = _raw_spdx if _raw_spdx and _raw_spdx != 'NOASSERTION' else None
                repos.append(GitHubRepo(
                    name=r['name'],
                    full_name=r['full_name'],
                    owner=r['owner']['login'],
                    description=r.get('description'),
                    is_fork=r.get('fork', False),
                    is_private=False,  # confirmed above: we skip any private=True
                    forked_from=r['parent']['full_name'] if r.get('fork') and r.get('parent') else None,
                    primary_language=r.get('language'),
                    github_url=r['html_url'],
                    stars=r.get('stargazers_count', 0),
                    forks_count=r.get('forks_count', 0),
                    open_issues_count=r.get('open_issues_count', 0),
                    is_archived=r.get('archived', False),
                    topics=r.get('topics', []),
                    updated_at=r['updated_at'],
                    created_at=r['created_at'],
                    # pushed_at = timestamp of the user's last push to this repo.
                    # Falls back to updated_at if the GitHub response omits it (rare but defensive).
                    pushed_at=r.get('pushed_at') or r['updated_at'],
                    default_branch=r.get('default_branch', 'main'),
                    license_spdx=license_spdx,
                ))
            if len(data) < 100:
                break
            page += 1

        # Hydrate forked_from for every fork. GET /users/{user}/repos returns
        # the minimal-repository schema which does NOT include `parent`, so
        # the `r['parent']['full_name']` branch above evaluates to None for
        # every fork. That is the root cause of the 2026-04-23 regression
        # where recently-added forks showed `perditioinc` as the only builder
        # instead of the upstream owner. Secondary-fetch GET /repos/{owner}/{name}
        # (full Repository schema) via get_fork_info() to populate it.
        for repo in repos:
            if repo.is_fork and repo.forked_from is None:
                info = await self.get_fork_info(repo.owner, repo.name)
                if info:
                    repo.forked_from = f'{info.upstream_owner}/{info.upstream_repo}'

        return repos

    async def get_fork_info(self, owner: str, repo: str) -> ForkInfo | None:
        data = await self._request('GET', f'/repos/{owner}/{repo}')
        if not data or not data.get('fork'):
            return None
        parent = data.get('parent', {})
        return ForkInfo(
            upstream_owner=parent.get('owner', {}).get('login', 'unknown'),
            upstream_repo=parent.get('name', 'unknown'),
            upstream_created_at=parent.get('created_at', ''),
            upstream_pushed_at=parent.get('pushed_at'),
            parent_stars=parent.get('stargazers_count', 0),
            parent_forks=parent.get('forks_count', 0),
            parent_archived=parent.get('archived', False),
            default_branch=parent.get('default_branch', 'main'),
        )

    async def get_readme(self, owner: str, repo: str) -> str | None:
        data = await self._request('GET', f'/repos/{owner}/{repo}/readme')
        if not data:
            return None
        import base64
        content = data.get('content', '')
        try:
            return base64.b64decode(content.replace('\n', '')).decode('utf-8', errors='replace')
        except Exception:
            return None

    async def get_languages(self, owner: str, repo: str) -> dict[str, int]:
        data = await self._request('GET', f'/repos/{owner}/{repo}/languages')
        return data if isinstance(data, dict) else {}

    async def get_fork_sync(
        self, fork_owner: str, fork_repo: str,
        upstream_owner: str, upstream_repo: str, branch: str
    ) -> ForkSyncStatus:
        data = await self._request(
            'GET',
            f'/repos/{upstream_owner}/{upstream_repo}/compare/{upstream_owner}:{branch}...{fork_owner}:{branch}'
        )
        if not data:
            return ForkSyncStatus(state='unknown')
        status = data.get('status', 'unknown')
        ahead = data.get('ahead_by', 0)
        behind = data.get('behind_by', 0)
        if status == 'identical':
            state = 'up-to-date'
        elif behind > 0 and ahead > 0:
            state = 'diverged'
        elif behind > 0:
            state = 'behind'
        elif ahead > 0:
            state = 'ahead'
        else:
            state = 'up-to-date'
        return ForkSyncStatus(state=state, behind_by=behind, ahead_by=ahead)

    async def get_commits_since(self, owner: str, repo: str, since: datetime) -> list[Commit]:
        data = await self._request('GET', f'/repos/{owner}/{repo}/commits', params={
            'since': since.isoformat(),
            'per_page': 30,
        })
        if not data or not isinstance(data, list):
            return []
        commits = []
        for c in data[:30]:
            commit_data = c.get('commit', {})
            committer = commit_data.get('committer', {}) or {}
            author_info = commit_data.get('author', {}) or committer
            commits.append(Commit(
                sha=c['sha'][:8],
                message=commit_data.get('message', '').split('\n')[0][:100],
                author=author_info.get('name', 'unknown'),
                committed_at=committer.get('date', ''),
                url=c.get('html_url', ''),
            ))
        return commits

    async def get_latest_release(self, owner: str, repo: str) -> Release | None:
        data = await self._request('GET', f'/repos/{owner}/{repo}/releases/latest')
        if not data:
            return None
        return Release(
            tag_name=data.get('tag_name', ''),
            name=data.get('name'),
            published_at=data.get('published_at', ''),
            url=data.get('html_url', ''),
        )

    async def get_file(self, owner: str, repo: str, filepath: str) -> str | None:
        """Fetch a raw file from a repo. Returns text content or None if not found."""
        data = await self._request('GET', f'/repos/{owner}/{repo}/contents/{filepath}')
        if not data or not isinstance(data, dict):
            return None
        import base64 as _b64
        content = data.get('content', '')
        encoding = data.get('encoding', 'base64')
        if encoding == 'base64':
            try:
                return _b64.b64decode(content.replace('\n', '')).decode('utf-8', errors='replace')
            except Exception:
                return None
        return None

    async def get_tree_paths(self, owner: str, repo: str, branch: str = 'main') -> list[str]:
        """
        Get top-level file/directory names in a repo tree (shallow — just root level).
        Used for has_tests and has_ci detection. Returns [] on any error.
        """
        data = await self._request('GET', f'/repos/{owner}/{repo}/git/trees/{branch}', params={'recursive': '0'})
        if not data or not isinstance(data, dict):
            # Try 'master' if 'main' fails (handled by caller retry)
            return []
        tree = data.get('tree', [])
        return [item.get('path', '') for item in tree if item.get('path')]

    async def get_rate_limit(self) -> RateLimitManager:
        data = await self._request('GET', '/rate_limit')
        if data:
            core = data.get('resources', {}).get('core', {})
            remaining = core.get('remaining', 5000)
            limit = core.get('limit', 5000)
            reset_ts = core.get('reset', 0)
            reset_at = datetime.fromtimestamp(reset_ts, tz=timezone.utc)
            self.rate_limiter.update(remaining, limit, reset_at)
        return self.rate_limiter
