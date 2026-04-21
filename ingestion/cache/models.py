from pydantic import BaseModel
from datetime import datetime
from enum import Enum


class CacheTier(str, Enum):
    PERMANENT = 'permanent'
    WEEKLY = 'weekly'
    DAILY = 'daily'
    REALTIME = 'realtime'


class RepoCacheRow(BaseModel):
    name: str
    github_updated_at: str | None = None

    # PERMANENT tier
    upstream_created_at: str | None = None
    original_owner: str | None = None
    forked_from: str | None = None
    permanent_fetched_at: str | None = None

    # WEEKLY tier
    parent_stars: int | None = None
    parent_forks: int | None = None
    parent_archived: bool | None = None
    language_breakdown: str | None = None  # JSON
    weekly_fetched_at: str | None = None

    # DAILY tier
    readme_content: str | None = None
    recent_commits: str | None = None  # JSON
    latest_release: str | None = None  # JSON
    daily_fetched_at: str | None = None

    # REALTIME tier
    fork_sync_state: str | None = None
    behind_by: int | None = None
    ahead_by: int | None = None
    sync_fetched_at: str | None = None


class IngestionRun(BaseModel):
    id: int | None = None
    started_at: str
    completed_at: str | None = None
    mode: str
    repos_processed: int = 0
    repos_updated: int = 0
    api_calls_made: int = 0
    rate_limit_hits: int = 0
    status: str = 'running'


CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS repo_cache (
  name              TEXT PRIMARY KEY,
  github_updated_at TEXT,

  upstream_created_at TEXT,
  original_owner    TEXT,
  forked_from       TEXT,
  permanent_fetched_at TEXT,

  parent_stars      INTEGER,
  parent_forks      INTEGER,
  parent_archived   INTEGER,
  language_breakdown TEXT,
  weekly_fetched_at TEXT,

  readme_content    TEXT,
  recent_commits    TEXT,
  latest_release    TEXT,
  daily_fetched_at  TEXT,

  fork_sync_state   TEXT,
  behind_by         INTEGER,
  ahead_by          INTEGER,
  sync_fetched_at   TEXT
);

CREATE TABLE IF NOT EXISTS ingestion_runs (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  started_at      TEXT NOT NULL,
  completed_at    TEXT,
  mode            TEXT NOT NULL,
  repos_processed INTEGER DEFAULT 0,
  repos_updated   INTEGER DEFAULT 0,
  api_calls_made  INTEGER DEFAULT 0,
  rate_limit_hits INTEGER DEFAULT 0,
  status          TEXT DEFAULT 'running'
);

CREATE TABLE IF NOT EXISTS api_call_log (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  timestamp   TEXT NOT NULL,
  endpoint    TEXT NOT NULL,
  status_code INTEGER,
  rate_limit_remaining INTEGER
);
"""
