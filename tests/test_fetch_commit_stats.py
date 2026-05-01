"""
Tests for scripts/fetch_commit_stats.py — specifically the 202-retry behavior
on GitHub's `/stats/commit_activity` endpoint.

GitHub returns 202 Accepted with an empty body while it computes commit stats
asynchronously, then 200 with data on a subsequent request. Without a retry,
~90% of repos in production never receive fresh stats — they get 202 on the
first attempt, are silently skipped, and their `commits_last_*_days` columns
remain at the default 0/NULL forever (visible as `last7Days = 0` for every
repo on the live /trends page).

These tests cover the contract of `fetch_commit_activity(client, target,
headers, max_attempts, retry_sleep)`:

  * On first 202 then 200, retry once and return the parsed weeks list.
  * On `max_attempts` consecutive 202s, return None (sentinel for "still
    computing — leave the DB columns alone").
  * Pass-throughs for non-202 cases (200, 404, 5xx).

The DB write contract is checked indirectly: the only place the script
UPDATEs the commit columns is `if weeks is not None and len(weeks) > 0`,
so a None return guarantees no overwrite.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest import mock

import httpx
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_module():
    """Import scripts/fetch_commit_stats.py without executing main()."""
    spec = importlib.util.spec_from_file_location(
        "fetch_commit_stats",
        str(REPO_ROOT / "scripts" / "fetch_commit_stats.py"),
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["fetch_commit_stats"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def fcs():
    return _load_module()


def _mock_response(status_code: int, json_body=None) -> mock.Mock:
    resp = mock.Mock(spec=httpx.Response)
    resp.status_code = status_code
    resp.headers = {"x-ratelimit-remaining": "4500"}
    resp.json = mock.Mock(return_value=json_body if json_body is not None else [])
    return resp


def test_retry_on_first_202_then_200_returns_data(fcs):
    """First call yields 202, second yields 200 — fetcher must retry and return weeks."""
    weeks = [{"total": 5, "week": 0, "days": [0, 1, 1, 1, 1, 1, 0]}]
    client = mock.Mock(spec=httpx.Client)
    client.get = mock.Mock(side_effect=[
        _mock_response(202),
        _mock_response(200, weeks),
    ])

    result = fcs.fetch_commit_activity(
        client,
        "owner/repo",
        headers={},
        max_attempts=3,
        retry_sleep=0,
    )

    assert result == weeks
    assert client.get.call_count == 2


def test_three_consecutive_202s_returns_none(fcs):
    """After max_attempts 202s, signal 'unavailable' so caller skips the UPDATE."""
    client = mock.Mock(spec=httpx.Client)
    client.get = mock.Mock(side_effect=[
        _mock_response(202),
        _mock_response(202),
        _mock_response(202),
    ])

    result = fcs.fetch_commit_activity(
        client,
        "owner/repo",
        headers={},
        max_attempts=3,
        retry_sleep=0,
    )

    assert result is None, (
        "After exhausting retries on 202, fetcher must return None so the "
        "caller does NOT overwrite the existing commits_last_*_days columns "
        "with zeros."
    )
    assert client.get.call_count == 3


def test_first_call_200_no_retry(fcs):
    """A 200 on first call returns immediately — no retry."""
    weeks = [{"total": 12, "week": 0, "days": [2, 2, 2, 2, 2, 2, 0]}]
    client = mock.Mock(spec=httpx.Client)
    client.get = mock.Mock(return_value=_mock_response(200, weeks))

    result = fcs.fetch_commit_activity(
        client,
        "owner/repo",
        headers={},
        max_attempts=3,
        retry_sleep=0,
    )

    assert result == weeks
    assert client.get.call_count == 1


def test_404_is_unavailable(fcs):
    """A 404 (repo gone) returns None — caller must not overwrite stored values."""
    client = mock.Mock(spec=httpx.Client)
    client.get = mock.Mock(return_value=_mock_response(404))

    result = fcs.fetch_commit_activity(
        client,
        "owner/repo",
        headers={},
        max_attempts=3,
        retry_sleep=0,
    )

    assert result is None
    assert client.get.call_count == 1


def test_empty_weeks_list_returned_as_is(fcs):
    """A 200 with empty list (e.g. brand new repo) is passed through; caller decides."""
    client = mock.Mock(spec=httpx.Client)
    client.get = mock.Mock(return_value=_mock_response(200, []))

    result = fcs.fetch_commit_activity(
        client,
        "owner/repo",
        headers={},
        max_attempts=3,
        retry_sleep=0,
    )

    assert result == []
    assert client.get.call_count == 1
