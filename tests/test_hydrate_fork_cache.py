"""Tests for _apply_cached_forked_from — fills fork `forked_from` from the
durable cache so hydrate_fork_parents only API-fetches genuinely new forks
(avoids ~1 GitHub call per fork / ~27min on every corpus-scale run)."""

from types import SimpleNamespace

import pytest

from ingestion.main import _apply_cached_forked_from

pytestmark = pytest.mark.no_db


def _repo(name, is_fork=True, forked_from=None):
    return SimpleNamespace(name=name, is_fork=is_fork, forked_from=forked_from)


def _cache_row(forked_from=None):
    return SimpleNamespace(forked_from=forked_from)


def test_fills_forked_from_from_cache_for_known_forks():
    repos = [_repo("fork-a"), _repo("fork-b")]
    cached = {"fork-a": _cache_row("up/a"), "fork-b": _cache_row("up/b")}
    filled = _apply_cached_forked_from(repos, cached)
    assert filled == 2
    assert repos[0].forked_from == "up/a"
    assert repos[1].forked_from == "up/b"


def test_leaves_new_forks_untouched_for_api_hydration():
    # A fork not present in cache must stay None so hydrate_fork_parents fetches it.
    repos = [_repo("brand-new-fork")]
    filled = _apply_cached_forked_from(repos, {})
    assert filled == 0
    assert repos[0].forked_from is None


def test_does_not_overwrite_already_set_forked_from():
    repos = [_repo("fork-a", forked_from="already/set")]
    cached = {"fork-a": _cache_row("up/a")}
    filled = _apply_cached_forked_from(repos, cached)
    assert filled == 0
    assert repos[0].forked_from == "already/set"


def test_ignores_non_forks():
    repos = [_repo("own-repo", is_fork=False)]
    cached = {"own-repo": _cache_row("up/x")}
    filled = _apply_cached_forked_from(repos, cached)
    assert filled == 0
    assert repos[0].forked_from is None


def test_skips_when_cache_has_no_forked_from():
    repos = [_repo("fork-a")]
    cached = {"fork-a": _cache_row(forked_from=None)}
    filled = _apply_cached_forked_from(repos, cached)
    assert filled == 0
    assert repos[0].forked_from is None
