"""Fix #3 follow-up: the posted trend snapshot must be HONEST about whether its
tag/category counts cover the full corpus or only this run's budgeted slice.

The raw GitHub corpus does not carry the enriched tags/categories (those are
built only for the processed slice; the cache does not persist them), so the
counts genuinely come from the slice on a budgeted run. Rather than ship empty
counts (a regression) or silently undercount, we label the snapshot's coverage
via `counts_basis` so a consumer / the server can decide whether to trust the
counts or recompute full-corpus trends from the DB.
"""
import pytest

from ingestion.analysis.trends import build_trend_snapshot

pytestmark = pytest.mark.no_db

_REPO = {
    "tags": ["RAG"],
    "categories": [{"category_name": "AI Agents"}],
    "github_updated_at": "",
}


def test_counts_basis_full_corpus_when_all_repos_present():
    snap = build_trend_snapshot([_REPO, _REPO], total_repos_override=2)
    assert snap.counts_basis == "full_corpus"


def test_counts_basis_run_slice_when_override_exceeds_processed():
    snap = build_trend_snapshot([_REPO], total_repos_override=10)
    assert snap.counts_basis == "run_slice"
    assert snap.total_repos == 10  # true corpus size still reported


def test_counts_basis_full_corpus_when_no_override():
    snap = build_trend_snapshot([_REPO, _REPO])
    assert snap.counts_basis == "full_corpus"
