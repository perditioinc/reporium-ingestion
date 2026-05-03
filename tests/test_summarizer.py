"""
Unit tests for the README fallback summarizer.

KAN-200 — the fallback floor (`_fallback_summary`) was `> 30` while the
KAN-191 probe rejects anything under 50 chars (`PROBE_SUMMARY_MIN_CHARS`).
Per KAN-196 RCA F3, that gap meant 4 of 20 sampled repos shipped fallback
summaries that the probe immediately failed. These tests pin the
post-fix contract:

  * fallback returns `None` when nothing clears the probe floor
  * fallback returns a string when the first substantive paragraph >= floor
  * the floor is configurable via `PROBE_SUMMARY_MIN_CHARS` (single source of
    truth shared with the probe)

The tests don't need a DB, an Ollama instance, or a network — they exercise
`_fallback_summary` directly. The `no_db` marker skips the conftest
DB-setup fixtures.
"""

import os

import pytest

# Settings load on first construction and require GH_TOKEN. Set a stub
# BEFORE importing the summarizer so a plain `RepoSummarizer()` works in
# isolated CI/local without secrets. Stays scoped to this test module.
os.environ.setdefault("GH_TOKEN", "test-token-not-used")

from ingestion.enrichment.summarizer import (  # noqa: E402
    RepoSummarizer,
    _fallback_min_chars,
)

pytestmark = pytest.mark.no_db


def _summarizer() -> RepoSummarizer:
    """Build a summarizer without triggering Ollama probing.

    `check_available` is the only thing in `__init__`/`summarize` that
    touches the network, and we never call `summarize` here, so a plain
    construction is safe.
    """
    return RepoSummarizer()


# ── floor reader ──────────────────────────────────────────────────────────────


def test_fallback_min_chars_default(monkeypatch):
    monkeypatch.delenv("PROBE_SUMMARY_MIN_CHARS", raising=False)
    assert _fallback_min_chars() == 50


def test_fallback_min_chars_reads_env(monkeypatch):
    monkeypatch.setenv("PROBE_SUMMARY_MIN_CHARS", "75")
    assert _fallback_min_chars() == 75


def test_fallback_min_chars_ignores_bad_values(monkeypatch):
    monkeypatch.setenv("PROBE_SUMMARY_MIN_CHARS", "not-an-int")
    assert _fallback_min_chars() == 50


# ── _fallback_summary contract ────────────────────────────────────────────────


def test_fallback_returns_none_when_readme_empty():
    assert _summarizer()._fallback_summary("") is None
    assert _summarizer()._fallback_summary(None) is None


def test_fallback_returns_none_when_no_paragraph_meets_floor(monkeypatch):
    """KAN-200: must NOT return a too-short string the probe will reject.

    Every paragraph here is below the 50-char floor, so the only correct
    behavior is `None` — a too-short fallback would be persisted to the DB
    and immediately fail the nightly probe (KAN-196 RCA F3).
    """
    monkeypatch.delenv("PROBE_SUMMARY_MIN_CHARS", raising=False)
    readme = "# Title\n\nShort.\n\nAlso short.\n\n![badge](x)\n"
    assert _summarizer()._fallback_summary(readme) is None


def test_fallback_returns_string_when_paragraph_meets_floor(monkeypatch):
    monkeypatch.delenv("PROBE_SUMMARY_MIN_CHARS", raising=False)
    long_para = (
        "A robust framework for autonomous agent orchestration that "
        "coordinates LLM tool calls and persists state across runs."
    )
    assert len(long_para) >= 50
    readme = f"# Title\n\n{long_para}\n"
    out = _summarizer()._fallback_summary(readme)
    assert out is not None
    assert out.startswith("A robust framework")


def test_fallback_skips_image_and_badge_paragraphs(monkeypatch):
    """Lines starting with `!` (image/badge syntax) must still be skipped."""
    monkeypatch.delenv("PROBE_SUMMARY_MIN_CHARS", raising=False)
    long_para = (
        "Production-grade vector store optimized for retrieval-augmented "
        "generation workloads with hybrid sparse-dense indexing."
    )
    readme = (
        f"![banner](https://example.com/banner.png){' x' * 30}\n\n"
        f"{long_para}\n"
    )
    out = _summarizer()._fallback_summary(readme)
    assert out is not None
    assert out.startswith("Production-grade vector store")


def test_fallback_caps_output_at_500_chars(monkeypatch):
    monkeypatch.delenv("PROBE_SUMMARY_MIN_CHARS", raising=False)
    long_para = "x" * 1200
    out = _summarizer()._fallback_summary(long_para)
    assert out is not None
    assert len(out) == 500


def test_fallback_30_to_49_chars_now_rejected_post_kan200(monkeypatch):
    """Regression pin for the exact bug KAN-200 closes.

    A 35-char paragraph used to pass the old `> 30` check and produce a
    summary the probe (>= 50) rejected. Now it must yield `None`.
    """
    monkeypatch.delenv("PROBE_SUMMARY_MIN_CHARS", raising=False)
    paragraph = "Tool to do thing for someone today"  # 34 chars
    assert 30 < len(paragraph) < 50
    readme = f"# Heading\n\n{paragraph}\n"
    assert _summarizer()._fallback_summary(readme) is None


def test_fallback_respects_env_override(monkeypatch):
    """When the probe floor is bumped, the fallback floor follows."""
    monkeypatch.setenv("PROBE_SUMMARY_MIN_CHARS", "200")
    paragraph = "x" * 100  # passes default 50, fails 200
    readme = f"# Heading\n\n{paragraph}\n"
    assert _summarizer()._fallback_summary(readme) is None
