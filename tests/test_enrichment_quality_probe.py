"""
Unit tests for the KAN-191 enrichment quality probe.

These tests exercise the pure check functions against synthetic samples — no
database needed. The `no_db` marker skips the DB-setup conftest fixtures so
the suite runs without a Postgres dependency.
"""

import pytest

from ingestion.enrichment.quality_probe import (
    CANONICAL_CATEGORY_NAMES,
    LLM_FAILURE_MARKERS,
    ProbeConfig,
    ProbeReport,
    check_category_in_vocabulary,
    check_no_llm_failure_markers,
    check_summary_length,
    check_tags_present_and_length,
    check_total_enriched_floor,
    report_to_dict,
    report_to_markdown,
)

pytestmark = pytest.mark.no_db


# Pull a real category name straight from the canonical set so we don't drift.
A_VALID_CATEGORY = next(iter(CANONICAL_CATEGORY_NAMES))


def _repo(**overrides):
    base = {
        "id": "deadbeef-dead-beef-dead-beefdeadbeef",
        "owner": "perditioinc",
        "name": "example",
        "primary_category": A_VALID_CATEGORY,
        "integration_tags": ["tag-a", "tag-b"],
        "readme_summary": "x" * 300,
        "updated_at": None,
    }
    base.update(overrides)
    return base


# ── 1. primary_category in vocabulary ────────────────────────────────────────


def test_category_check_passes_with_valid_categories():
    sample = [_repo(), _repo(name="b")]
    result = check_category_in_vocabulary(sample)
    assert result.passed
    assert result.failures == []


def test_category_check_passes_with_null_categories():
    """Null primary_category is allowed (DQ gate's concern, not the probe's)."""
    sample = [_repo(primary_category=None)]
    result = check_category_in_vocabulary(sample)
    assert result.passed


def test_category_check_fails_on_out_of_vocab():
    sample = [_repo(primary_category="Made Up Category")]
    result = check_category_in_vocabulary(sample)
    assert not result.passed
    assert len(result.failures) == 1
    assert result.failures[0]["primary_category"] == "Made Up Category"


def test_category_check_drift_canary_uses_real_taxonomy():
    """If taxonomy.py renames a category, this assertion gets us a clear fail."""
    assert A_VALID_CATEGORY in CANONICAL_CATEGORY_NAMES
    assert "AI Agents" in CANONICAL_CATEGORY_NAMES  # taxonomy.py:18


# ── 2. integration_tags presence + length ─────────────────────────────────────


def test_tags_check_passes_with_healthy_tags():
    sample = [_repo(integration_tags=["a", "b", "c"]) for _ in range(20)]
    result = check_tags_present_and_length(
        sample, tag_max_chars=50, tags_total_floor=30
    )
    assert result.passed


def test_tags_check_fails_on_empty_tags():
    sample = [_repo(integration_tags=[])]
    result = check_tags_present_and_length(
        sample, tag_max_chars=50, tags_total_floor=0
    )
    assert not result.passed
    assert any(f["issue"] == "empty_tags" for f in result.failures)


def test_tags_check_fails_on_overlong_tag():
    sample = [_repo(integration_tags=["x" * 80])]
    result = check_tags_present_and_length(
        sample, tag_max_chars=50, tags_total_floor=0
    )
    assert not result.passed
    assert any(f["issue"] == "tag_too_long" for f in result.failures)


def test_tags_check_fails_when_total_below_floor():
    sample = [_repo(integration_tags=["one"]) for _ in range(2)]
    result = check_tags_present_and_length(
        sample, tag_max_chars=50, tags_total_floor=100
    )
    assert not result.passed
    assert any(f["issue"] == "total_tags_below_floor" for f in result.failures)


def test_tags_check_handles_jsonb_string_form():
    """psycopg2 normally decodes JSONB to list, but be defensive about strings."""
    sample = [_repo(integration_tags='["a", "b"]')]
    result = check_tags_present_and_length(
        sample, tag_max_chars=50, tags_total_floor=0
    )
    assert result.passed


def test_tags_check_handles_none_as_empty():
    sample = [_repo(integration_tags=None)]
    result = check_tags_present_and_length(
        sample, tag_max_chars=50, tags_total_floor=0
    )
    assert not result.passed
    assert any(f["issue"] == "empty_tags" for f in result.failures)


# ── 3. summary length ─────────────────────────────────────────────────────────


def test_summary_check_passes_in_range():
    sample = [_repo(readme_summary="x" * 300)]
    result = check_summary_length(sample, min_chars=50, max_chars=2000)
    assert result.passed


def test_summary_check_fails_too_short():
    sample = [_repo(readme_summary="hi")]
    result = check_summary_length(sample, min_chars=50, max_chars=2000)
    assert not result.passed
    assert any(f["issue"] == "too_short" for f in result.failures)


def test_summary_check_fails_too_long():
    sample = [_repo(readme_summary="x" * 9999)]
    result = check_summary_length(sample, min_chars=50, max_chars=2000)
    assert not result.passed
    assert any(f["issue"] == "too_long" for f in result.failures)


def test_summary_check_fails_on_null():
    sample = [_repo(readme_summary=None)]
    result = check_summary_length(sample, min_chars=50, max_chars=2000)
    assert not result.passed
    assert any(f["issue"] == "null_summary" for f in result.failures)


# ── 4. LLM failure markers ────────────────────────────────────────────────────


@pytest.mark.parametrize("marker", LLM_FAILURE_MARKERS)
def test_llm_marker_check_catches_each_marker(marker):
    sample = [_repo(readme_summary=f"Some preamble. {marker} comply with this request.")]
    result = check_no_llm_failure_markers(sample)
    assert not result.passed
    assert len(result.failures) == 1
    assert result.failures[0]["marker"] == marker


def test_llm_marker_check_is_case_insensitive():
    sample = [_repo(readme_summary="i CANNOT do that, sorry.")]
    result = check_no_llm_failure_markers(sample)
    assert not result.passed


def test_llm_marker_check_passes_clean_summaries():
    sample = [
        _repo(readme_summary="A robust framework for autonomous agent orchestration."),
        _repo(readme_summary="Vector database optimized for RAG applications."),
    ]
    result = check_no_llm_failure_markers(sample)
    assert result.passed


# ── 5. total enriched floor ───────────────────────────────────────────────────


def test_total_enriched_floor_passes_at_or_above():
    assert check_total_enriched_floor(1500, 1500).passed
    assert check_total_enriched_floor(1750, 1500).passed


def test_total_enriched_floor_fails_below():
    result = check_total_enriched_floor(1200, 1500)
    assert not result.passed
    assert result.failures and result.failures[0]["observed"] == 1200


# ── Reporting ─────────────────────────────────────────────────────────────────


def _build_report(overall_passed: bool) -> ProbeReport:
    sample = [_repo()]
    cat = check_category_in_vocabulary(sample)
    if not overall_passed:
        cat.passed = False
        cat.failures = [{"repo": "x/y", "primary_category": "Bogus"}]
    return ProbeReport(
        run_at="2026-05-03T00:00:00+00:00",
        sample_size_target=20,
        sample_size_actual=1,
        total_enriched_in_corpus=1700,
        overall_passed=overall_passed,
        checks=[cat],
    )


def test_report_to_dict_round_trips():
    rep = _build_report(overall_passed=True)
    d = report_to_dict(rep)
    assert d["overall_passed"] is True
    assert isinstance(d["checks"], list)


def test_report_to_markdown_includes_status():
    pass_md = report_to_markdown(_build_report(overall_passed=True))
    fail_md = report_to_markdown(_build_report(overall_passed=False))
    assert "PASS" in pass_md
    assert "FAIL" in fail_md


# ── Config ────────────────────────────────────────────────────────────────────


def test_probe_config_from_env_uses_defaults(monkeypatch):
    for k in [
        "PROBE_SAMPLE_SIZE",
        "PROBE_TAGS_TOTAL_FLOOR",
        "PROBE_SUMMARY_MIN_CHARS",
        "PROBE_SUMMARY_MAX_CHARS",
        "PROBE_TAG_MAX_CHARS",
        "PROBE_TOTAL_ENRICHED_FLOOR",
        "PROBE_FRESHNESS_HOURS",
    ]:
        monkeypatch.delenv(k, raising=False)
    cfg = ProbeConfig.from_env()
    assert cfg.sample_size == 20
    assert cfg.total_enriched_floor == 1500
    assert cfg.summary_min_chars == 50
    assert cfg.summary_max_chars == 2000


def test_probe_config_from_env_reads_overrides(monkeypatch):
    monkeypatch.setenv("PROBE_SAMPLE_SIZE", "50")
    monkeypatch.setenv("PROBE_TOTAL_ENRICHED_FLOOR", "2000")
    cfg = ProbeConfig.from_env()
    assert cfg.sample_size == 50
    assert cfg.total_enriched_floor == 2000


def test_probe_config_ignores_invalid_int(monkeypatch):
    monkeypatch.setenv("PROBE_SAMPLE_SIZE", "not-a-number")
    cfg = ProbeConfig.from_env()
    assert cfg.sample_size == 20  # falls back to default
