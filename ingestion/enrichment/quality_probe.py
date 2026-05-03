"""
Enrichment Quality Probe (KAN-191)

Closes the "deepest unknown" surfaced by the 2026-04 audit: there was zero
quality gate on what nightly Anthropic enrichment writes to the repos table.
The same silent-failure pattern that broke the Ask Quality Gate for 27 days
could be silently degrading category / tag / summary outputs without anyone
noticing.

The probe runs AFTER nightly enrichment (Cloud Run Job
`reporium-ingestion-nightly`, 03:00 UTC) and samples N repos to verify:

    1. primary_category is in the canonical vocabulary (taxonomy.py CATEGORIES).
    2. integration_tags is non-empty + each tag length-sane (<50 chars).
    3. readme_summary length is in [50, 2000].
    4. No LLM-failure markers ("I cannot", "As an AI", ...) appear in summaries.
    5. Total enriched corpus is >= configured floor (sudden drop = pipeline broken).

Any FLOOR breach exits non-zero. Wired into nightly_graph_build.yml the
non-zero exit fails the workflow, which fires the existing Workato → JIRA
notify-on-failure pipeline (mirroring KAN-147's graph-quality pattern).

Mirrors the design choices established in:
- KAN-147 graph-quality probe (Workato → JIRA on failure)
- ADR series on additive enrichment + post-write verification

Configuration (env vars, all optional):
    DATABASE_URL                         (required)
    PROBE_SAMPLE_SIZE                    default 20
    PROBE_TAGS_TOTAL_FLOOR               default 100
    PROBE_SUMMARY_MIN_CHARS              default 50
    PROBE_SUMMARY_MAX_CHARS              default 2000
    PROBE_TAG_MAX_CHARS                  default 50
    PROBE_TOTAL_ENRICHED_FLOOR           default 1500
    PROBE_FRESHNESS_HOURS                default 36   (window for "recent")
    PROBE_REPORT_DIR                     default ./quality_probe_reports
"""

from __future__ import annotations

import json
import logging
import os
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor

from ingestion.enrichment.taxonomy import CATEGORIES

logger = logging.getLogger(__name__)


# ── Canonical vocabulary ──────────────────────────────────────────────────────

# `primary_category` in the DB stores the human-readable category NAME
# (see ingestion/main.py:186 — written as `category_name`). This must match
# what taxonomy.assign_primary_category() returns.
CANONICAL_CATEGORY_NAMES: frozenset[str] = frozenset(c["name"] for c in CATEGORIES)


# Telltale strings that indicate the LLM refused / errored / hallucinated a
# meta-comment instead of producing a clean summary. Match anywhere in the
# summary, case-insensitive.
LLM_FAILURE_MARKERS: tuple[str, ...] = (
    "I cannot",
    "As an AI",
    "I don't have",
    "I do not have",
    "Sorry, I",
    "I'm unable",
    "I am unable",
    "I'm sorry",
    "I am sorry",
)


# ── Result types ──────────────────────────────────────────────────────────────


@dataclass
class CheckResult:
    name: str
    passed: bool
    floor: str
    observed: str
    failures: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class ProbeReport:
    run_at: str
    sample_size_target: int
    sample_size_actual: int
    total_enriched_in_corpus: int
    overall_passed: bool
    checks: list[CheckResult]


# ── Config ────────────────────────────────────────────────────────────────────


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid %s=%r — using default %d", name, raw, default)
        return default


@dataclass
class ProbeConfig:
    sample_size: int = 20
    tags_total_floor: int = 100
    summary_min_chars: int = 50
    summary_max_chars: int = 2000
    tag_max_chars: int = 50
    total_enriched_floor: int = 1500
    freshness_hours: int = 36
    report_dir: Path = Path("./quality_probe_reports")

    @classmethod
    def from_env(cls) -> "ProbeConfig":
        return cls(
            sample_size=_env_int("PROBE_SAMPLE_SIZE", 20),
            tags_total_floor=_env_int("PROBE_TAGS_TOTAL_FLOOR", 100),
            summary_min_chars=_env_int("PROBE_SUMMARY_MIN_CHARS", 50),
            summary_max_chars=_env_int("PROBE_SUMMARY_MAX_CHARS", 2000),
            tag_max_chars=_env_int("PROBE_TAG_MAX_CHARS", 50),
            total_enriched_floor=_env_int("PROBE_TOTAL_ENRICHED_FLOOR", 1500),
            freshness_hours=_env_int("PROBE_FRESHNESS_HOURS", 36),
            report_dir=Path(os.environ.get("PROBE_REPORT_DIR", "./quality_probe_reports")),
        )


# ── DB helpers ────────────────────────────────────────────────────────────────


def _normalize_db_url(url: str) -> str:
    """Strip asyncpg-only driver suffix so psycopg2 accepts the URL.

    Mirrors scripts/enrich_new_repos.normalize_db_url() — the same DATABASE_URL
    secret is used by both async API code and sync ingestion scripts.
    """
    import urllib.parse

    url = url.replace("+asyncpg", "")
    if url.startswith("postgresql+"):
        url = "postgresql" + url[url.index("://"):]
    parsed = urllib.parse.urlsplit(url)
    params = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    ssl_val = params.pop("ssl", [None])[0]
    if ssl_val and "sslmode" not in params:
        v = ssl_val.lower()
        if v in ("true", "1", "require"):
            params["sslmode"] = ["require"]
        elif v in ("false", "0", "disable"):
            params["sslmode"] = ["disable"]
    new_query = urllib.parse.urlencode({k: v[0] for k, v in params.items()}, doseq=False)
    return urllib.parse.urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, new_query, parsed.fragment)
    )


def _fetch_sample(conn, sample_size: int, freshness_hours: int) -> list[dict[str, Any]]:
    """Pull a sample of recently-enriched repos for inspection.

    "Recently enriched" = updated_at within freshness_hours AND has a non-null
    enrichment field. There's no `last_enriched_at` column today (KAN-191
    follow-up could add one); `updated_at` is the closest proxy because the
    enrichment UPDATE bumps it.

    If fewer than sample_size fresh rows exist, falls back to the most-recently
    updated enriched rows so the probe can still run on demand.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # `make_interval` lets us bind hours as a parameter rather than
        # interpolating into the SQL string (avoids SQL injection / quoting
        # bugs when freshness_hours comes from env).
        cur.execute(
            """
            SELECT
                id::text          AS id,
                owner,
                name,
                primary_category,
                integration_tags,
                readme_summary,
                updated_at
            FROM repos
            WHERE is_private = false
              AND (
                  primary_category IS NOT NULL
                  OR integration_tags IS NOT NULL
                  OR readme_summary IS NOT NULL
              )
              AND updated_at >= NOW() - make_interval(hours => %s)
            ORDER BY updated_at DESC
            LIMIT %s
            """,
            (freshness_hours, sample_size),
        )
        rows = cur.fetchall()

    if len(rows) < sample_size:
        # Fall back to most-recently-updated enriched rows regardless of window.
        # This stops the probe from being totally silent on days where
        # enrichment hasn't yet run, while still surfacing fresh-row scarcity.
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    id::text          AS id,
                    owner,
                    name,
                    primary_category,
                    integration_tags,
                    readme_summary,
                    updated_at
                FROM repos
                WHERE is_private = false
                  AND (
                      primary_category IS NOT NULL
                      OR integration_tags IS NOT NULL
                      OR readme_summary IS NOT NULL
                  )
                ORDER BY updated_at DESC
                LIMIT %s
                """,
                (sample_size,),
            )
            rows = cur.fetchall()
    return list(rows)


def _fetch_total_enriched(conn) -> int:
    """Corpus-shape check: how many public repos have any enrichment at all."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM repos
            WHERE is_private = false
              AND (
                  readme_summary IS NOT NULL
                  OR primary_category IS NOT NULL
                  OR integration_tags IS NOT NULL
              )
            """
        )
        return int(cur.fetchone()[0])


def _coerce_tags(raw: Any) -> list[str]:
    """integration_tags is JSONB. psycopg2 typically auto-decodes to list/dict
    but tolerate strings (raw JSON) and None.
    """
    if raw is None:
        return []
    if isinstance(raw, list):
        return [t for t in raw if isinstance(t, str)]
    if isinstance(raw, str):
        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError:
            return []
        return [t for t in decoded if isinstance(t, str)] if isinstance(decoded, list) else []
    return []


# ── Checks ────────────────────────────────────────────────────────────────────


def _short(repo: dict[str, Any]) -> str:
    return f"{repo.get('owner') or '?'}/{repo.get('name') or '?'}"


def check_category_in_vocabulary(sample: list[dict[str, Any]]) -> CheckResult:
    failures: list[dict[str, Any]] = []
    for repo in sample:
        cat = repo.get("primary_category")
        if cat is None:
            # Allow null — sample includes any-enrichment rows; null
            # primary_category is a separate corpus-shape concern (DQ gate).
            continue
        if cat not in CANONICAL_CATEGORY_NAMES:
            failures.append({"repo": _short(repo), "primary_category": cat})
    passed = len(failures) == 0
    return CheckResult(
        name="primary_category_in_vocabulary",
        passed=passed,
        floor="100% of non-null primary_category values must match taxonomy.CATEGORIES names",
        observed=f"{len(sample) - len(failures)}/{len(sample)} valid (or null), {len(failures)} out-of-vocab",
        failures=failures,
    )


def check_tags_present_and_length(
    sample: list[dict[str, Any]],
    *,
    tag_max_chars: int,
    tags_total_floor: int,
) -> CheckResult:
    failures: list[dict[str, Any]] = []
    total_tags = 0
    empty_count = 0
    for repo in sample:
        tags = _coerce_tags(repo.get("integration_tags"))
        if not tags:
            empty_count += 1
            failures.append({"repo": _short(repo), "issue": "empty_tags"})
            continue
        total_tags += len(tags)
        for t in tags:
            if len(t) > tag_max_chars:
                failures.append(
                    {
                        "repo": _short(repo),
                        "issue": "tag_too_long",
                        "tag": t[:80],
                        "len": len(t),
                    }
                )
    if total_tags < tags_total_floor:
        failures.append(
            {
                "issue": "total_tags_below_floor",
                "total_tags": total_tags,
                "floor": tags_total_floor,
            }
        )
    passed = len(failures) == 0
    return CheckResult(
        name="integration_tags_present_and_sane",
        passed=passed,
        floor=(
            f">=1 tag per repo, each tag <= {tag_max_chars} chars, "
            f"total >= {tags_total_floor}"
        ),
        observed=(
            f"{len(sample) - empty_count}/{len(sample)} repos have tags, "
            f"total tags = {total_tags}"
        ),
        failures=failures,
    )


def check_summary_length(
    sample: list[dict[str, Any]],
    *,
    min_chars: int,
    max_chars: int,
) -> CheckResult:
    failures: list[dict[str, Any]] = []
    in_range = 0
    for repo in sample:
        s = repo.get("readme_summary")
        if s is None:
            failures.append({"repo": _short(repo), "issue": "null_summary"})
            continue
        n = len(s)
        if n < min_chars:
            failures.append({"repo": _short(repo), "issue": "too_short", "len": n})
        elif n > max_chars:
            failures.append({"repo": _short(repo), "issue": "too_long", "len": n})
        else:
            in_range += 1
    passed = len(failures) == 0
    return CheckResult(
        name="readme_summary_length_in_range",
        passed=passed,
        floor=f"100% of summaries in [{min_chars}, {max_chars}] chars",
        observed=f"{in_range}/{len(sample)} in range, {len(failures)} out of range or null",
        failures=failures,
    )


def check_no_llm_failure_markers(sample: list[dict[str, Any]]) -> CheckResult:
    failures: list[dict[str, Any]] = []
    for repo in sample:
        s = repo.get("readme_summary")
        if not s:
            continue
        s_lower = s.lower()
        for marker in LLM_FAILURE_MARKERS:
            if marker.lower() in s_lower:
                failures.append(
                    {"repo": _short(repo), "marker": marker, "snippet": s[:160]}
                )
                break  # one hit per repo is enough
    passed = len(failures) == 0
    return CheckResult(
        name="no_llm_failure_markers_in_summary",
        passed=passed,
        floor="0 occurrences across sample",
        observed=f"{len(failures)} repos contain a failure marker",
        failures=failures,
    )


def check_total_enriched_floor(total_enriched: int, floor: int) -> CheckResult:
    passed = total_enriched >= floor
    return CheckResult(
        name="total_enriched_corpus_floor",
        passed=passed,
        floor=f">= {floor} enriched public repos",
        observed=f"{total_enriched} enriched public repos",
        failures=(
            []
            if passed
            else [{"issue": "below_floor", "observed": total_enriched, "floor": floor}]
        ),
    )


# ── Orchestration ─────────────────────────────────────────────────────────────


def run_probe(conn, config: ProbeConfig) -> ProbeReport:
    sample = _fetch_sample(conn, config.sample_size, config.freshness_hours)
    total_enriched = _fetch_total_enriched(conn)

    checks = [
        check_category_in_vocabulary(sample),
        check_tags_present_and_length(
            sample,
            tag_max_chars=config.tag_max_chars,
            tags_total_floor=config.tags_total_floor,
        ),
        check_summary_length(
            sample,
            min_chars=config.summary_min_chars,
            max_chars=config.summary_max_chars,
        ),
        check_no_llm_failure_markers(sample),
        check_total_enriched_floor(total_enriched, config.total_enriched_floor),
    ]

    return ProbeReport(
        run_at=datetime.now(timezone.utc).isoformat(),
        sample_size_target=config.sample_size,
        sample_size_actual=len(sample),
        total_enriched_in_corpus=total_enriched,
        overall_passed=all(c.passed for c in checks),
        checks=checks,
    )


def report_to_dict(report: ProbeReport) -> dict[str, Any]:
    d = asdict(report)
    return d


def report_to_markdown(report: ProbeReport) -> str:
    status_line = "PASS" if report.overall_passed else "FAIL"
    lines = [
        f"# Enrichment Quality Probe — {status_line}",
        "",
        f"- Run at: `{report.run_at}`",
        f"- Sample: {report.sample_size_actual}/{report.sample_size_target}",
        f"- Total enriched in corpus: {report.total_enriched_in_corpus}",
        "",
        "## Checks",
        "",
    ]
    for c in report.checks:
        emoji = "PASS" if c.passed else "FAIL"
        lines.append(f"### {c.name} — {emoji}")
        lines.append("")
        lines.append(f"- Floor: {c.floor}")
        lines.append(f"- Observed: {c.observed}")
        if c.failures:
            lines.append(f"- Failures ({len(c.failures)}):")
            for f in c.failures[:10]:
                lines.append(f"  - {json.dumps(f, default=str)}")
            if len(c.failures) > 10:
                lines.append(f"  - ... and {len(c.failures) - 10} more")
        lines.append("")
    return "\n".join(lines)


def write_reports(report: ProbeReport, report_dir: Path) -> tuple[Path, Path]:
    report_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    json_path = report_dir / f"probe_{stamp}.json"
    md_path = report_dir / f"probe_{stamp}.md"
    json_path.write_text(json.dumps(report_to_dict(report), indent=2, default=str))
    md_path.write_text(report_to_markdown(report))
    return json_path, md_path


# ── Entry point ───────────────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    raw_url = os.environ.get("DATABASE_URL", "").strip()
    if not raw_url:
        logger.error("DATABASE_URL not set")
        return 2

    config = ProbeConfig.from_env()
    logger.info(
        "Probe config: sample=%d freshness_hours=%d total_floor=%d",
        config.sample_size,
        config.freshness_hours,
        config.total_enriched_floor,
    )

    db_url = _normalize_db_url(raw_url)
    conn = psycopg2.connect(db_url)
    try:
        report = run_probe(conn, config)
    finally:
        conn.close()

    json_path, md_path = write_reports(report, config.report_dir)
    logger.info("Wrote reports: %s, %s", json_path, md_path)

    # Always print markdown so workflow logs surface the reason for any FAIL.
    print(report_to_markdown(report))

    if not report.overall_passed:
        # Workflow non-zero exit triggers existing notify-on-failure → Workato → JIRA
        # (KAN-147 pattern).
        logger.error("Quality probe FAILED — see report above")
        return 1
    logger.info("Quality probe PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
