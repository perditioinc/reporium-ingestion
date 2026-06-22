"""#148: the no-regression GATE for the enrichment frontier->local swap.

This is the proof that swapping the paid Claude enrichment call for the local
qwen2.5:7b path does NOT regress. It reuses the estate's $0 eval harness
(``local_inference.eval``) and runs two systems over the enrichment golden set:

  * candidate = LOCAL qwen2.5:7b via Ollama, constrained-decoding the same
    enrichment prompt into taxonomy JSON (the system under test);
  * baseline  = the curated REFERENCE taxonomy for each repo (the frontier-quality
    expected answer the swap must not fall below).

Both flow through the tiered gate:
  Tier 1 deterministic - taxonomy SHAPE contract (valid JSON, all 11 fields,
         in-vocab enums, expected integration tags) - the model-free ground truth;
  Tier 2 NLI           - the candidate's taxonomy entails the reference;
  Tier 3 local judge   - nuanced quality, calibrated against Tier 1.

The gate FAILS (exit non-zero) if the local candidate's composite drops more than
``--threshold`` below the reference. It is $0: candidate + judge + NLI all run on
the local Ollama; nothing hits a paid API.

Run:
    python -m eval.run_enrichment_gate            # default threshold 0.05
    python -m eval.run_enrichment_gate --json out.json
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from local_inference.eval.goldenset import GoldenRecord, load_goldenset, run_gate

from ingestion.enrichers.local_enricher import ENRICHMENT_SCHEMA

HERE = Path(__file__).parent
GOLDEN = HERE / "enrichment_goldenset.jsonl"


def _build_reference_map(records: list[GoldenRecord]) -> dict[str, str]:
    """input-prompt -> reference taxonomy, so the baseline system can answer."""
    return {r.input: r.reference for r in records}


def make_local_system(num_predict: int = 768, timeout: float = 240.0):
    """Candidate system: local qwen produces taxonomy JSON for the prompt.

    The gate's ``System`` contract is ``Callable[[str], str]``: it gets the
    record's input (the rendered enrichment prompt) and returns an answer string.
    Here the answer is the raw JSON the local model decodes, so the deterministic
    SHAPE checks (json_valid / json_key / enum regex / tag contains) run directly
    against the real model output."""
    from local_inference import LocalClient

    client = LocalClient()

    def system(prompt: str) -> str:
        res = client.classify_json(
            prompt, ENRICHMENT_SCHEMA, num_predict=num_predict, temperature=0.0, timeout=timeout
        )
        return json.dumps(res.data)

    return system


def make_reference_system(records: list[GoldenRecord]):
    """Baseline system: return the curated reference taxonomy for each prompt.

    This stands in for the frontier (Claude) behavior the swap must not regress
    below. The reference is prose (NLI/judge friendly); it is intentionally NOT
    JSON, so the deterministic SHAPE tier abstains for the baseline and the
    baseline's composite reflects NLI+judge only. The candidate is held to the
    stricter bar (it must ALSO pass the JSON shape contract)."""
    ref = _build_reference_map(records)

    def system(prompt: str) -> str:
        return ref.get(prompt, "")

    return system


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="#148 enrichment frontier->local no-regression gate")
    p.add_argument("--goldenset", default=str(GOLDEN))
    p.add_argument("--threshold", type=float, default=0.05,
                   help="max tolerated composite drop vs the reference baseline")
    p.add_argument("--floor", type=float, default=0.6,
                   help="absolute composite floor if run without a baseline")
    p.add_argument("--quality-floor", type=float, default=0.65,
                   help="absolute floor on the candidate's mean(NLI, judge) quality")
    p.add_argument("--no-judge", action="store_true")
    p.add_argument("--no-nli", action="store_true")
    p.add_argument("--json", dest="json_out", default=None, help="write the full result JSON here")
    args = p.parse_args(argv)

    records = load_goldenset(args.goldenset)
    print(f"loaded {len(records)} enrichment golden records from {args.goldenset}", file=sys.stderr)

    candidate = make_local_system()
    baseline = make_reference_system(records)

    t0 = time.time()
    result = run_gate(
        records,
        candidate,
        candidate_label="local_qwen2.5_7b",
        baseline=baseline,
        baseline_label="frontier_reference",
        threshold=args.threshold,
        floor=args.floor,
        use_judge=not args.no_judge,
        use_nli=not args.no_nli,
    )
    elapsed = time.time() - t0

    cand = result.candidate
    base = result.baseline
    summary = {
        "passed": result.passed,
        "reason": result.reason,
        "threshold": result.threshold,
        "elapsed_s": round(elapsed, 1),
        "n_records": len(records),
        "candidate": {
            "label": cand.label,
            "composite": round(cand.composite, 4),
            "tiers": {k: round(v, 4) for k, v in cand.tier_means.items()},
            "skipped_tiers": list(cand.skipped_tiers),
        },
    }
    if base is not None:
        summary["baseline"] = {
            "label": base.label,
            "composite": round(base.composite, 4),
            "tiers": {k: round(v, 4) for k, v in base.tier_means.items()},
            "skipped_tiers": list(base.skipped_tiers),
        }
        summary["delta"] = round(result.delta, 4) if result.delta is not None else None

    # Per-record deterministic shape pass-rate (the json-valid + field contract).
    det_scores = [rs.deterministic for rs in cand.records if rs.deterministic is not None]
    if det_scores:
        summary["candidate"]["deterministic_per_record"] = [round(s, 3) for s in det_scores]
        summary["candidate"]["json_shape_pass_rate"] = round(
            sum(1 for s in det_scores if s >= 0.999) / len(det_scores), 3
        )
        summary["candidate"]["mean_deterministic"] = round(
            sum(det_scores) / len(det_scores), 4
        )

    # Apples-to-apples QUALITY delta. The prose reference is not JSON, so its
    # deterministic tier abstains; comparing raw composites would unfairly credit
    # the candidate for ALSO passing the shape contract. We therefore also report
    # the quality-only delta over the tiers BOTH systems produced (NLI + judge),
    # which is the honest "is the local taxonomy as semantically good as the
    # frontier reference?" measure.
    if base is not None:
        shared = [t for t in ("nli", "judge") if t in cand.tier_means and t in base.tier_means]
        if shared:
            cq = sum(cand.tier_means[t] for t in shared) / len(shared)
            bq = sum(base.tier_means[t] for t in shared) / len(shared)
            summary["quality_only"] = {
                "tiers": shared,
                "candidate": round(cq, 4),
                "reference": round(bq, 4),
                "delta": round(cq - bq, 4),
                "caveat": (
                    "The baseline is the curated reference itself, so its NLI/judge "
                    "tiers are a near-perfect self-comparison (an upper bound, not a "
                    "fair frontier bar at $0). Read the candidate's quality as an "
                    "absolute floor (see candidate_quality_floor), and the "
                    "deterministic shape pass-rate as the real regression contract."
                ),
            }

    # Absolute quality floor on the CANDIDATE alone (independent of the
    # self-perfect baseline). This is the honest 'is the local taxonomy good
    # enough?' bar: mean(NLI, judge) over the candidate must clear --quality-floor.
    cand_shared = [t for t in ("nli", "judge") if t in cand.tier_means]
    if cand_shared:
        cand_quality = sum(cand.tier_means[t] for t in cand_shared) / len(cand_shared)
        floor_ok = cand_quality >= args.quality_floor
        summary["candidate_quality_floor"] = {
            "tiers": cand_shared,
            "candidate_quality": round(cand_quality, 4),
            "floor": args.quality_floor,
            "passed": floor_ok,
        }
        if not floor_ok:
            summary["passed"] = False
            summary["reason"] = (
                f"{summary['reason']} | BUT candidate quality {cand_quality:.4f} "
                f"< floor {args.quality_floor}"
            )

    print(json.dumps(summary, indent=2))
    if args.json_out:
        Path(args.json_out).write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(f"wrote full result -> {args.json_out}", file=sys.stderr)

    # The gate passes only if BOTH the no-regression delta AND the candidate
    # quality floor hold (summary['passed'] is updated above if the floor fails).
    return 0 if summary["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
