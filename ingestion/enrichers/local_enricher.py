"""Local-first AI enrichment using qwen2.5:7b via Ollama (issue #148 / R0).

This is the $0 local replacement for the paid Claude enrichment call. It runs the
SAME ``ENRICHMENT_PROMPT`` and the SAME ``_parse_enrichment_response`` parser as
``ingestion.enrichers.ai_enricher`` so the output dict shape is byte-for-byte
compatible with both enrichment paths (the per-payload nightly pass in
``ingestion.main`` and the corpus-wide ``run_ai_enrichment`` catch-up).

The difference is the transport: instead of ``anthropic.AsyncAnthropic`` it calls
the estate's importable local-first client (``local_inference.LocalClient``),
which talks to the local Ollama (OpenAI-compatible, 127.0.0.1:11434) and decodes
into JSON under a schema constraint. Enrichment is OFF the hot path (nightly
taxonomy refresh), so the latency tradeoff vs Claude is irrelevant; the win is
that every call is owned-hardware and therefore $0.

Design:
  - ``ENRICHMENT_SCHEMA`` is the JSON Schema the local model decodes into. It
    mirrors the eleven taxonomy fields the Claude prompt asks for. Constrained
    decoding (``LocalClient.classify_json``) guarantees the result parses as JSON
    with the required keys, so the downstream ``_parse_enrichment_response``
    (which only normalizes/validates values) always has a well-formed object.
  - ``LocalEnricher.enrich`` returns the SAME ``(data, input_tokens, output_tokens,
    confidence)`` tuple shape the provider seam expects, so the per-payload merge
    in ``ingestion.main`` is provider-agnostic.
  - ``local_inference`` is an OPTIONAL import. If it is not installed the enricher
    raises ``LocalEnricherUnavailable`` and the provider falls back to the
    configured escalation path (frontier) rather than crashing the run.

No secrets, no cloud, no production side effects: this module only talks to the
loopback Ollama. It never touches GCP, Postgres, or the API.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

from .ai_enricher import (
    ENRICHMENT_PROMPT,
    _build_repo_context,
    _parse_enrichment_response,
)

logger = logging.getLogger(__name__)

#: The local default. qwen2.5:7b-instruct-q4_K_M is the keep-warm 7B on the dev
#: box; override via ``ENRICHMENT_LOCAL_MODEL`` / the ``model`` arg if a different
#: local tag is pulled.
DEFAULT_LOCAL_MODEL = "qwen2.5:7b-instruct-q4_K_M"
DEFAULT_LOCAL_HOST = "http://127.0.0.1:11434"

#: JSON Schema for constrained decoding. Mirrors the eleven taxonomy fields the
#: Claude ENRICHMENT_PROMPT asks for. All eleven are required so the local model
#: cannot silently drop a dimension; ``_parse_enrichment_response`` then coerces
#: out-of-vocab enum values to the safe defaults (medium / None) exactly as it
#: does for the Claude path, so a hallucinated enum never breaks the row.
ENRICHMENT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "readme_summary": {"type": "string"},
        "problem_solved": {"type": "string"},
        "quality_assessment": {"type": "string", "enum": ["high", "medium", "low"]},
        "maturity_level": {
            "type": "string",
            "enum": ["research", "prototype", "beta", "production"],
        },
        "skill_areas": {"type": "array", "items": {"type": "string"}},
        "industries": {"type": "array", "items": {"type": "string"}},
        "use_cases": {"type": "array", "items": {"type": "string"}},
        "modalities": {"type": "array", "items": {"type": "string"}},
        "ai_trends": {"type": "array", "items": {"type": "string"}},
        "deployment_context": {"type": "array", "items": {"type": "string"}},
        "integration_tags": {"type": "array", "items": {"type": "string"}},
    },
    "required": [
        "readme_summary",
        "problem_solved",
        "quality_assessment",
        "maturity_level",
        "skill_areas",
        "industries",
        "use_cases",
        "modalities",
        "ai_trends",
        "deployment_context",
        "integration_tags",
    ],
}


class LocalEnricherUnavailable(Exception):
    """Raised when the local-inference client cannot be imported/constructed, so
    the provider seam can fall back to the frontier escalation path instead of
    crashing the run."""


@dataclass(frozen=True)
class LocalEnrichmentResult:
    """Output of one local enrichment call.

    ``data`` is the parsed/normalized taxonomy dict (same shape as
    ``ai_enricher._parse_enrichment_response``). ``input_tokens`` /
    ``output_tokens`` come from the local model's own counters (Ollama reports
    them) and are $0 - they are kept only for parity with the Anthropic usage
    shape so call-site stats code is provider-agnostic. ``confidence`` is the
    local client's confidence signal, used by the provider seam to decide
    whether to escalate to a frontier model."""

    data: dict[str, Any]
    input_tokens: int
    output_tokens: int
    confidence: float
    model: str
    repaired: bool


class LocalEnricher:
    """Enrich one repo's taxonomy with the local 7B via Ollama. Construct once
    and reuse; the underlying ``LocalClient`` is cheap and keeps the model warm
    via ``keep_alive``."""

    def __init__(
        self,
        *,
        host: str = DEFAULT_LOCAL_HOST,
        model: str = DEFAULT_LOCAL_MODEL,
        keep_alive: str = "10m",
        num_predict: int = 768,
        timeout: float = 240.0,
    ) -> None:
        try:
            from local_inference import LocalClient  # noqa: PLC0415
        except ImportError as exc:  # pragma: no cover - exercised via provider test
            raise LocalEnricherUnavailable(
                "local_inference is not installed; install it (pip install -e "
                "<path-to>/local-inference) to enable $0 local enrichment, or set "
                "ENRICHMENT_PROVIDER=frontier."
            ) from exc

        self._client = LocalClient(host=host, chat_model=model, keep_alive=keep_alive)
        self.model = model
        self.num_predict = num_predict
        self.timeout = timeout

    def enrich(self, context_row: dict) -> LocalEnrichmentResult:
        """Run local constrained-decoding enrichment for one repo context row.

        ``context_row`` is the same dict shape ``_build_repo_context`` consumes
        (owner/name/description/primary_language/forked_from/dependencies). The
        return value's ``data`` is post-processed through the shared
        ``_parse_enrichment_response`` so it is identical in shape to the Claude
        path (lists cleaned/deduped, enums coerced, tags lowercased)."""
        from local_inference import LocalInferenceError, StructuredError  # noqa: PLC0415

        prompt = ENRICHMENT_PROMPT.format(repo_context=_build_repo_context(context_row))
        try:
            result = self._client.classify_json(
                prompt,
                ENRICHMENT_SCHEMA,
                num_predict=self.num_predict,
                temperature=0.0,
                timeout=self.timeout,
            )
        except (LocalInferenceError, StructuredError) as exc:
            # Transport down or the model could not produce schema-valid JSON
            # even after repair. Surface as unavailable so the provider can
            # escalate; do NOT swallow into an empty enrichment (that would
            # regress integration_tags exactly like the KAN-191 incident).
            raise LocalEnricherUnavailable(
                f"local enrichment failed for "
                f"{context_row.get('owner')}/{context_row.get('name')}: {exc}"
            ) from exc

        # Re-use the shared normalizer so value coercion (enum safety, list
        # cleaning, tag lowercasing) is identical to the Claude path. We dump the
        # already-valid dict back to JSON text because the normalizer's signature
        # is text-in (it also strips code fences, which constrained output won't
        # have, so this is a no-op cleanup that keeps a single source of truth).
        import json  # noqa: PLC0415

        data = _parse_enrichment_response(json.dumps(result.data))

        # Ollama reports prompt/eval token counts on the structured result's
        # underlying call; LocalClient surfaces them on GenerateResult but the
        # constrained path returns StructuredResult, which does not carry token
        # counts. Token counts are $0 and informational only, so report 0/0 here
        # rather than guessing.
        confidence = self._client.confidence_of(result.text)
        return LocalEnrichmentResult(
            data=data,
            input_tokens=0,
            output_tokens=0,
            confidence=confidence,
            model=self.model,
            repaired=bool(getattr(result, "repaired", False)),
        )


def build_enrichment_prompt(context_row: dict) -> str:
    """Render the shared enrichment prompt for a repo context row. Exposed so the
    eval harness can build identical inputs for the local + reference systems."""
    return ENRICHMENT_PROMPT.format(repo_context=_build_repo_context(context_row))


__all__ = [
    "LocalEnricher",
    "LocalEnricherUnavailable",
    "LocalEnrichmentResult",
    "ENRICHMENT_SCHEMA",
    "DEFAULT_LOCAL_MODEL",
    "DEFAULT_LOCAL_HOST",
    "build_enrichment_prompt",
]
