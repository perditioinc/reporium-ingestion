"""Enrichment provider seam: local-first, frontier-as-escalation (issue #148).

This is the flag-gate that decides WHICH model enriches a repo's taxonomy. It
keeps the paid Claude path fully intact as an escalation target while making the
local qwen2.5:7b path (``LocalEnricher``) the $0 default. The selection is driven
by ``ENRICHMENT_PROVIDER`` (or the ``provider`` arg):

    local     - always use the local 7B; never call a paid API. ($0, hard.)
    frontier  - always use Claude (the pre-#148 behavior). Requires an API key.
    auto      - local-first: try the local 7B; escalate to Claude ONLY when the
                local path is unavailable (server down / install missing) or the
                local result's confidence is below ``escalation_threshold`` AND a
                frontier api_key is configured. If no api_key, ``auto`` degrades
                to local-only (still $0, never silently empty).

The seam returns a uniform ``ProviderEnrichment`` regardless of which backend
served the call, so ``ingestion.main._enrich_payloads_with_ai`` is
provider-agnostic and the per-payload merge is unchanged.

Doctrine (program-wide): the frontier->local swap defaults to local, escalates
only on low confidence, and stays $0 by default. The escalation path is
preserved, not deleted, so a future eval-gate regression can flip the default
back to ``frontier`` via one env var with zero code change.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Any, Optional

from .ai_enricher import (
    ENRICHMENT_PROMPT,
    _build_repo_context,
    _parse_enrichment_response,
)
from .local_enricher import LocalEnricher, LocalEnricherUnavailable

logger = logging.getLogger(__name__)

#: Provider flag values.
PROVIDER_LOCAL = "local"
PROVIDER_FRONTIER = "frontier"
PROVIDER_AUTO = "auto"
VALID_PROVIDERS = (PROVIDER_LOCAL, PROVIDER_FRONTIER, PROVIDER_AUTO)

#: Default escalation threshold for ``auto``. Local confidence below this (and a
#: frontier key present) triggers a Claude retry. The local client's confidence
#: signal is a heuristic today (see local_inference.client.confidence_of), so the
#: default is conservative; tune via ``ENRICHMENT_ESCALATION_THRESHOLD``.
DEFAULT_ESCALATION_THRESHOLD = float(
    os.environ.get("ENRICHMENT_ESCALATION_THRESHOLD", "0.0") or "0.0"
)


def resolve_provider(configured: Optional[str] = None) -> str:
    """Resolve the active provider. Precedence: explicit arg -> ENRICHMENT_PROVIDER
    env -> 'local' (the $0 default). Unknown values fall back to 'local' with a
    warning rather than failing the run."""
    raw = (configured or os.environ.get("ENRICHMENT_PROVIDER") or PROVIDER_LOCAL).strip().lower()
    if raw not in VALID_PROVIDERS:
        logger.warning(
            "Unknown ENRICHMENT_PROVIDER=%r; falling back to 'local'. "
            "Valid: %s",
            raw,
            ", ".join(VALID_PROVIDERS),
        )
        return PROVIDER_LOCAL
    return raw


@dataclass(frozen=True)
class ProviderEnrichment:
    """Uniform result of enriching one repo, whichever backend served it.

    ``backend`` is 'local:<model>' or 'frontier:<model>' for audit/metrics.
    ``escalated`` is True when ``auto`` fell through from local to frontier."""

    data: dict[str, Any]
    backend: str
    input_tokens: int
    output_tokens: int
    confidence: float
    escalated: bool


class EnrichmentProvider:
    """Provider-agnostic enrichment façade. Construct once per run and reuse.

    The local enricher is built lazily (and only when the provider can use it)
    so ``frontier`` mode never imports/requires local_inference, and a missing
    local install only matters when local is actually selected."""

    def __init__(
        self,
        *,
        provider: Optional[str] = None,
        local_model: Optional[str] = None,
        local_host: Optional[str] = None,
        escalation_threshold: float = DEFAULT_ESCALATION_THRESHOLD,
    ) -> None:
        self.provider = resolve_provider(provider)
        self.escalation_threshold = escalation_threshold
        self._local_model = local_model or os.environ.get(
            "ENRICHMENT_LOCAL_MODEL"
        )
        self._local_host = local_host
        self._local: Optional[LocalEnricher] = None
        self._local_failed = False  # latch: stop retrying a dead local server

    def _get_local(self) -> Optional[LocalEnricher]:
        """Lazily build the local enricher. Returns None (latched) if it cannot
        be constructed, so the provider escalates instead of raising per call."""
        if self._local is not None:
            return self._local
        if self._local_failed:
            return None
        kwargs: dict[str, Any] = {}
        if self._local_model:
            kwargs["model"] = self._local_model
        if self._local_host:
            kwargs["host"] = self._local_host
        try:
            self._local = LocalEnricher(**kwargs)
            return self._local
        except LocalEnricherUnavailable as exc:
            logger.warning("Local enricher unavailable: %s", exc)
            self._local_failed = True
            return None

    async def enrich_one(
        self,
        context_row: dict,
        *,
        api_key: str = "",
        frontier_model: str = "claude-sonnet-4-20250514",
    ) -> ProviderEnrichment:
        """Enrich one repo per the active provider policy.

        Raises only if NO backend can serve the call (e.g. provider=frontier with
        no api_key, or provider=local with the server down and no fallback). The
        caller treats a raised exception as a per-repo failure (logged, counted)
        exactly as the Anthropic path already does."""
        if self.provider == PROVIDER_FRONTIER:
            return await self._enrich_frontier(context_row, api_key, frontier_model)

        # local or auto: try local first.
        local = self._get_local()
        if local is not None:
            try:
                # The local client is sync (stdlib urllib); run it off the event
                # loop so concurrent enrich_one() calls do not serialize on it.
                res = await asyncio.to_thread(local.enrich, context_row)
            except LocalEnricherUnavailable as exc:
                logger.warning("Local enrichment failed: %s", exc)
                self._local_failed = True
                res = None
            else:
                escalate = (
                    self.provider == PROVIDER_AUTO
                    and api_key
                    and res.confidence < self.escalation_threshold
                )
                if not escalate:
                    return ProviderEnrichment(
                        data=res.data,
                        backend=f"local:{res.model}",
                        input_tokens=res.input_tokens,
                        output_tokens=res.output_tokens,
                        confidence=res.confidence,
                        escalated=False,
                    )
                logger.info(
                    "auto: local confidence %.3f < %.3f; escalating to frontier",
                    res.confidence,
                    self.escalation_threshold,
                )

        # Reached here => local unavailable OR auto-escalation. Use frontier if
        # we can; otherwise, in 'local' mode, surface the failure.
        if self.provider == PROVIDER_LOCAL:
            raise LocalEnricherUnavailable(
                "ENRICHMENT_PROVIDER=local but the local enricher is unavailable "
                "and no fallback is permitted in local mode."
            )
        if not api_key:
            # auto with no key and local dead: nothing left to try.
            raise LocalEnricherUnavailable(
                "auto: local enricher unavailable and no frontier api_key to "
                "escalate to."
            )
        result = await self._enrich_frontier(context_row, api_key, frontier_model)
        return ProviderEnrichment(
            data=result.data,
            backend=result.backend,
            input_tokens=result.input_tokens,
            output_tokens=result.output_tokens,
            confidence=result.confidence,
            escalated=True,
        )

    async def _enrich_frontier(
        self, context_row: dict, api_key: str, frontier_model: str
    ) -> ProviderEnrichment:
        """Call Claude for one repo (the escalation / frontier path). Mirrors the
        existing ``main._enrich_payloads_with_ai`` Anthropic call so behavior is
        unchanged when provider=frontier."""
        if not api_key:
            raise RuntimeError(
                "ENRICHMENT_PROVIDER=frontier but ANTHROPIC_API_KEY is not set."
            )
        try:
            import anthropic  # noqa: PLC0415
        except ImportError as exc:
            raise RuntimeError(
                "anthropic SDK not installed; cannot use the frontier enrichment "
                "path. Install `anthropic` or set ENRICHMENT_PROVIDER=local."
            ) from exc

        client = anthropic.AsyncAnthropic(api_key=api_key)
        try:
            prompt = ENRICHMENT_PROMPT.format(
                repo_context=_build_repo_context(context_row)
            )
            response = await client.messages.create(
                model=frontier_model,
                max_tokens=800,
                messages=[{"role": "user", "content": prompt}],
            )
            data = _parse_enrichment_response(response.content[0].text)
            return ProviderEnrichment(
                data=data,
                backend=f"frontier:{frontier_model}",
                input_tokens=int(getattr(response.usage, "input_tokens", 0) or 0),
                output_tokens=int(getattr(response.usage, "output_tokens", 0) or 0),
                confidence=1.0,
                escalated=False,
            )
        finally:
            await client.close()


__all__ = [
    "EnrichmentProvider",
    "ProviderEnrichment",
    "resolve_provider",
    "PROVIDER_LOCAL",
    "PROVIDER_FRONTIER",
    "PROVIDER_AUTO",
    "VALID_PROVIDERS",
    "DEFAULT_ESCALATION_THRESHOLD",
]
