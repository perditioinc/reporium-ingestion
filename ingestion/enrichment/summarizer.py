import os

import httpx

from ..config import get_settings


def _fallback_min_chars() -> int:
    """
    Fallback summary floor — must match the KAN-191 probe's
    `PROBE_SUMMARY_MIN_CHARS` (default 50). Reading the same env var here
    keeps writer and reader on a single source of truth, so a future bump of
    the probe floor automatically tightens what fallbacks we emit. KAN-200.
    """
    raw = os.getenv("PROBE_SUMMARY_MIN_CHARS", "50")
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 50


class RepoSummarizer:
    """
    Generates 2-3 sentence summaries using local Ollama.
    Falls back to first paragraph of README if Ollama unavailable.
    """

    def __init__(self):
        self.settings = get_settings()
        self._available: bool | None = None

    async def check_available(self) -> bool:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f'{self.settings.ollama_url}/api/tags')
                self._available = resp.status_code == 200
        except Exception:
            self._available = False
        return self._available

    async def summarize(self, repo_name: str, readme: str, tags: list[str]) -> str | None:
        if self._available is None:
            await self.check_available()

        if not self._available:
            return self._fallback_summary(readme)

        prompt = (
            f'Summarize this GitHub repository in 2-3 sentences.\n'
            f'Repository: {repo_name}\n'
            f'Tags: {", ".join(tags)}\n'
            f'README: {readme[:2000]}\n\n'
            f'Write a clear, factual summary. Focus on what it does and who it\'s for. '
            f'Do not start with "This repository" or "This project".'
        )

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                resp = await client.post(
                    f'{self.settings.ollama_url}/api/generate',
                    json={
                        'model': self.settings.ollama_model,
                        'prompt': prompt,
                        'stream': False,
                    }
                )
                if resp.status_code == 200:
                    data = resp.json()
                    return data.get('response', '').strip() or self._fallback_summary(readme)
                self._available = False
                return self._fallback_summary(readme)
        except Exception:
            self._available = False
            return self._fallback_summary(readme)

    def _fallback_summary(self, readme: str | None) -> str | None:
        """
        First substantive paragraph of the README, capped at 500 chars.

        KAN-200: floor aligned with the KAN-191 probe (default 50 chars). The
        old `> 30` threshold produced strings the probe rejected as too_short
        — caller-side regression where 4 of 20 sample repos failed the
        probe's summary-length check (KAN-196 RCA F3). We now mirror the
        probe's `PROBE_SUMMARY_MIN_CHARS` env var; if no paragraph clears the
        floor we return `None` rather than write a too-short summary that
        will fail the next probe.
        """
        if not readme:
            return None
        min_chars = _fallback_min_chars()
        # Return first non-empty paragraph that meets the probe's floor
        for para in readme.split('\n\n'):
            cleaned = para.strip().lstrip('#').strip()
            # Skip lines that are just images, badges, or headings
            if cleaned and not cleaned.startswith('!') and len(cleaned) >= min_chars:
                return cleaned[:500]
        return None
