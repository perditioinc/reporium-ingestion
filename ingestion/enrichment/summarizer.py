import httpx
from ..config import get_settings


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
        if not readme:
            return None
        # Return first non-empty paragraph
        for para in readme.split('\n\n'):
            cleaned = para.strip().lstrip('#').strip()
            # Skip lines that are just images, badges, or headings
            if cleaned and not cleaned.startswith('!') and len(cleaned) > 30:
                return cleaned[:500]
        return None
