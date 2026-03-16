import httpx
from ..config import get_settings


class EmbeddingGenerator:
    """
    Generates embeddings using nomic-embed-text via Ollama.
    Input: repo name + description + tags + summary
    Output: list[float] (768-dimensional)
    Falls back gracefully if Ollama unavailable.
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

    async def generate(
        self,
        name: str,
        description: str | None,
        tags: list[str],
        summary: str | None = None,
    ) -> list[float] | None:
        if self._available is None:
            await self.check_available()

        if not self._available:
            return None

        parts = [name]
        if description:
            parts.append(description)
        if tags:
            parts.append(' '.join(tags))
        if summary:
            parts.append(summary)

        text = ' '.join(parts)[:4096]

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(
                    f'{self.settings.ollama_url}/api/embeddings',
                    json={
                        'model': self.settings.embedding_model,
                        'prompt': text,
                    }
                )
                if resp.status_code == 200:
                    data = resp.json()
                    return data.get('embedding')
                self._available = False
                return None
        except Exception:
            self._available = False
            return None
