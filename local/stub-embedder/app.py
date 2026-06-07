"""
Local Ollama-compatible stub embedder for the $0 substrate.

reporium-ingestion's `ingestion/enrichment/embeddings.py` talks to an
Ollama-style API (`GET /api/tags`, `POST /api/embeddings`). The production
path uses local sentence-transformers (already $0) or Ollama; for a fast,
dependency-light, deterministic local smoke we return a hashed 384-dim unit
vector per input. No model download, no network, no cost.

Swap this service for a real `ollama/ollama` container if you want true
semantic vectors locally — the API shape is identical.
"""
import hashlib
import json
import math
from http.server import BaseHTTPRequestHandler, HTTPServer

DIM = 384


def embed(text: str) -> list[float]:
    """Deterministic pseudo-embedding: seed a stream from the text hash, then
    L2-normalize to a unit vector so cosine distance is well defined."""
    seed = hashlib.sha256(text.encode("utf-8")).digest()
    vals: list[float] = []
    counter = 0
    while len(vals) < DIM:
        block = hashlib.sha256(seed + counter.to_bytes(4, "big")).digest()
        for i in range(0, len(block), 2):
            if len(vals) >= DIM:
                break
            n = int.from_bytes(block[i:i + 2], "big")
            vals.append((n / 65535.0) * 2.0 - 1.0)  # [-1, 1]
        counter += 1
    norm = math.sqrt(sum(v * v for v in vals)) or 1.0
    return [v / norm for v in vals]


class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, body: dict) -> None:
        payload = json.dumps(body).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):  # noqa: N802
        if self.path == "/api/tags":
            self._send(200, {"models": [{"name": "stub-embed-384"}]})
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):  # noqa: N802
        if self.path == "/api/embeddings":
            length = int(self.headers.get("Content-Length", 0))
            data = json.loads(self.rfile.read(length) or b"{}")
            prompt = data.get("prompt", "")
            self._send(200, {"embedding": embed(prompt)})
        else:
            self._send(404, {"error": "not found"})

    def log_message(self, *args):  # silence default request logging
        return


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", 11434), Handler).serve_forever()
