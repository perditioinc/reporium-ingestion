"""
One-shot: enrich the 338 repos with quality_signals IS NULL using claude-sonnet-4-20250514.
These are repos that got description-based fallback summaries on 2026-03-30.
After this run, ai_enricher.py default switches to claude-haiku-4-5 for future passes.
"""
import asyncio
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import urllib.parse
from google.cloud import secretmanager

def get_secret(name):
    client = secretmanager.SecretManagerServiceClient()
    resp = client.access_secret_version(request={"name": f"projects/perditio-platform/secrets/{name}/versions/latest"})
    return resp.payload.data.decode().strip()

def normalize_db_url(url: str) -> str:
    """Strip asyncpg driver suffix and convert ?ssl=true to sslmode=require for psycopg2."""
    url = url.replace("+asyncpg", "")
    if url.startswith("postgresql+"):
        url = "postgresql" + url[url.index("://"):]
    parsed = urllib.parse.urlsplit(url)
    params = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    ssl_val = params.pop("ssl", [None])[0]
    if ssl_val and "sslmode" not in params:
        if ssl_val.lower() in ("true", "1", "require"):
            params["sslmode"] = ["require"]
        elif ssl_val.lower() in ("false", "0", "disable"):
            params["sslmode"] = ["disable"]
    new_query = urllib.parse.urlencode({k: v[0] for k, v in params.items()})
    return urllib.parse.urlunsplit(parsed._replace(query=new_query))

from ingestion.enrichers.ai_enricher import run_ai_enrichment

async def main():
    db_url = normalize_db_url(get_secret("reporium-db-url-async"))
    api_key = get_secret("anthropic-api-key")
    stats = await run_ai_enrichment(
        db_url=db_url,
        api_key=api_key,
        model="claude-sonnet-4-20250514",
        base_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    )
    print(f"\nDone: {stats.enriched} enriched, {stats.errors} errors")

if __name__ == "__main__":
    asyncio.run(main())
