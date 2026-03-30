"""
One-shot: enrich the 338 repos with quality_signals IS NULL using claude-sonnet-4-20250514.
These are repos that got description-based fallback summaries on 2026-03-30.
After this run, ai_enricher.py default switches to claude-haiku-4-5 for future passes.
"""
import asyncio
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google.cloud import secretmanager

def get_secret(name):
    client = secretmanager.SecretManagerServiceClient()
    resp = client.access_secret_version(request={"name": f"projects/perditio-platform/secrets/{name}/versions/latest"})
    return resp.payload.data.decode().strip()

from ingestion.enrichers.ai_enricher import run_ai_enrichment

async def main():
    db_url = get_secret("reporium-db-url-async")
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
