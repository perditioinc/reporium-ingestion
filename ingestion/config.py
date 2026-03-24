import os
import logging
from pydantic_settings import BaseSettings
from pydantic import Field, model_validator
from enum import Enum

logger = logging.getLogger(__name__)


def _load_secret(secret_id: str, project_id: str = "perditio-platform") -> str | None:
    """
    Load a secret from GCP Secret Manager, stripping any trailing whitespace/newlines.
    Returns None if the secret is not found or GCP is not available.
    This is the ONLY place secrets are loaded from GCP — all \r\n stripping happens here.
    """
    try:
        from google.cloud import secretmanager
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8").strip()
    except Exception as exc:
        logger.debug("Could not load secret %s from GCP: %s", secret_id, exc)
        return None


def _resolve_credential(env_var: str, secret_id: str | None = None) -> str | None:
    """
    Resolve a credential from (in order): GCP Secret Manager → .env → system environment.
    Always strips whitespace. Never logs the value.
    """
    # 1. GCP Secret Manager
    if secret_id:
        val = _load_secret(secret_id)
        if val:
            return val

    # 2. Environment variable (includes .env loaded by pydantic)
    val = os.getenv(env_var)
    if val:
        return val.strip()

    return None


class RunMode(str, Enum):
    QUICK = 'quick'
    WEEKLY = 'weekly'
    FULL = 'full'
    FIX = 'fix'


class Settings(BaseSettings):
    # GitHub
    gh_token: str = Field(..., env='GH_TOKEN')
    gh_username: str = Field('perditioinc', env='GH_USERNAME')

    # reporium-api
    reporium_api_url: str = Field('http://localhost:8000', env='REPORIUM_API_URL')
    reporium_api_key: str = Field('', env='REPORIUM_API_KEY')
    ingest_api_key: str = Field('', env='INGEST_API_KEY')

    # Claude API (replaces Ollama)
    anthropic_api_key: str = Field('', env='ANTHROPIC_API_KEY')
    enrichment_model: str = Field('claude-sonnet-4-20250514', env='ENRICHMENT_MODEL')

    # Embeddings (local sentence-transformers, free)
    embedding_model: str = Field('all-MiniLM-L6-v2', env='EMBEDDING_MODEL')

    # Database
    database_url: str = Field('', env='DATABASE_URL')

    # Run mode
    default_run_mode: RunMode = Field(RunMode.QUICK, env='DEFAULT_RUN_MODE')

    # Rate limit
    min_rate_limit_buffer: int = Field(100, env='MIN_RATE_LIMIT_BUFFER')
    max_concurrency: int = Field(2, env='MAX_CONCURRENCY')
    request_delay_ms: int = Field(500, env='REQUEST_DELAY_MS')

    # Cache
    cache_db_path: str = Field('./data/cache.db', env='CACHE_DB_PATH')

    model_config = {'env_file': '.env', 'env_file_encoding': 'utf-8', 'extra': 'ignore'}

    @model_validator(mode='after')
    def _strip_strings_and_resolve_secrets(self) -> 'Settings':
        """Strip whitespace from all string fields and resolve GCP secrets."""
        # Strip all string fields (catches \r\n from any source)
        for field_name in self.model_fields:
            val = getattr(self, field_name)
            if isinstance(val, str):
                object.__setattr__(self, field_name, val.strip())

        # Resolve secrets from GCP if not already set
        secret_map = {
            'anthropic_api_key': 'anthropic-api-key',
            'reporium_api_key': 'reporium-ingestion-api-key',
            'ingest_api_key': 'reporium-ingest-api-key',
            'database_url': 'reporium-db-url',
        }
        for field_name, secret_id in secret_map.items():
            current = getattr(self, field_name)
            if not current:
                resolved = _load_secret(secret_id)
                if resolved:
                    object.__setattr__(self, field_name, resolved)

        return self


_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
