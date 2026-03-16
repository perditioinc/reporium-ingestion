from pydantic_settings import BaseSettings
from pydantic import Field
from enum import Enum


class RunMode(str, Enum):
    QUICK = 'quick'
    WEEKLY = 'weekly'
    FULL = 'full'
    FIX = 'fix'


class Settings(BaseSettings):
    # GitHub
    gh_token: str = Field(..., env='GH_TOKEN')
    gh_username: str = Field(..., env='GH_USERNAME')

    # reporium-api
    reporium_api_url: str = Field('http://localhost:8000', env='REPORIUM_API_URL')
    reporium_api_key: str = Field(..., env='REPORIUM_API_KEY')

    # Ollama / AI
    ollama_url: str = Field('http://localhost:11434', env='OLLAMA_URL')
    ollama_model: str = Field('llama3.1:8b', env='OLLAMA_MODEL')
    embedding_model: str = Field('nomic-embed-text', env='EMBEDDING_MODEL')

    # Run mode
    default_run_mode: RunMode = Field(RunMode.QUICK, env='DEFAULT_RUN_MODE')
    quick_schedule: str = Field('0 9 * * *', env='QUICK_SCHEDULE')
    weekly_schedule: str = Field('0 2 * * 0', env='WEEKLY_SCHEDULE')
    full_schedule: str = Field('0 3 1 * *', env='FULL_SCHEDULE')

    # Rate limit
    min_rate_limit_buffer: int = Field(100, env='MIN_RATE_LIMIT_BUFFER')
    max_concurrency: int = Field(2, env='MAX_CONCURRENCY')
    request_delay_ms: int = Field(500, env='REQUEST_DELAY_MS')

    # Cache
    cache_db_path: str = Field('./data/cache.db', env='CACHE_DB_PATH')

    model_config = {'env_file': '.env', 'env_file_encoding': 'utf-8', 'extra': 'ignore'}


_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
