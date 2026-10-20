from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    api_key: str = Field(min_length=1)
    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"

    gateway_min_delay_seconds: float = Field(default=2, ge=0)
    gateway_max_delay_seconds: float = Field(default=5, ge=0)
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)

    max_attempts: int = Field(default=3, ge=1)
    retry_base_delay_seconds: float = Field(default=2, gt=0)

    webhook_timeout_seconds: float = Field(default=5, gt=0)
    outbox_poll_interval_seconds: float = Field(default=0.5, gt=0)
    outbox_batch_size: int = Field(default=100, ge=1)
    consumer_prefetch_count: int = Field(default=10, ge=1)


@lru_cache
def get_settings() -> Settings:
    return Settings()
