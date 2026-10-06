from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Конфигурация сервиса. Значения читаются из переменных окружения / .env."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # API
    api_key: str = "super-secret-api-key"

    # Инфраструктура
    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"

    # Эмуляция обработки платежа
    process_min_seconds: float = 2
    process_max_seconds: float = 5
    success_rate: float = 0.9

    # Retry / DLQ
    max_retries: int = 3
    retry_base_delay: float = 2

    # Outbox relay
    outbox_poll_interval: float = 0.5
    outbox_batch_size: int = 100


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
