from __future__ import annotations

import os
import socket
from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from webhook_relay.core.backoff import DEFAULT_SCHEDULE, parse_schedule


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_env: str = Field(default="development", pattern="^(development|test|staging|production)$")
    log_level: str = "INFO"
    database_url: str = "sqlite:///./relay.db"
    api_key: str | None = None

    worker_enabled: bool = True
    poll_interval_seconds: float = 2.0
    batch_size: int = Field(default=50, ge=1, le=1000)
    http_timeout_seconds: float = 10.0
    max_attempts: int = Field(default=7, ge=1, le=50)
    backoff_schedule: str = ",".join(str(s) for s in DEFAULT_SCHEDULE)
    jitter_fraction: float = Field(default=0.1, ge=0, lt=1)
    signature_tolerance_seconds: int = 300

    worker_id: str = Field(default_factory=lambda: f"{socket.gethostname()}-{os.getpid()}")
    # How long a claimed delivery stays reserved for the worker that claimed it. If the worker
    # dies mid-batch the lease runs out and another worker picks the delivery up again.
    # Unset = long enough for a full batch to time out: batch_size * http_timeout + 60s.
    claim_lease_seconds: int | None = Field(default=None, ge=1)

    circuit_failure_threshold: int = Field(default=5, ge=1)
    circuit_cooldown_seconds: int = Field(default=300, ge=1)

    @field_validator("backoff_schedule")
    @classmethod
    def _valid_schedule(cls, value: str) -> str:
        parse_schedule(value)
        return value

    @field_validator("database_url")
    @classmethod
    def _normalise_database_url(cls, value: str) -> str:
        """Managed Postgres (Render, Heroku) hands out ``postgres://`` or ``postgresql://`` URLs;
        SQLAlchemy needs the installed psycopg 3 driver spelled out."""
        for prefix in ("postgres://", "postgresql://"):
            if value.startswith(prefix):
                return "postgresql+psycopg://" + value[len(prefix) :]
        return value

    @property
    def lease_seconds(self) -> int:
        if self.claim_lease_seconds is not None:
            return self.claim_lease_seconds
        return int(self.batch_size * self.http_timeout_seconds) + 60

    @property
    def schedule(self) -> tuple[int, ...]:
        return parse_schedule(self.backoff_schedule)


@lru_cache
def get_settings() -> Settings:
    return Settings()
