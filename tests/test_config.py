from __future__ import annotations

import pytest

from webhook_relay.config import Settings


@pytest.mark.parametrize(
    "given",
    [
        "postgres://relay:secret@db.render.internal:5432/relay",
        "postgresql://relay:secret@db.render.internal:5432/relay",
    ],
)
def test_managed_postgres_urls_are_rewritten_to_psycopg3(given: str) -> None:
    settings = Settings(_env_file=None, database_url=given)
    assert (
        settings.database_url == "postgresql+psycopg://relay:secret@db.render.internal:5432/relay"
    )


@pytest.mark.parametrize(
    "given",
    ["sqlite://", "sqlite:///./relay.db", "postgresql+psycopg://relay:relay@db:5432/relay"],
)
def test_explicit_driver_urls_are_left_alone(given: str) -> None:
    assert Settings(_env_file=None, database_url=given).database_url == given


def test_retention_is_off_by_default() -> None:
    settings = Settings(_env_file=None)
    assert settings.retention_days is None
    assert settings.retention_batch_size == 2000
    assert settings.retention_interval_seconds == 3600


def test_retention_settings_from_environment(monkeypatch) -> None:
    monkeypatch.setenv("RETENTION_DAYS", "30")
    monkeypatch.setenv("RETENTION_BATCH_SIZE", "500")
    settings = Settings(_env_file=None)
    assert (settings.retention_days, settings.retention_batch_size) == (30, 500)


@pytest.mark.parametrize(
    "field", [{"retention_days": 0}, {"retention_batch_size": 0}, {"retention_interval_seconds": 5}]
)
def test_retention_settings_reject_nonsense(field) -> None:
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        Settings(_env_file=None, **field)
