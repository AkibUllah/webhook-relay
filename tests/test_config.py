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
