from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from webhook_relay.config import Settings
from webhook_relay.database import init_db
from webhook_relay.main import create_app

API_KEY = "test-admin-key"
T0 = datetime(2026, 10, 21, 9, 0, 0, tzinfo=timezone.utc)


@dataclass
class FakeTarget:
    """Receives deliveries; `responses` maps URL path -> list of status codes (or exceptions)."""

    requests: list[httpx.Request] = field(default_factory=list)
    scripts: dict[str, list] = field(default_factory=dict)
    default_status: int = 200

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        script = self.scripts.get(request.url.path)
        outcome = script.pop(0) if script else self.default_status
        if isinstance(outcome, Exception):
            raise outcome
        return httpx.Response(outcome, json={"ok": outcome < 300})

    def for_path(self, path: str) -> list[httpx.Request]:
        return [r for r in self.requests if r.url.path == path]


class Clock:
    def __init__(self, start: datetime = T0) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **kwargs) -> datetime:
        self.now += timedelta(**kwargs)
        return self.now


@pytest.fixture
def settings() -> Settings:
    return Settings(
        _env_file=None,
        app_env="test",
        database_url="sqlite://",
        api_key=API_KEY,
        worker_enabled=False,
        jitter_fraction=0.0,
        max_attempts=4,
        backoff_schedule="10,60,300",
        circuit_failure_threshold=3,
        circuit_cooldown_seconds=120,
        log_level="WARNING",
    )


@pytest.fixture
def target() -> FakeTarget:
    return FakeTarget()


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def app(settings, target, clock):
    return create_app(settings, transport=httpx.MockTransport(target.handler), clock=clock)


@pytest.fixture
def client(app):
    with TestClient(app) as c:
        yield c


@pytest.fixture
def auth() -> dict[str, str]:
    return {"X-API-Key": API_KEY}


@pytest.fixture
def dispatcher(app):
    return app.state.dispatcher


def register(client, auth, path="/hooks", **overrides) -> dict:
    body = {"url": f"https://merchant.example.com{path}", "secret": "s3cr3t-s3cr3t-s3cr3t"}
    body.update(overrides)
    r = client.post("/v1/endpoints", json=body, headers=auth)
    assert r.status_code == 201, r.text
    return r.json()


def publish(client, auth, type="payment.captured", **overrides) -> dict:
    body = {
        "type": type,
        "data": {"payment_id": "pay_1", "amount_minor": 150000, "currency": "BDT"},
    }
    body.update(overrides)
    r = client.post("/v1/events", json=body, headers=auth)
    assert r.status_code in (200, 201), r.text
    return r.json()


# -- PostgreSQL (claiming and retention suites; CI runs them against a service container) --

PG_URL = os.environ.get("TEST_DATABASE_URL")
pg_only = pytest.mark.skipif(not PG_URL, reason="set TEST_DATABASE_URL to a PostgreSQL database")


@pytest.fixture
def pg_sessions():
    settings = Settings(_env_file=None, database_url=PG_URL)
    engine = create_engine(settings.database_url, future=True)
    with engine.begin() as conn:
        for table in ("delivery_attempts", "deliveries", "events", "endpoints"):
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {table} CASCADE")
    init_db(engine)
    yield settings, sessionmaker(bind=engine, expire_on_commit=False)
    engine.dispose()
