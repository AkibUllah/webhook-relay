"""Retention: which rows are purged, batching, orphaned events and the disabled default."""

from datetime import timedelta

import pytest
from sqlalchemy import func, select

from tests.conftest import T0, publish, register
from webhook_relay.models import Delivery, DeliveryAttempt, Endpoint, Event, utc_now
from webhook_relay.services.retention import RetentionService

DAY = timedelta(days=1)


def counts(app) -> dict[str, int]:
    with app.state.session_factory() as db:
        return {
            "events": db.scalar(select(func.count(Event.id))),
            "deliveries": db.scalar(select(func.count(Delivery.id))),
            "attempts": db.scalar(select(func.count(DeliveryAttempt.id))),
        }


def statuses(app) -> dict[str, str]:
    with app.state.session_factory() as db:
        return {d.event_id: d.status for d in db.scalars(select(Delivery))}


def seed(app, *rows) -> None:
    seed_rows(app.state.session_factory, *rows)


def seed_rows(session_factory, *rows) -> None:
    """rows: (event_id, status, delivered_at or None, attempts). One endpoint, one delivery each."""
    with session_factory() as db:
        endpoint = Endpoint(url="https://merchant.example.com/hooks", secret="s" * 24)
        db.add(endpoint)
        db.flush()
        for event_id, status, delivered_at, attempts in rows:
            created = (delivered_at or T0) - timedelta(minutes=1)
            db.add(Event(id=event_id, type="order.paid", payload={}, created_at=created))
            delivery = Delivery(
                event_id=event_id,
                endpoint_id=endpoint.id,
                status=status,
                delivered_at=delivered_at,
                created_at=created,
            )
            delivery.attempts = [
                DeliveryAttempt(attempt_number=n + 1, succeeded=n == attempts - 1)
                for n in range(attempts)
            ]
            db.add(delivery)
        db.commit()


@pytest.fixture
def retention(app, client, settings):  # client starts the app, which creates the tables
    settings.retention_days = 30
    return RetentionService(app.state.session_factory, settings)


def test_disabled_by_default_keeps_everything(app, client, settings):
    seed(app, ("evt_old", "succeeded", T0 - 400 * DAY, 1))
    result = RetentionService(app.state.session_factory, settings).purge(T0)
    assert not result.enabled and result.as_dict()["cutoff"] is None
    assert counts(app) == {"events": 1, "deliveries": 1, "attempts": 1}


def test_purges_only_old_succeeded_deliveries_with_their_attempts(app, retention):
    seed(
        app,
        ("evt_old_ok", "succeeded", T0 - 31 * DAY, 3),
        ("evt_new_ok", "succeeded", T0 - 29 * DAY, 1),
        ("evt_old_dead", "dead", None, 4),
        ("evt_pending", "pending", None, 2),
        ("evt_inflight", "delivering", None, 1),
    )
    result = retention.purge(T0)
    assert (result.deliveries, result.attempts, result.events) == (1, 3, 1)
    assert result.cutoff == T0 - 30 * DAY
    assert statuses(app) == {
        "evt_new_ok": "succeeded",
        "evt_old_dead": "dead",
        "evt_pending": "pending",
        "evt_inflight": "delivering",
    }
    assert counts(app) == {"events": 4, "deliveries": 4, "attempts": 8}


def test_cutoff_is_exclusive(app, retention):
    cutoff = T0 - 30 * DAY
    seed(
        app,
        ("evt_at", "succeeded", cutoff, 1),
        ("evt_before", "succeeded", cutoff - timedelta(seconds=1), 1),
    )
    retention.purge(T0)
    assert set(statuses(app)) == {"evt_at"}


def test_event_is_kept_while_any_delivery_remains(app, retention):
    seed(app, ("evt_shared", "succeeded", T0 - 60 * DAY, 1))
    with app.state.session_factory() as db:
        other = Endpoint(url="https://other.example.com/hooks", secret="s" * 24)
        db.add(other)
        db.flush()
        db.add(Delivery(event_id="evt_shared", endpoint_id=other.id, status="dead"))
        db.commit()
    result = retention.purge(T0)
    assert (result.deliveries, result.events) == (1, 0)
    assert counts(app)["events"] == 1


def test_old_events_that_never_matched_an_endpoint_are_removed(app, retention):
    with app.state.session_factory() as db:
        db.add(Event(id="evt_nobody", type="x", payload={}, created_at=T0 - 90 * DAY))
        db.add(Event(id="evt_recent", type="x", payload={}, created_at=T0 - DAY))
        db.commit()
    assert retention.purge(T0).events == 1
    with app.state.session_factory() as db:
        assert db.scalars(select(Event.id)).all() == ["evt_recent"]


def test_deletes_in_bounded_batches(app, retention, settings):
    settings.retention_batch_size = 2
    seed(app, *[(f"evt_{i}", "succeeded", T0 - (40 + i) * DAY, 1) for i in range(5)])
    result = retention.purge(T0)
    assert (result.deliveries, result.attempts, result.events) == (5, 5, 5)
    # deliveries: 2 + 2 + 1, events: 2 + 2 + 1
    assert result.batches == 6
    assert counts(app) == {"events": 0, "deliveries": 0, "attempts": 0}
    assert retention.purge(T0).batches == 0  # nothing left: a single empty probe per table


async def test_end_to_end_with_the_dispatcher(
    app, client, auth, dispatcher, clock, retention, target
):
    register(client, auth, path="/ok")
    register(client, auth, path="/down")
    target.scripts["/down"] = [500] * 10
    publish(client, auth)
    await dispatcher.run_once()
    for _ in range(3):  # walk the failing delivery to the dead-letter queue (max_attempts=4)
        clock.advance(days=1)
        await dispatcher.run_once()

    clock.advance(days=31)
    result = retention.purge(clock())
    assert (result.deliveries, result.attempts, result.events) == (1, 1, 0)
    remaining = client.get("/v1/deliveries", headers=auth).json()
    assert [d["status"] for d in remaining] == ["dead"]
    assert counts(app) == {"events": 1, "deliveries": 1, "attempts": 4}


async def test_replayed_delivery_is_not_purged(app, client, auth, dispatcher, clock, retention):
    register(client, auth)
    publish(client, auth)
    await dispatcher.run_once()
    clock.advance(days=45)
    with app.state.session_factory() as db:
        delivery_id = db.scalar(select(Delivery.id))
    assert client.post(f"/v1/deliveries/{delivery_id}/replay", headers=auth).status_code == 202
    assert retention.purge(clock()).deliveries == 0
    assert list(statuses(app).values()) == ["pending"]


# -- worker loop --------------------------------------------------------------------------


async def test_worker_purges_at_most_once_per_interval(
    app, client, auth, dispatcher, clock, settings
):
    settings.retention_days = 30
    settings.retention_interval_seconds = 3600
    seed(app, ("evt_a", "succeeded", T0 - 40 * DAY, 2))

    first = await dispatcher.maybe_purge()
    assert first is not None and first.deliveries == 1
    seed(app, ("evt_b", "succeeded", T0 - 40 * DAY, 1))
    clock.advance(minutes=59)
    assert await dispatcher.maybe_purge() is None  # too soon
    assert "evt_b" in statuses(app)
    clock.advance(minutes=2)
    assert (await dispatcher.maybe_purge()).deliveries == 1

    worker = client.get("/metrics").json()["worker"]["retention"]
    assert worker == {
        "purges": 2,
        "deliveries": 2,
        "attempts": 3,
        "events": 2,
        "last_purge_at": clock().isoformat(),
    }


async def test_worker_skips_retention_when_disabled(app, client, dispatcher):
    seed(app, ("evt_a", "succeeded", T0 - 400 * DAY, 1))
    assert await dispatcher.maybe_purge() is None
    assert dispatcher.stats.purges == 0
    assert counts(app)["deliveries"] == 1


def test_in_app_worker_loop_applies_retention(settings, target, tmp_path):
    import asyncio

    import httpx
    from fastapi.testclient import TestClient

    from webhook_relay.main import create_app

    settings.worker_enabled = True
    settings.poll_interval_seconds = 0.05
    settings.retention_days = 30
    # A file database gives the background purge its own connection, as in production; the
    # shared in-memory connection would let it see the seed's uncommitted rows.
    settings.database_url = f"sqlite:///{tmp_path / 'relay.db'}"
    app = create_app(settings, transport=httpx.MockTransport(target.handler))
    with TestClient(app) as client:
        seed(app, ("evt_ancient", "succeeded", T0 - 4000 * DAY, 1))
        app.state.dispatcher.stats.last_purge_at = None  # the first loop pass may have run already
        empty = {"events": 0, "deliveries": 0, "attempts": 0}
        for _ in range(100):  # wait for the whole purge (deliveries, then events) to land
            if counts(app) == empty and app.state.dispatcher.stats.purged_events:
                break
            client.portal.call(asyncio.sleep, 0.05)
        assert counts(app) == empty
        assert client.get("/metrics").json()["worker"]["retention"]["deliveries"] == 1


# -- python -m webhook_relay.worker --purge-once ----------------------------------------------


@pytest.fixture
def file_db(tmp_path, monkeypatch):
    from webhook_relay.config import get_settings

    url = f"sqlite:///{tmp_path / 'relay.db'}"
    monkeypatch.setenv("DATABASE_URL", url)
    monkeypatch.setenv("LOG_LEVEL", "WARNING")
    monkeypatch.delenv("RETENTION_DAYS", raising=False)
    monkeypatch.chdir(tmp_path)  # no stray .env from the repo
    get_settings.cache_clear()
    yield url
    get_settings.cache_clear()


def _seed_file_db(url: str) -> None:
    from webhook_relay.database import init_db, make_engine, make_session_factory

    engine = make_engine(url)
    init_db(engine)
    seed_rows(
        make_session_factory(engine),
        ("evt_old", "succeeded", utc_now() - 90 * DAY, 2),
        ("evt_dead", "dead", None, 1),
    )
    engine.dispose()


def test_purge_once_cli(file_db, monkeypatch, capsys):
    import json

    from webhook_relay import worker

    _seed_file_db(file_db)
    monkeypatch.setenv("RETENTION_DAYS", "30")
    assert worker.main(["--purge-once"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert (out["deliveries"], out["attempts"], out["events"]) == (1, 2, 1)
    assert worker.main(["--purge-once"]) == 0
    assert json.loads(capsys.readouterr().out)["deliveries"] == 0


def test_purge_once_cli_days_override_and_disabled(file_db, capsys):
    import json

    from webhook_relay import worker

    _seed_file_db(file_db)
    assert worker.main(["--purge-once"]) == 2
    assert "RETENTION_DAYS" in capsys.readouterr().err
    assert worker.main(["--purge-once", "--days", "120"]) == 0  # 90-day-old row is kept
    assert json.loads(capsys.readouterr().out)["deliveries"] == 0
    assert worker.main(["--purge-once", "--days", "60"]) == 0
    assert json.loads(capsys.readouterr().out)["deliveries"] == 1


@pytest.mark.parametrize("argv", [["--days", "30"], ["--purge-once", "--days", "0"]])
def test_purge_once_cli_rejects_bad_arguments(argv):
    from webhook_relay import worker

    with pytest.raises(SystemExit) as exc:
        worker.main(argv)
    assert exc.value.code == 2
