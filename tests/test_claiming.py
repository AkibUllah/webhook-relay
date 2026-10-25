"""Claiming: leases, crash recovery and (on PostgreSQL) SKIP LOCKED isolation between workers."""

import os
from datetime import timedelta

import httpx
import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from tests.conftest import T0, publish, register
from webhook_relay.config import Settings
from webhook_relay.database import init_db
from webhook_relay.models import Delivery, Endpoint, Event
from webhook_relay.services.dispatcher import Dispatcher


def _delivery(app, delivery_id=None) -> Delivery:
    with app.state.session_factory() as db:
        stmt = select(Delivery)
        if delivery_id:
            stmt = stmt.where(Delivery.id == delivery_id)
        return db.scalars(stmt).one()


def test_lease_defaults_to_a_full_batch_timing_out():
    s = Settings(_env_file=None, batch_size=20, http_timeout_seconds=5)
    assert s.lease_seconds == 20 * 5 + 60
    assert Settings(_env_file=None, claim_lease_seconds=45).lease_seconds == 45


def test_worker_id_defaults_to_host_and_pid():
    worker_id = Settings(_env_file=None).worker_id
    assert worker_id.endswith(f"-{os.getpid()}")


async def test_claim_records_owner_and_lease_then_clears_after_attempt(
    app, client, auth, dispatcher, clock, settings
):
    settings.worker_id = "worker-a"
    settings.claim_lease_seconds = 90
    register(client, auth)
    publish(client, auth)

    with app.state.session_factory() as db:
        claimed = dispatcher._claim_due(db, clock())
        assert [d.claimed_by for d in claimed] == ["worker-a"]
        assert claimed[0].status == "delivering"
        delivery_id = claimed[0].id
    held = _delivery(app, delivery_id)
    assert held.claimed_by == "worker-a"
    assert held.claimed_until.replace(tzinfo=T0.tzinfo) == T0 + timedelta(seconds=90)

    # The claimed row is not handed out again while the lease is live.
    with app.state.session_factory() as db:
        assert dispatcher._claim_due(db, clock() + timedelta(seconds=30)) == []

    # Once the lease runs out the row is due again; this attempt completes and releases it.
    clock.advance(seconds=95)
    assert await dispatcher.run_once() == 1  # lease expired -> reclaimed and sent
    done = _delivery(app, delivery_id)
    assert done.status == "succeeded"
    assert done.claimed_by is None and done.claimed_until is None


async def test_expired_lease_is_reclaimed_after_a_worker_crash(
    app, client, auth, dispatcher, clock, settings, target
):
    settings.claim_lease_seconds = 60
    register(client, auth)
    publish(client, auth)
    with app.state.session_factory() as db:
        # Worker "dead-pod" claims the row and then disappears without recording anything.
        dead = Dispatcher(
            app.state.session_factory, settings.model_copy(update={"worker_id": "dead-pod"})
        )
        dead._claim_due(db, clock())

    assert await dispatcher.run_once() == 0  # still leased
    assert target.requests == []

    clock.advance(seconds=61)
    assert await dispatcher.run_once() == 1
    assert dispatcher.stats.reclaimed == 1
    assert len(target.requests) == 1
    assert _delivery(app).status == "succeeded"
    assert dispatcher.stats.as_dict()["reclaimed"] == 1


async def test_retry_after_failure_releases_the_claim(app, client, auth, dispatcher, target):
    register(client, auth)
    publish(client, auth)
    target.default_status = 500
    assert await dispatcher.run_once() == 1
    retrying = _delivery(app)
    assert retrying.status == "pending"
    assert retrying.claimed_by is None and retrying.claimed_until is None


def test_replay_clears_a_stale_claim(app, client, auth, dispatcher, clock):
    register(client, auth)
    publish(client, auth)
    with app.state.session_factory() as db:
        delivery_id = dispatcher._claim_due(db, clock())[0].id
    r = client.post(f"/v1/deliveries/{delivery_id}/replay", headers=auth)
    assert r.status_code == 202
    body = r.json()
    assert body["status"] == "pending"
    assert body["claimed_by"] is None and body["claimed_until"] is None


def test_api_exposes_claim_owner_while_in_flight(app, client, auth, dispatcher, clock, settings):
    settings.worker_id = "worker-b"
    register(client, auth)
    publish(client, auth)
    with app.state.session_factory() as db:
        delivery_id = dispatcher._claim_due(db, clock())[0].id
    body = client.get(f"/v1/deliveries/{delivery_id}", headers=auth).json()
    assert body["status"] == "delivering"
    assert body["claimed_by"] == "worker-b"
    assert body["claimed_until"] is not None


# --------------------------------------------------------------------------- PostgreSQL
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


def _seed(sessions, n: int) -> None:
    with sessions() as db:
        endpoint = Endpoint(
            url="https://merchant.example.com/hooks", secret="s" * 24, event_types=["*"]
        )
        db.add(endpoint)
        db.flush()
        for i in range(n):
            event = Event(id=f"evt_{i}", type="payment.captured", payload={}, created_at=T0)
            db.add(event)
            db.add(
                Delivery(
                    event_id=event.id,
                    endpoint_id=endpoint.id,
                    status="pending",
                    next_attempt_at=T0 - timedelta(seconds=i),
                    created_at=T0,
                )
            )
        db.commit()


@pg_only
def test_concurrent_claimers_get_disjoint_batches_on_postgres(pg_sessions):
    settings, sessions = pg_sessions
    settings = settings.model_copy(update={"batch_size": 5})
    _seed(sessions, 8)
    a = Dispatcher(sessions, settings.model_copy(update={"worker_id": "a"}))
    b = Dispatcher(sessions, settings.model_copy(update={"worker_id": "b"}))

    # Worker A selects and locks its batch but has not committed yet ...
    tx_a = sessions()
    rows_a = list(tx_a.scalars(a.claim_statement(T0)).all())
    # ... meanwhile worker B claims: locked rows are skipped, not waited on or duplicated.
    with sessions() as tx_b:
        claimed_b = b._claim_due(tx_b, T0)
    ids_a = {d.id for d in rows_a}
    ids_b = {d.id for d in claimed_b}
    tx_a.rollback()
    tx_a.close()

    assert len(ids_a) == 5
    assert len(ids_b) == 3
    assert ids_a.isdisjoint(ids_b)


@pg_only
async def test_two_workers_deliver_each_event_exactly_once_on_postgres(pg_sessions):
    settings, sessions = pg_sessions
    settings = settings.model_copy(update={"batch_size": 3, "jitter_fraction": 0.0})
    _seed(sessions, 10)
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers["X-Relay-Event-Id"])
        return httpx.Response(200)

    workers = [
        Dispatcher(
            sessions,
            settings.model_copy(update={"worker_id": name}),
            transport=httpx.MockTransport(handler),
            clock=lambda: T0,
        )
        for name in ("a", "b")
    ]
    for _ in range(4):
        for w in workers:
            await w.run_once()

    assert sorted(seen) == sorted(f"evt_{i}" for i in range(10))
    assert len(seen) == len(set(seen))
    with sessions() as db:
        statuses = set(db.scalars(select(Delivery.status)).all())
    assert statuses == {"succeeded"}
