"""Limits across real concurrent workers: every worker here has its own connection and thread."""

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import httpx
from sqlalchemy import func, select, text

from tests.conftest import T0, pg_only
from webhook_relay.models import Delivery, DeliveryAttempt, Endpoint, Event
from webhook_relay.services.dispatcher import Dispatcher, SendResult

WORKERS = 4


def _seed(sessions, n: int, **limits) -> str:
    with sessions() as db:
        endpoint = Endpoint(
            url="https://merchant.example.com/hooks", secret="s" * 24, event_types=["*"], **limits
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
                    next_attempt_at=T0 - timedelta(seconds=1),
                    created_at=T0 + timedelta(milliseconds=i),
                )
            )
        db.commit()
        return endpoint.id


def _workers(settings, sessions, **kwargs) -> list[Dispatcher]:
    return [
        Dispatcher(
            sessions,
            settings.model_copy(update={"worker_id": f"w{i}", "jitter_fraction": 0.0}),
            **kwargs,
        )
        for i in range(WORKERS)
    ]


def _claim_together(workers, sessions, now) -> int:
    """Every worker claims at the same instant. Returns the number of rows they got."""
    barrier = threading.Barrier(len(workers))

    def claim(worker: Dispatcher) -> int:
        with sessions() as db:
            barrier.wait()
            return len(worker._claim_due(db, now))

    with ThreadPoolExecutor(len(workers)) as pool:
        return sum(pool.map(claim, workers))


@pg_only
def test_max_in_flight_holds_when_workers_race_on_postgres(pg_sessions):
    settings, sessions = pg_sessions
    _seed(sessions, 30, max_in_flight=3)
    workers = _workers(settings, sessions)

    claimed = sum(_claim_together(workers, sessions, T0) for _ in range(10))

    assert claimed == 3
    with sessions() as db:
        in_flight = db.scalar(
            select(func.count(Delivery.id)).where(Delivery.status == "delivering")
        )
    assert in_flight == 3


@pg_only
def test_max_rps_holds_when_workers_race_on_postgres(pg_sessions):
    settings, sessions = pg_sessions
    _seed(sessions, 200, max_rps=5)
    workers = _workers(settings, sessions)

    claimed = 0
    for tick in range(31):  # three seconds in 100ms steps
        claimed += _claim_together(workers, sessions, T0 + timedelta(milliseconds=100 * tick))

    # capacity + rate * seconds. A round lost to a held lock is made up by the next refill.
    assert 15 <= claimed <= 5 + 5 * 3
    with sessions() as db:
        waiting = list(db.scalars(select(Delivery).where(Delivery.status == "pending")))
    assert len(waiting) == 200 - claimed
    assert all(d.attempt_count == 0 for d in waiting)


@pg_only
def test_in_flight_cap_holds_end_to_end_across_workers_on_postgres(pg_sessions):
    settings, sessions = pg_sessions
    _seed(sessions, 12, max_in_flight=2)
    lock = threading.Lock()
    seen = {"open": 0, "peak": 0, "sent": 0}

    async def handler(_: httpx.Request) -> httpx.Response:
        with lock:
            seen["open"] += 1
            seen["peak"] = max(seen["peak"], seen["open"])
        await asyncio.sleep(0.02)
        with lock:
            seen["open"] -= 1
            seen["sent"] += 1
        return httpx.Response(200)

    workers = _workers(settings, sessions, transport=httpx.MockTransport(handler), clock=lambda: T0)

    def drive(worker: Dispatcher) -> None:
        async def rounds() -> None:
            for _ in range(200):
                if seen["sent"] >= 12:
                    return
                await worker.run_once()
                await asyncio.sleep(0.002)

        asyncio.run(rounds())

    with ThreadPoolExecutor(WORKERS) as pool:
        list(pool.map(drive, workers))

    assert seen["sent"] == 12
    assert seen["peak"] == 2
    with sessions() as db:
        assert db.scalar(select(func.count(DeliveryAttempt.id))) == 12
        assert set(db.scalars(select(Delivery.status))) == {"succeeded"}


@pg_only
def test_worker_passes_over_an_endpoint_being_claimed_on_postgres(pg_sessions):
    settings, sessions = pg_sessions
    endpoint_id = _seed(sessions, 3, max_rps=100)
    worker = _workers(settings, sessions)[0]

    holder = sessions()
    holder.scalar(
        select(Endpoint).where(Endpoint.id == endpoint_id).with_for_update(key_share=True)
    )
    with sessions() as db:
        assert worker._claim_due(db, T0) == []  # returns at once instead of queueing on the lock
    holder.rollback()
    holder.close()

    with sessions() as db:
        assert len(worker._claim_due(db, T0)) == 3


@pg_only
def test_a_claim_does_not_block_event_fan_out_on_postgres(pg_sessions):
    _, sessions = pg_sessions
    endpoint_id = _seed(sessions, 0, max_rps=1)

    holder = sessions()
    holder.scalar(
        select(Endpoint)
        .where(Endpoint.id == endpoint_id)
        .with_for_update(skip_locked=True, key_share=True)
    )
    with sessions() as db:
        db.execute(text("SET LOCAL lock_timeout = '500ms'"))
        db.add(Event(id="evt_new", type="payment.captured", payload={}, created_at=T0))
        db.flush()
        db.add(Delivery(event_id="evt_new", endpoint_id=endpoint_id, status="pending"))
        db.commit()  # the foreign-key check needs only a key-share lock on the endpoint
    holder.rollback()
    holder.close()


@pg_only
async def test_outcomes_for_one_endpoint_count_every_failure_on_postgres(pg_sessions):
    settings, sessions = pg_sessions
    endpoint_id = _seed(sessions, 2)
    transport = httpx.MockTransport(lambda _: httpx.Response(500))
    a, b = _workers(settings, sessions, transport=transport, clock=lambda: T0)[:2]
    with sessions() as db:
        rows = a._claim_due(db, T0)
    assert len(rows) == 2

    def fail(worker, delivery_id):
        worker._record(delivery_id, SendResult(1, False, 500, None, "HTTP 500", 1), T0)

    with ThreadPoolExecutor(2) as pool:
        list(pool.map(fail, (a, b), (rows[0].id, rows[1].id)))
    with sessions() as db:
        assert db.get(Endpoint, endpoint_id).consecutive_failures == 2
