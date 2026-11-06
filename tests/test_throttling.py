"""Per-endpoint limits: rate and in-flight caps hold, and waiting never costs a retry attempt."""

import asyncio
from datetime import timedelta

import httpx
from sqlalchemy import func, select

from tests.conftest import T0, publish, register
from tests.test_lanes import Probe
from webhook_relay.models import Delivery, DeliveryAttempt, Endpoint
from webhook_relay.services.dispatcher import ATTEMPT_HEADER, Dispatcher


def _rows(app, endpoint_id=None) -> list[Delivery]:
    with app.state.session_factory() as db:
        stmt = select(Delivery).order_by(Delivery.created_at)
        if endpoint_id:
            stmt = stmt.where(Delivery.endpoint_id == endpoint_id)
        return list(db.scalars(stmt).all())


def _statuses(app) -> dict[str, int]:
    with app.state.session_factory() as db:
        rows = db.execute(select(Delivery.status, func.count()).group_by(Delivery.status))
        return {status: count for status, count in rows}


def _publish(client, auth, clock, n: int) -> None:
    for _ in range(n):
        publish(client, auth)
        clock.advance(milliseconds=1)


def _claim(app, worker, now) -> list[Delivery]:
    with app.state.session_factory() as db:
        return worker._claim_due(db, now)


# ------------------------------------------------------------------------------- no limits
async def test_endpoints_without_limits_are_untouched(app, client, auth, dispatcher, clock):
    ep = register(client, auth)
    _publish(client, auth, clock, 5)

    assert await dispatcher.run_once() == 5
    assert dispatcher.stats.skipped_rate_limited == 0
    assert dispatcher.stats.skipped_in_flight == 0
    with app.state.session_factory() as db:
        endpoint = db.get(Endpoint, ep["id"])
        assert endpoint.rate_tokens is None and endpoint.rate_updated_at is None


# --------------------------------------------------------------------------- max_in_flight
def test_a_batch_never_claims_more_than_max_in_flight(app, client, auth, dispatcher, clock):
    register(client, auth, max_in_flight=2)
    _publish(client, auth, clock, 5)

    claimed = _claim(app, dispatcher, clock())
    assert len(claimed) == 2
    assert _statuses(app) == {"delivering": 2, "pending": 3}


def test_claims_held_elsewhere_use_up_the_cap(app, client, auth, dispatcher, clock, settings):
    register(client, auth, max_in_flight=3)
    _publish(client, auth, clock, 6)
    other = Dispatcher(
        app.state.session_factory, settings.model_copy(update={"worker_id": "other"})
    )

    assert len(_claim(app, other, clock())) == 3
    assert _claim(app, dispatcher, clock()) == []
    assert dispatcher.stats.skipped_in_flight == 1
    waiting = [d for d in _rows(app) if d.status == "pending"]
    assert all(d.attempt_count == 0 for d in waiting)


async def test_slots_free_up_as_requests_finish(client, auth, dispatcher, target, clock):
    register(client, auth, max_in_flight=2)
    _publish(client, auth, clock, 5)

    assert [await dispatcher.run_once() for _ in range(4)] == [2, 2, 1, 0]
    assert len(target.requests) == 5


async def test_a_failed_request_frees_its_slot(app, client, auth, dispatcher, target, clock):
    register(client, auth, max_in_flight=1)
    target.default_status = 500
    _publish(client, auth, clock, 2)

    assert await dispatcher.run_once() == 1
    assert await dispatcher.run_once() == 1  # the other row; the first is waiting on its backoff
    assert sorted(d.attempt_count for d in _rows(app)) == [1, 1]


def test_an_expired_lease_does_not_hold_a_slot(app, client, auth, clock, settings):
    register(client, auth, max_in_flight=1)
    _publish(client, auth, clock, 2)
    settings.claim_lease_seconds = 60
    dead = Dispatcher(app.state.session_factory, settings.model_copy(update={"worker_id": "dead"}))
    alive = Dispatcher(app.state.session_factory, settings.model_copy(update={"worker_id": "ok"}))
    held = _claim(app, dead, clock())[0].id

    assert _claim(app, alive, clock()) == []
    later = clock.advance(seconds=61)
    claimed = _claim(app, alive, later)
    assert [d.id for d in claimed] == [held]
    assert claimed[0].claimed_by == "ok"
    assert alive.stats.reclaimed == 1


async def test_a_lane_uses_the_concurrency_the_endpoint_allows(client, auth, dispatcher, clock):
    probe = Probe()
    dispatcher._transport = httpx.MockTransport(probe)
    register(client, auth, max_in_flight=3)
    _publish(client, auth, clock, 7)

    assert await dispatcher.run_once() == 3
    assert probe.peak == 3


# --------------------------------------------------------------------------------- max_rps
async def test_rate_limited_rows_wait_at_the_limit_without_using_an_attempt(
    app, client, auth, dispatcher, target, clock
):
    ep = register(client, auth, max_rps=2)
    _publish(client, auth, clock, 5)

    assert await dispatcher.run_once() == 2
    assert len(target.requests) == 2
    waiting = [d for d in _rows(app) if d.status == "pending"]
    assert len(waiting) == 3
    assert all(d.attempt_count == 0 and d.last_error is None for d in waiting)
    assert dispatcher.stats.skipped_rate_limited == 3
    assert dispatcher.stats.failed == 0
    with app.state.session_factory() as db:
        assert db.scalar(select(func.count(DeliveryAttempt.id))) == 2
    # Each row comes due in its own slot.
    gaps = [d.next_attempt_at.replace(tzinfo=T0.tzinfo) - clock() for d in waiting]
    assert gaps == [timedelta(seconds=s) for s in (0.5, 1.0, 1.5)]

    clock.advance(seconds=0.5)
    assert await dispatcher.run_once() == 1
    clock.advance(seconds=0.5)
    assert await dispatcher.run_once() == 1
    clock.advance(seconds=0.5)
    assert await dispatcher.run_once() == 1
    assert _statuses(app) == {"succeeded": 5}
    assert all(d.attempt_count == 1 for d in _rows(app, ep["id"]))
    assert {r.headers[ATTEMPT_HEADER] for r in target.requests} == {"1"}


async def test_waiting_never_dead_letters_a_delivery(app, client, auth, dispatcher, clock):
    register(client, auth, max_rps=0.1)
    _publish(client, auth, clock, 3)

    assert await dispatcher.run_once() == 1
    for _ in range(50):  # far more rounds than MAX_ATTEMPTS
        clock.advance(milliseconds=50)
        assert await dispatcher.run_once() == 0
    assert _statuses(app) == {"pending": 2, "succeeded": 1}
    assert dispatcher.stats.dead_lettered == 0


async def test_sub_one_rate_spaces_requests_out(client, auth, dispatcher, target, clock):
    register(client, auth, max_rps=0.5)
    _publish(client, auth, clock, 3)

    assert await dispatcher.run_once() == 1
    clock.advance(seconds=1)
    assert await dispatcher.run_once() == 0
    clock.advance(seconds=1)
    assert await dispatcher.run_once() == 1


async def test_retries_pay_for_a_token_too(app, client, auth, dispatcher, target, clock):
    register(client, auth, max_rps=1)
    target.scripts["/hooks"] = [500, 200, 200]
    _publish(client, auth, clock, 2)

    assert await dispatcher.run_once() == 1  # first row fails, retry in 10s
    clock.advance(seconds=10)
    # Both rows are due and one token is ready: creation order decides, the retry goes first.
    assert await dispatcher.run_once() == 1
    rows = _rows(app)
    assert rows[0].status == "succeeded" and rows[0].attempt_count == 2
    assert rows[1].status == "pending" and rows[1].attempt_count == 0


async def test_a_held_row_keeps_its_place_ahead_of_a_younger_backlog(
    app, client, auth, settings, target, clock
):
    register(client, auth, max_rps=1)
    _publish(client, auth, clock, 6)
    worker = Dispatcher(
        app.state.session_factory,
        settings.model_copy(update={"batch_size": 3}),
        transport=httpx.MockTransport(target.handler),
        clock=clock,
    )
    created = [d.id for d in _rows(app)]

    for _ in range(6):
        await worker.run_once()
        clock.advance(seconds=1)
    sent = [r.headers["X-Relay-Delivery-Id"] for r in target.requests]
    assert sent == created


async def test_the_bucket_is_shared_by_every_worker(app, client, auth, clock, settings, target):
    ep = register(client, auth, max_rps=3)
    _publish(client, auth, clock, 8)
    workers = [
        Dispatcher(
            app.state.session_factory,
            settings.model_copy(update={"worker_id": f"w{i}"}),
            transport=httpx.MockTransport(target.handler),
            clock=clock,
        )
        for i in range(3)
    ]
    assert [await w.run_once() for w in workers] == [3, 0, 0]
    with app.state.session_factory() as db:
        assert db.get(Endpoint, ep["id"]).rate_tokens == 0

    clock.advance(seconds=1)
    assert [await w.run_once() for w in workers] == [3, 0, 0]


def test_a_reclaimed_row_over_the_limit_goes_back_to_pending(app, client, auth, clock, settings):
    ep = register(client, auth)
    _publish(client, auth, clock, 1)
    settings.claim_lease_seconds = 60
    dead = Dispatcher(app.state.session_factory, settings.model_copy(update={"worker_id": "dead"}))
    _claim(app, dead, clock())

    later = clock.advance(seconds=61)
    with app.state.session_factory() as db:  # the limit is set and spent while the row is stuck
        endpoint = db.get(Endpoint, ep["id"])
        endpoint.max_rps, endpoint.rate_tokens, endpoint.rate_updated_at = 1, 0, later
        db.commit()
    assert _claim(app, dead, later) == []
    row = _rows(app)[0]
    assert row.status == "pending" and row.claimed_by is None and row.claimed_until is None


# --------------------------------------------------------------------- with everything else
async def test_an_open_circuit_spends_no_tokens(app, client, auth, dispatcher, target, clock):
    ep = register(client, auth, max_rps=5)
    target.default_status = 500
    _publish(client, auth, clock, 3)
    assert await dispatcher.run_once() == 3  # three failures open the breaker
    assert client.get(f"/v1/endpoints/{ep['id']}", headers=auth).json()["circuit_state"] == "open"

    clock.advance(seconds=10)
    with app.state.session_factory() as db:
        tokens = db.get(Endpoint, ep["id"]).rate_tokens
    assert await dispatcher.run_once() == 0
    assert dispatcher.stats.skipped_circuit_open == 3
    assert dispatcher.stats.skipped_rate_limited == 0
    with app.state.session_factory() as db:
        assert db.get(Endpoint, ep["id"]).rate_tokens > tokens


async def test_unused_share_of_a_throttled_endpoint_goes_to_the_others(
    app, client, auth, settings, target, clock
):
    register(client, auth, path="/throttled", max_in_flight=1)
    other = register(client, auth, path="/other")
    _publish(client, auth, clock, 8)
    worker = Dispatcher(app.state.session_factory, settings.model_copy(update={"batch_size": 4}))
    worker._last_served = other["id"]  # the capped endpoint may or may not come first

    claimed = _claim(app, worker, clock())
    capped = [d for d in claimed if d.endpoint_id != other["id"]]
    assert len(capped) == 1
    assert len(claimed) - len(capped) >= 2


async def test_lowering_a_limit_applies_to_the_next_claim(client, auth, dispatcher, clock):
    ep = register(client, auth, max_in_flight=4)
    _publish(client, auth, clock, 6)
    r = client.patch(f"/v1/endpoints/{ep['id']}", json={"max_in_flight": 1}, headers=auth)
    assert r.json()["max_in_flight"] == 1
    assert await dispatcher.run_once() == 1

    r = client.patch(f"/v1/endpoints/{ep['id']}", json={"max_in_flight": None}, headers=auth)
    assert await dispatcher.run_once() == 5


def test_metrics_report_the_skips(client, auth, dispatcher, app, clock):
    register(client, auth, max_rps=1)
    _publish(client, auth, clock, 3)
    _claim(app, dispatcher, clock())
    worker = client.get("/metrics").json()["worker"]
    assert worker["skipped_rate_limited"] == 2
    assert worker["skipped_in_flight"] == 0


async def test_worker_wakes_for_a_rate_limit_sooner_than_the_poll_interval(
    app, client, auth, settings
):
    probe = Probe()
    register(client, auth, max_rps=10)
    for _ in range(12):
        publish(client, auth)
    worker = Dispatcher(
        app.state.session_factory,
        settings.model_copy(update={"poll_interval_seconds": 60}),
        transport=httpx.MockTransport(probe),
    )

    loop = asyncio.create_task(worker.run_forever())

    async def all_sent() -> None:
        while len(probe.paths) < 12:
            await asyncio.sleep(0.01)

    await asyncio.wait_for(all_sent(), timeout=5)
    worker.stop()
    await asyncio.wait_for(loop, timeout=2)


def test_dashboard_and_metrics_show_endpoint_limits(client, auth):
    register(client, auth, path="/both", max_rps=2.5, max_in_flight=4)
    register(client, auth, path="/rate", max_rps=1)
    register(client, auth, path="/none")

    assert client.get("/metrics").json()["endpoints_limited"] == 2
    page = client.get("/dashboard").text
    assert "2.5/s · 4 in flight" in page
    assert "Held by rate limit" in page
