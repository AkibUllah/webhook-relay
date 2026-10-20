import json
from datetime import timedelta

import httpx

from tests.conftest import T0, publish, register
from webhook_relay.core.signing import SIGNATURE_HEADER, verify_signature
from webhook_relay.services.dispatcher import (
    ATTEMPT_HEADER,
    DELIVERY_HEADER,
    EVENT_HEADER,
    EVENT_ID_HEADER,
    TIMESTAMP_HEADER,
)


async def test_successful_delivery_is_signed_and_recorded(client, auth, dispatcher, target, clock):
    ep = register(client, auth)
    event = publish(client, auth)

    assert await dispatcher.run_once() == 1
    assert len(target.requests) == 1
    request = target.requests[0]
    assert str(request.url) == "https://merchant.example.com/hooks"
    assert request.headers[EVENT_HEADER] == "payment.captured"
    assert request.headers[EVENT_ID_HEADER] == event["id"]
    assert request.headers[ATTEMPT_HEADER] == "1"
    assert int(request.headers[TIMESTAMP_HEADER]) == int(T0.timestamp())
    verify_signature(
        ep["secret"], request.content, request.headers[SIGNATURE_HEADER], now=int(T0.timestamp())
    )

    body = json.loads(request.content)
    assert body["type"] == "payment.captured"
    assert body["data"] == {"payment_id": "pay_1", "amount_minor": 150000, "currency": "BDT"}
    assert body["attempt"] == 1

    delivery = client.get(f"/v1/deliveries/{request.headers[DELIVERY_HEADER]}", headers=auth).json()
    assert delivery["status"] == "succeeded"
    assert delivery["attempt_count"] == 1
    assert delivery["delivered_at"] is not None
    assert delivery["attempts"][0]["status_code"] == 200
    assert delivery["attempts"][0]["succeeded"] is True
    assert dispatcher.stats.succeeded == 1
    assert client.get("/metrics").json()["success_rate"] == 1.0

    # Nothing left to do.
    assert await dispatcher.run_once() == 0


async def test_failures_follow_backoff_schedule_then_dead_letter(
    client, auth, dispatcher, target, clock
):
    register(client, auth)
    target.scripts["/hooks"] = [500, 503, httpx.ConnectTimeout("slow"), 502]
    event = publish(client, auth)
    delivery_id = event["deliveries"][0]["id"]

    def state():
        return client.get(f"/v1/deliveries/{delivery_id}", headers=auth).json()

    # attempt 1 fails -> retry in 10s
    assert await dispatcher.run_once() == 1
    d = state()
    assert d["status"] == "pending" and d["attempt_count"] == 1
    assert d["last_error"] == "HTTP 500"
    assert d["next_attempt_at"] == (T0 + timedelta(seconds=10)).isoformat().replace("+00:00", "Z")

    # not due yet
    clock.advance(seconds=5)
    assert await dispatcher.run_once() == 0

    # attempt 2 fails -> retry in 60s
    clock.advance(seconds=5)
    assert await dispatcher.run_once() == 1
    d = state()
    assert d["attempt_count"] == 2
    assert d["next_attempt_at"] == (clock.now + timedelta(seconds=60)).isoformat().replace(
        "+00:00", "Z"
    )

    # attempt 3 times out -> retry in 300s
    clock.advance(seconds=60)
    assert await dispatcher.run_once() == 1
    d = state()
    assert d["attempt_count"] == 3
    assert d["last_status_code"] is None
    assert d["last_error"].startswith("ConnectTimeout")
    assert d["attempts"][2]["error"].startswith("ConnectTimeout")

    # attempt 4 fails -> max_attempts (4) reached -> dead letter
    clock.advance(seconds=300)
    assert await dispatcher.run_once() == 1
    d = state()
    assert d["status"] == "dead"
    assert d["dead_lettered_at"] is not None
    assert d["next_attempt_at"] is None
    assert len(d["attempts"]) == 4

    dead = client.get("/v1/dead-letters", headers=auth)
    assert dead.headers["X-Total-Count"] == "1"
    assert dead.json()[0]["id"] == delivery_id
    assert dispatcher.stats.dead_lettered == 1
    assert client.get("/metrics").json()["dead_letters"] == 1


async def test_circuit_breaker_opens_and_resumes_after_cooldown(
    client, auth, dispatcher, target, clock
):
    ep = register(client, auth)
    target.default_status = 500
    for i in range(4):
        publish(client, auth, id=f"evt-{i}")

    # Three consecutive failures (threshold=3) open the circuit; the 4th is skipped.
    processed = await dispatcher.run_once()
    assert processed == 4
    assert len(target.requests) == 4  # all four were already claimed in the same batch
    target.requests.clear()

    endpoint = client.get(f"/v1/endpoints/{ep['id']}", headers=auth).json()
    assert endpoint["circuit_state"] == "open"
    assert endpoint["consecutive_failures"] == 4
    assert client.get("/metrics").json()["endpoints_circuit_open"] == 1

    # Retries become due, but the open circuit blocks them without touching the target.
    clock.advance(seconds=10)
    assert await dispatcher.run_once() == 0
    assert target.requests == []
    assert dispatcher.stats.skipped_circuit_open == 4
    pending = client.get("/v1/deliveries", params={"status": "pending"}, headers=auth).json()
    assert len(pending) == 4

    # After the cooldown the breaker goes half-open: probes are allowed and succeed -> closed.
    target.default_status = 200
    clock.advance(seconds=120)
    assert await dispatcher.run_once() == 4
    endpoint = client.get(f"/v1/endpoints/{ep['id']}", headers=auth).json()
    assert endpoint["circuit_state"] == "closed"
    assert endpoint["consecutive_failures"] == 0
    assert (
        client.get("/v1/deliveries", params={"status": "succeeded"}, headers=auth).headers[
            "X-Total-Count"
        ]
        == "4"
    )


async def test_half_open_failure_reopens_circuit_and_manual_reset(
    client, auth, dispatcher, target, clock
):
    ep = register(client, auth)
    target.default_status = 503
    for i in range(3):
        publish(client, auth, id=f"evt-{i}")
    await dispatcher.run_once()
    assert client.get(f"/v1/endpoints/{ep['id']}", headers=auth).json()["circuit_state"] == "open"

    clock.advance(seconds=130)  # cooldown passed -> half-open probe, still failing
    await dispatcher.run_once()
    endpoint = client.get(f"/v1/endpoints/{ep['id']}", headers=auth).json()
    assert endpoint["circuit_state"] == "open"
    assert endpoint["circuit_opened_at"].startswith("2026-10-21T09:02:10")

    reset = client.post(f"/v1/endpoints/{ep['id']}/reset-circuit", headers=auth).json()
    assert reset["circuit_state"] == "closed" and reset["consecutive_failures"] == 0


async def test_fan_out_delivers_each_endpoint_independently(client, auth, dispatcher, target):
    register(client, auth, path="/a")
    register(client, auth, path="/b")
    target.scripts["/b"] = [500]
    publish(client, auth)

    assert await dispatcher.run_once() == 2
    statuses = {
        d["endpoint_url"].rsplit("/", 1)[-1]: d["status"]
        for d in client.get("/v1/deliveries", headers=auth).json()
    }
    assert statuses == {"a": "succeeded", "b": "pending"}
    assert len(target.for_path("/a")) == 1 and len(target.for_path("/b")) == 1


async def test_replay_requeues_dead_and_succeeded_deliveries(
    client, auth, dispatcher, target, clock
):
    register(client, auth)
    target.default_status = 500
    event = publish(client, auth)
    delivery_id = event["deliveries"][0]["id"]
    for _ in range(4):
        await dispatcher.run_once()
        clock.advance(seconds=400)
    assert client.get(f"/v1/deliveries/{delivery_id}", headers=auth).json()["status"] == "dead"

    target.default_status = 200
    replayed = client.post(f"/v1/deliveries/{delivery_id}/replay", headers=auth)
    assert replayed.status_code == 202
    body = replayed.json()
    assert body["status"] == "pending" and body["attempt_count"] == 0 and body["replay_count"] == 1
    assert body["dead_lettered_at"] is None

    assert await dispatcher.run_once() == 1
    final = client.get(f"/v1/deliveries/{delivery_id}", headers=auth).json()
    assert final["status"] == "succeeded"
    assert len(final["attempts"]) == 5  # history is kept across the replay
    assert final["attempts"][-1]["attempt_number"] == 1

    # At-least-once: replaying a succeeded delivery sends it again.
    client.post(f"/v1/deliveries/{delivery_id}/replay", headers=auth)
    assert await dispatcher.run_once() == 1
    assert client.get(f"/v1/deliveries/{delivery_id}", headers=auth).json()["replay_count"] == 2
    assert client.get("/v1/dead-letters", headers=auth).json() == []


async def test_410_gone_deactivates_endpoint(client, auth, dispatcher, target):
    ep = register(client, auth)
    target.scripts["/hooks"] = [410]
    event = publish(client, auth)
    await dispatcher.run_once()
    assert client.get(f"/v1/endpoints/{ep['id']}", headers=auth).json()["is_active"] is False
    delivery = client.get(f"/v1/deliveries/{event['deliveries'][0]['id']}", headers=auth).json()
    assert delivery["status"] == "dead"


async def test_inactive_endpoints_are_not_dispatched(client, auth, dispatcher, target):
    ep = register(client, auth)
    publish(client, auth)
    client.delete(f"/v1/endpoints/{ep['id']}", headers=auth)
    assert await dispatcher.run_once() == 0
    assert target.requests == []


async def test_batch_size_limits_a_run(client, auth, settings, dispatcher, target):
    settings.batch_size = 2
    register(client, auth)
    for i in range(5):
        publish(client, auth, id=f"evt-{i}")
    assert await dispatcher.run_once() == 2
    assert await dispatcher.run_once() == 2
    assert await dispatcher.run_once() == 1
    assert await dispatcher.run_once() == 0
    assert len(target.requests) == 5


async def test_in_app_worker_lifecycle(settings, target):
    import asyncio

    from fastapi.testclient import TestClient

    from webhook_relay.main import create_app

    settings.worker_enabled = True
    settings.poll_interval_seconds = 0.05
    app = create_app(settings, transport=httpx.MockTransport(target.handler))
    with TestClient(app) as client:
        auth = {"X-API-Key": settings.api_key}
        register(client, auth)
        publish(client, auth)
        # Lifespan runs the worker in the test client's event loop; give it a few polls.
        for _ in range(50):
            if target.requests:
                break
            client.portal.call(asyncio.sleep, 0.05)
        assert len(target.requests) == 1
        assert client.get("/health").json()["worker_running"] is True
    assert app.state.dispatcher.stats.running is False
