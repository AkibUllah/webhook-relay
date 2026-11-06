"""One lane per endpoint: a slow receiver delays itself, not the rest of the batch."""

import asyncio

import httpx
from sqlalchemy import select

from tests.conftest import publish, register
from webhook_relay.models import Delivery
from webhook_relay.services.dispatcher import Dispatcher


class Probe:
    """Async receiver that records arrivals and how many requests overlapped."""

    def __init__(self) -> None:
        self.paths: list[str] = []
        self.open = 0
        self.peak = 0
        self.gates: dict[str, asyncio.Event] = {}

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.open += 1
        self.peak = max(self.peak, self.open)
        try:
            await asyncio.sleep(0)
            if path in self.gates:
                await asyncio.wait_for(self.gates[path].wait(), timeout=5)
            self.paths.append(path)
            return httpx.Response(200)
        finally:
            self.open -= 1


async def test_slow_endpoint_does_not_hold_up_the_rest_of_the_batch(client, auth, dispatcher):
    probe = Probe()
    probe.gates["/slow"] = asyncio.Event()
    dispatcher._transport = httpx.MockTransport(probe)
    register(client, auth, path="/slow")
    register(client, auth, path="/fast")
    publish(client, auth)

    run = asyncio.create_task(dispatcher.run_once())
    while "/fast" not in probe.paths:
        await asyncio.sleep(0)
    assert probe.paths == ["/fast"]  # delivered while /slow is still waiting on its receiver
    probe.gates["/slow"].set()
    assert await run == 2
    assert sorted(probe.paths) == ["/fast", "/slow"]


async def test_one_endpoint_is_sent_one_request_at_a_time(client, auth, dispatcher):
    probe = Probe()
    dispatcher._transport = httpx.MockTransport(probe)
    register(client, auth)
    for _ in range(4):
        publish(client, auth)

    assert await dispatcher.run_once() == 4
    assert probe.peak == 1


async def test_dispatch_concurrency_bounds_requests_across_endpoints(app, client, auth, settings):
    probe = Probe()
    dispatcher = Dispatcher(
        app.state.session_factory,
        settings.model_copy(update={"dispatch_concurrency": 2}),
        transport=httpx.MockTransport(probe),
        clock=app.state.clock,
    )
    for i in range(5):
        register(client, auth, path=f"/hooks-{i}")
    publish(client, auth)

    assert await dispatcher.run_once() == 5
    assert probe.peak == 2


async def test_a_crashing_lane_leaves_the_others_running(app, client, auth, dispatcher):
    delivered: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/broken":
            raise RuntimeError("boom")
        delivered.append(request.url.path)
        return httpx.Response(200)

    dispatcher._transport = httpx.MockTransport(handler)
    register(client, auth, path="/broken")
    register(client, auth, path="/fine")
    publish(client, auth)

    await dispatcher.run_once()
    assert delivered == ["/fine"]
    assert "boom" in dispatcher.stats.errors[-1]
    with app.state.session_factory() as db:
        statuses = sorted(db.scalars(select(Delivery.status)).all())
    assert statuses == ["delivering", "succeeded"]  # recovered when the lease expires
