"""The delivery worker.

`run_once(now)` picks due deliveries, sends them, records attempts and schedules retries.
`run_forever()` does the same on a poll interval without waiting for slow endpoints: each
endpoint's deliveries run on their own lane, and the next claim happens while a lane is still
busy. Claiming is safe to run from several worker
replicas against one PostgreSQL database: due rows are locked with `FOR UPDATE SKIP LOCKED`,
so concurrent workers receive disjoint batches, a batch is shared out evenly between the
endpoints that have work, and every claim carries a lease so work held
by a crashed worker is picked up again once the lease expires.

Everything time-related goes through `clock` so tests can drive the schedule
deterministically, and the HTTP transport is injectable.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from bisect import bisect_right
from collections import deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from math import ceil

import httpx
from sqlalchemy import ColumnElement, Select, and_, or_, select, update
from sqlalchemy.orm import Session, selectinload, sessionmaker

from webhook_relay.config import Settings
from webhook_relay.core.backoff import next_attempt_at
from webhook_relay.core.circuit import Circuit, CircuitState
from webhook_relay.core.signing import SIGNATURE_HEADER, build_signature_header
from webhook_relay.models import Delivery, DeliveryAttempt, Endpoint, Event, as_utc, utc_now
from webhook_relay.services.retention import PurgeResult, RetentionService

logger = logging.getLogger(__name__)

EVENT_HEADER = "X-Relay-Event"
EVENT_ID_HEADER = "X-Relay-Event-Id"
DELIVERY_HEADER = "X-Relay-Delivery-Id"
ATTEMPT_HEADER = "X-Relay-Attempt"
TIMESTAMP_HEADER = "X-Relay-Timestamp"


@dataclass
class WorkerStats:
    started_at: datetime | None = None
    last_run_at: datetime | None = None
    runs: int = 0
    dispatched: int = 0
    succeeded: int = 0
    failed: int = 0
    dead_lettered: int = 0
    skipped_circuit_open: int = 0
    reclaimed: int = 0
    purges: int = 0
    purged_deliveries: int = 0
    purged_attempts: int = 0
    purged_events: int = 0
    last_purge_at: datetime | None = None
    worker_id: str | None = None
    running: bool = False
    errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "running": self.running,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "last_run_at": self.last_run_at.isoformat() if self.last_run_at else None,
            "runs": self.runs,
            "dispatched": self.dispatched,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "dead_lettered": self.dead_lettered,
            "skipped_circuit_open": self.skipped_circuit_open,
            "reclaimed": self.reclaimed,
            "retention": {
                "purges": self.purges,
                "deliveries": self.purged_deliveries,
                "attempts": self.purged_attempts,
                "events": self.purged_events,
                "last_purge_at": self.last_purge_at.isoformat() if self.last_purge_at else None,
            },
            "worker_id": self.worker_id,
        }


@dataclass(frozen=True)
class SendResult:
    """What came back from one HTTP attempt, ready to be recorded."""

    attempt_number: int
    ok: bool
    status_code: int | None
    excerpt: str | None
    error: str | None
    duration_ms: int


def build_payload(event: Event, delivery: Delivery, attempt: int) -> bytes:
    body = {
        "id": event.id,
        "type": event.type,
        "created_at": as_utc(event.created_at).isoformat(),  # type: ignore[union-attr]
        "delivery_id": delivery.id,
        "attempt": attempt,
        "data": event.payload,
    }
    return json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")


def circuit_from(endpoint: Endpoint) -> Circuit:
    return Circuit(
        state=CircuitState(endpoint.circuit_state),
        consecutive_failures=endpoint.consecutive_failures,
        opened_at=as_utc(endpoint.circuit_opened_at),
    )


def circuit_to(endpoint: Endpoint, circuit: Circuit) -> None:
    endpoint.circuit_state = circuit.state.value
    endpoint.consecutive_failures = circuit.consecutive_failures
    endpoint.circuit_opened_at = circuit.opened_at


class Dispatcher:
    def __init__(
        self,
        session_factory: sessionmaker[Session],
        settings: Settings,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        clock: Callable[[], datetime] = utc_now,
        rng: Callable[[], float] = random.random,
    ) -> None:
        self._sessions = session_factory
        self.settings = settings
        self._transport = transport
        self._clock = clock
        self._rng = rng
        self.stats = WorkerStats(worker_id=settings.worker_id)
        self.retention = RetentionService(session_factory, settings)
        self._stop = asyncio.Event()
        self._send_slots = asyncio.Semaphore(settings.dispatch_concurrency)
        self._lanes: dict[str, asyncio.Task[None]] = {}
        self._last_served = ""

    # -- public ------------------------------------------------------------------
    async def run_forever(self) -> None:
        self.stats.running = True
        self.stats.started_at = self._clock()
        logger.info(
            "delivery worker started (poll every %.1fs)", self.settings.poll_interval_seconds
        )
        try:
            async with self._http_client() as client:
                try:
                    while not self._stop.is_set():
                        try:
                            processed = self._start_batch(client, self._clock)
                            await self.maybe_purge()
                        except Exception as exc:  # keep the loop alive on unexpected errors
                            logger.exception("worker iteration failed")
                            self.stats.errors = (self.stats.errors + [repr(exc)])[-10:]
                            processed = 0
                        await self._pause(processed)
                    await self._wait_for_lanes()
                except asyncio.CancelledError:
                    lanes = list(self._lanes.values())
                    for lane in lanes:
                        lane.cancel()
                    await asyncio.gather(*lanes, return_exceptions=True)
                    raise
        finally:
            self.stats.running = False
            logger.info("delivery worker stopped")

    def stop(self) -> None:
        self._stop.set()

    async def run_once(self, now: datetime | None = None) -> int:
        """Process one batch of due deliveries. Returns the number dispatched."""
        clock = self._clock if now is None else (lambda: now)
        async with self._http_client() as client:
            processed = self._start_batch(client, clock)
            await self._wait_for_lanes()
        return processed

    async def maybe_purge(self, now: datetime | None = None) -> PurgeResult | None:
        """Apply retention if it is enabled and RETENTION_INTERVAL_SECONDS have passed since the
        last purge. Runs in a thread: a large first purge must not stall deliveries or the API."""
        if self.settings.retention_days is None:
            return None
        now = now or self._clock()
        last = self.stats.last_purge_at
        if last and now - last < timedelta(seconds=self.settings.retention_interval_seconds):
            return None
        self.stats.last_purge_at = now
        result = await asyncio.to_thread(self.retention.purge, now)
        self.stats.purges += 1
        self.stats.purged_deliveries += result.deliveries
        self.stats.purged_attempts += result.attempts
        self.stats.purged_events += result.events
        return result

    # -- internals ---------------------------------------------------------------
    def _http_client(self) -> httpx.AsyncClient:
        slots = self.settings.dispatch_concurrency
        return httpx.AsyncClient(
            timeout=self.settings.http_timeout_seconds,
            transport=self._transport,
            limits=httpx.Limits(max_connections=slots, max_keepalive_connections=slots),
        )

    def _start_batch(self, client: httpx.AsyncClient, clock: Callable[[], datetime]) -> int:
        """Claim due work and start a lane per endpoint. Endpoints with a lane still running are
        left out, which keeps sends to one endpoint in order within this worker while everyone
        else carries on. Returns the number of deliveries claimed."""
        now = clock()
        self.stats.runs += 1
        self.stats.last_run_at = now
        with self._sessions() as db:
            due = self._claim_due(db, now, busy=frozenset(self._lanes))
        lanes: dict[str, list[Delivery]] = {}
        for delivery in due:
            lanes.setdefault(delivery.endpoint_id, []).append(delivery)
        for endpoint_id, deliveries in lanes.items():
            lane = asyncio.create_task(self._run_lane(client, deliveries, clock))
            self._lanes[endpoint_id] = lane
            lane.add_done_callback(lambda _, key=endpoint_id: self._lanes.pop(key, None))
        return len(due)

    async def _wait_for_lanes(self) -> None:
        await asyncio.gather(*self._lanes.values())

    async def _pause(self, processed: int) -> None:
        """Wait before the next claim: until every lane is done, a stop is requested or the poll
        interval passes, whichever comes first. The poll interval is therefore the longest a
        slow lane can keep this worker from picking up other endpoints' work."""
        lanes = set(self._lanes.values())
        if processed and not lanes:
            return
        waiters = {asyncio.ensure_future(self._stop.wait())}
        if lanes:
            waiters.add(asyncio.ensure_future(asyncio.wait(lanes)))
        await asyncio.wait(
            waiters,
            timeout=self.settings.poll_interval_seconds,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for waiter in waiters:
            waiter.cancel()

    def _release(self, delivery_ids: list[str]) -> None:
        """Hand back claimed deliveries that were never sent. No attempt happened, so none is
        recorded and they are immediately due for the next worker."""
        with self._sessions() as db:
            db.execute(
                update(Delivery)
                .where(
                    Delivery.id.in_(delivery_ids),
                    Delivery.status == "delivering",
                    Delivery.claimed_by == self.settings.worker_id,
                )
                .values(status="pending", claimed_by=None, claimed_until=None)
            )
            db.commit()

    async def _run_lane(
        self,
        client: httpx.AsyncClient,
        deliveries: Sequence[Delivery],
        clock: Callable[[], datetime],
    ) -> None:
        """Send one endpoint's deliveries in order. Lanes run side by side, so an endpoint that
        answers slowly only ever delays itself. An unexpected error ends this lane alone; the
        rows it still held become due again when their lease runs out."""
        queue = deque(deliveries)
        try:
            while queue and not self._stop.is_set():
                delivery = queue.popleft()
                async with self._send_slots:
                    moment = clock()
                    result = await self._send(client, delivery, moment)
                self._record(delivery.id, result, moment)
            if queue:  # shutting down: do not make the next worker wait out the lease
                self._release([d.id for d in queue])
        except Exception as exc:
            logger.exception("lane for endpoint %s failed", deliveries[0].endpoint_id)
            self.stats.errors = (self.stats.errors + [repr(exc)])[-10:]

    @staticmethod
    def _due_filter(now: datetime) -> ColumnElement[bool]:
        due = and_(Delivery.status == "pending", Delivery.next_attempt_at <= now)
        abandoned = and_(Delivery.status == "delivering", Delivery.claimed_until < now)
        return and_(or_(due, abandoned), Endpoint.is_active.is_(True))

    def claim_statement(
        self, now: datetime, endpoint_id: str | None = None, limit: int | None = None
    ) -> Select[tuple[Delivery]]:
        """Due work: pending deliveries whose time has come, plus deliveries still marked
        `delivering` whose claim lease has expired (their worker died mid-batch).

        `FOR UPDATE OF deliveries SKIP LOCKED` makes concurrent claimers on PostgreSQL skip
        rows another transaction has already locked instead of blocking on them or claiming
        them twice. SQLite has no row locks (it serialises writers), so SQLAlchemy omits the
        clause there and the status/lease check alone keeps a single process consistent.
        """
        stmt = (
            select(Delivery)
            .join(Endpoint, Delivery.endpoint_id == Endpoint.id)
            .options(selectinload(Delivery.event), selectinload(Delivery.endpoint))
            .where(self._due_filter(now))
            .order_by(Delivery.next_attempt_at)
            .limit(limit or self.settings.batch_size)
            .with_for_update(skip_locked=True, of=Delivery)
        )
        return stmt if endpoint_id is None else stmt.where(Delivery.endpoint_id == endpoint_id)

    def _endpoints_with_due_work(
        self, db: Session, now: datetime, busy: frozenset[str]
    ) -> list[str]:
        """Endpoints that have something to send, in id order starting after the one this worker
        reached last time, so with more endpoints than batch slots everyone gets a turn."""
        endpoints = list(
            db.scalars(
                select(Delivery.endpoint_id)
                .join(Endpoint, Delivery.endpoint_id == Endpoint.id)
                .where(self._due_filter(now), Delivery.endpoint_id.not_in(busy))
                .group_by(Delivery.endpoint_id)
                .order_by(Delivery.endpoint_id)
            ).all()
        )
        start = bisect_right(endpoints, self._last_served)
        return endpoints[start:] + endpoints[:start]

    def _claim_due(
        self, db: Session, now: datetime, busy: frozenset[str] = frozenset()
    ) -> list[Delivery]:
        """Fill a batch endpoint by endpoint, so a backlog on one endpoint cannot crowd out the
        others. Each takes an even share of what is left and whatever it does not use rolls
        over to the next, so an endpoint alone in the queue still gets the whole batch."""
        endpoints = self._endpoints_with_due_work(db, now, busy)
        budget = self.settings.batch_size
        claimed: list[Delivery] = []
        for position, endpoint_id in enumerate(endpoints):
            if budget == 0:
                break
            self._last_served = endpoint_id
            share = ceil(budget / (len(endpoints) - position))
            taken = self._claim_endpoint(db, endpoint_id, share, now)
            budget -= len(taken)
            claimed.extend(taken)
        return claimed

    def _claim_endpoint(
        self, db: Session, endpoint_id: str, share: int, now: datetime
    ) -> list[Delivery]:
        due = list(db.scalars(self.claim_statement(now, endpoint_id, share)).all())
        lease = timedelta(seconds=self.settings.lease_seconds)
        cooldown = timedelta(seconds=self.settings.circuit_cooldown_seconds)
        claimed: list[Delivery] = []
        for delivery in due:
            circuit = circuit_from(delivery.endpoint)
            allowed, circuit = circuit.allows_request(cooldown, now)
            circuit_to(delivery.endpoint, circuit)
            if not allowed:
                # Leave it pending; push the check past the cooldown so we do not spin.
                retry_after = circuit.retry_after(cooldown, now) or 0.0
                delivery.next_attempt_at = now + timedelta(seconds=retry_after)
                self.stats.skipped_circuit_open += 1
                continue
            if delivery.status == "delivering":
                # The previous owner's lease ran out before it recorded an outcome. The send
                # may or may not have happened, so this is a redelivery (at-least-once).
                self.stats.reclaimed += 1
                logger.warning(
                    "reclaiming %s from %s (lease expired %s)",
                    delivery.id,
                    delivery.claimed_by,
                    as_utc(delivery.claimed_until).isoformat() if delivery.claimed_until else "?",
                )
            delivery.status = "delivering"
            delivery.claimed_by = self.settings.worker_id
            delivery.claimed_until = now + lease
            claimed.append(delivery)
        db.commit()
        return claimed

    async def _send(
        self, client: httpx.AsyncClient, delivery: Delivery, now: datetime
    ) -> SendResult:
        """One HTTP attempt. Touches no database session, so no connection is held while a
        receiver takes its time to answer."""
        endpoint = delivery.endpoint
        attempt_number = delivery.attempt_count + 1
        body = build_payload(delivery.event, delivery, attempt_number)
        timestamp = int(now.timestamp())
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "webhook-relay/0.1",
            EVENT_HEADER: delivery.event.type,
            EVENT_ID_HEADER: delivery.event.id,
            DELIVERY_HEADER: delivery.id,
            ATTEMPT_HEADER: str(attempt_number),
            TIMESTAMP_HEADER: str(timestamp),
            SIGNATURE_HEADER: build_signature_header(endpoint.secret, body, timestamp),
        }
        started = time.perf_counter()
        status_code: int | None = None
        excerpt: str | None = None
        error: str | None = None
        try:
            response = await client.post(endpoint.url, content=body, headers=headers)
            status_code = response.status_code
            excerpt = response.text[:500]
            ok = 200 <= response.status_code < 300
            if not ok:
                error = f"HTTP {response.status_code}"
        except httpx.HTTPError as exc:
            ok = False
            error = f"{type(exc).__name__}: {exc}"[:500]
        duration_ms = int((time.perf_counter() - started) * 1000)
        return SendResult(attempt_number, ok, status_code, excerpt, error, duration_ms)

    def _record(self, delivery_id: str, result: SendResult, now: datetime) -> None:
        """Store an attempt and its consequences in a short transaction of its own. The endpoint
        row is locked (without blocking event inserts) so concurrent outcomes for one endpoint
        update the breaker one at a time instead of overwriting each other."""
        with self._sessions() as db:
            delivery = db.get(Delivery, delivery_id)
            if delivery is None:
                return
            endpoint = db.scalar(
                select(Endpoint)
                .where(Endpoint.id == delivery.endpoint_id)
                .with_for_update(key_share=True)
            )
            self._apply_result(db, delivery, endpoint, result, now)
            db.commit()

    def _apply_result(
        self,
        db: Session,
        delivery: Delivery,
        endpoint: Endpoint,
        result: SendResult,
        now: datetime,
    ) -> None:
        attempt_number, ok = result.attempt_number, result.ok
        status_code, error = result.status_code, result.error
        db.add(
            DeliveryAttempt(
                delivery_id=delivery.id,
                attempt_number=attempt_number,
                status_code=status_code,
                response_excerpt=result.excerpt,
                error=error,
                duration_ms=result.duration_ms,
                succeeded=ok,
                created_at=now,
            )
        )
        delivery.attempt_count = attempt_number
        delivery.claimed_by = None
        delivery.claimed_until = None
        delivery.last_status_code = status_code
        delivery.last_error = error
        self.stats.dispatched += 1

        circuit = circuit_from(endpoint)
        if ok:
            delivery.status = "succeeded"
            delivery.delivered_at = now
            delivery.next_attempt_at = None
            circuit_to(endpoint, circuit.record_success())
            self.stats.succeeded += 1
            logger.info(
                "delivered %s -> %s (%s, %sms)",
                delivery.id,
                endpoint.url,
                status_code,
                result.duration_ms,
            )
            return

        self.stats.failed += 1
        circuit_to(endpoint, circuit.record_failure(self.settings.circuit_failure_threshold, now))
        if status_code == 410:
            # Endpoint says it is gone: stop hammering it.
            endpoint.is_active = False
            delivery.status = "dead"
            delivery.dead_lettered_at = now
            delivery.next_attempt_at = None
            self.stats.dead_lettered += 1
            logger.warning("endpoint %s returned 410; deactivated", endpoint.id)
            return
        if attempt_number >= self.settings.max_attempts:
            delivery.status = "dead"
            delivery.dead_lettered_at = now
            delivery.next_attempt_at = None
            self.stats.dead_lettered += 1
            logger.warning(
                "dead-lettered %s after %s attempts (%s)", delivery.id, attempt_number, error
            )
            return
        delivery.status = "pending"
        delivery.next_attempt_at = next_attempt_at(
            now, attempt_number, self.settings.schedule, self.settings.jitter_fraction, self._rng
        )
        logger.info(
            "retry %s scheduled attempt=%s at=%s (%s)",
            delivery.id,
            attempt_number + 1,
            delivery.next_attempt_at.isoformat(),
            error,
        )
