"""The delivery worker.

`run_once(now)` picks due deliveries, sends them, records attempts and schedules retries.
`run_forever()` loops with a poll interval. Claiming is safe to run from several worker
replicas against one PostgreSQL database: due rows are locked with `FOR UPDATE SKIP LOCKED`,
so concurrent workers receive disjoint batches, and every claim carries a lease so work held
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
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import httpx
from sqlalchemy import Select, and_, or_, select
from sqlalchemy.orm import Session, selectinload, sessionmaker

from webhook_relay.config import Settings
from webhook_relay.core.backoff import next_attempt_at
from webhook_relay.core.circuit import Circuit, CircuitState
from webhook_relay.core.signing import SIGNATURE_HEADER, build_signature_header
from webhook_relay.models import Delivery, Endpoint, Event, as_utc, utc_now

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
            "worker_id": self.worker_id,
        }


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
        self._stop = asyncio.Event()

    # -- public ------------------------------------------------------------------
    async def run_forever(self) -> None:
        self.stats.running = True
        self.stats.started_at = self._clock()
        logger.info(
            "delivery worker started (poll every %.1fs)", self.settings.poll_interval_seconds
        )
        try:
            while not self._stop.is_set():
                try:
                    processed = await self.run_once()
                except Exception as exc:  # keep the loop alive on unexpected errors
                    logger.exception("worker iteration failed")
                    self.stats.errors = (self.stats.errors + [repr(exc)])[-10:]
                    processed = 0
                if processed == 0:
                    try:
                        await asyncio.wait_for(
                            self._stop.wait(), timeout=self.settings.poll_interval_seconds
                        )
                    except asyncio.TimeoutError:
                        pass
        finally:
            self.stats.running = False
            logger.info("delivery worker stopped")

    def stop(self) -> None:
        self._stop.set()

    async def run_once(self, now: datetime | None = None) -> int:
        """Process one batch of due deliveries. Returns the number dispatched."""
        now = now or self._clock()
        self.stats.runs += 1
        self.stats.last_run_at = now
        with self._sessions() as db:
            due = self._claim_due(db, now)
            if not due:
                return 0
            async with httpx.AsyncClient(
                timeout=self.settings.http_timeout_seconds, transport=self._transport
            ) as client:
                for delivery in due:
                    await self._dispatch(db, client, delivery, now)
                    db.commit()
            return len(due)

    # -- internals ---------------------------------------------------------------
    def claim_statement(self, now: datetime) -> Select[tuple[Delivery]]:
        """Due work: pending deliveries whose time has come, plus deliveries still marked
        `delivering` whose claim lease has expired (their worker died mid-batch).

        `FOR UPDATE OF deliveries SKIP LOCKED` makes concurrent claimers on PostgreSQL skip
        rows another transaction has already locked instead of blocking on them or claiming
        them twice. SQLite has no row locks (it serialises writers), so SQLAlchemy omits the
        clause there and the status/lease check alone keeps a single process consistent.
        """
        due = and_(Delivery.status == "pending", Delivery.next_attempt_at <= now)
        abandoned = and_(Delivery.status == "delivering", Delivery.claimed_until < now)
        return (
            select(Delivery)
            .join(Endpoint, Delivery.endpoint_id == Endpoint.id)
            .options(selectinload(Delivery.event), selectinload(Delivery.endpoint))
            .where(or_(due, abandoned), Endpoint.is_active.is_(True))
            .order_by(Delivery.next_attempt_at)
            .limit(self.settings.batch_size)
            .with_for_update(skip_locked=True, of=Delivery)
        )

    def _claim_due(self, db: Session, now: datetime) -> list[Delivery]:
        due = list(db.scalars(self.claim_statement(now)).all())
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

    async def _dispatch(
        self, db: Session, client: httpx.AsyncClient, delivery: Delivery, now: datetime
    ) -> None:
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

        from webhook_relay.models import DeliveryAttempt

        db.add(
            DeliveryAttempt(
                delivery_id=delivery.id,
                attempt_number=attempt_number,
                status_code=status_code,
                response_excerpt=excerpt,
                error=error,
                duration_ms=duration_ms,
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
                "delivered %s -> %s (%s, %sms)", delivery.id, endpoint.url, status_code, duration_ms
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
