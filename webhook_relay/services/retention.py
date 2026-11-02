"""Retention: delete old succeeded deliveries, their attempts and events left without deliveries.

Only ``succeeded`` deliveries whose ``delivered_at`` is older than ``RETENTION_DAYS`` are removed.
Dead letters stay until someone replays or inspects them, and pending or in-flight rows are
never touched. Deletion runs in batches of ``RETENTION_BATCH_SIZE`` rows, one transaction each,
so a large backlog never holds long locks. On PostgreSQL candidate rows are locked with
``SKIP LOCKED``: rows a worker or a replay is updating right now are skipped this round.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import delete, exists, select
from sqlalchemy.orm import Session, sessionmaker

from webhook_relay.config import Settings
from webhook_relay.models import Delivery, DeliveryAttempt, Event

logger = logging.getLogger(__name__)


@dataclass
class PurgeResult:
    cutoff: datetime | None
    deliveries: int = 0
    attempts: int = 0
    events: int = 0
    batches: int = 0

    @property
    def enabled(self) -> bool:
        return self.cutoff is not None

    def as_dict(self) -> dict:
        return {
            "cutoff": self.cutoff.isoformat() if self.cutoff else None,
            "deliveries": self.deliveries,
            "attempts": self.attempts,
            "events": self.events,
            "batches": self.batches,
        }


class RetentionService:
    def __init__(self, session_factory: sessionmaker[Session], settings: Settings) -> None:
        self._sessions = session_factory
        self.settings = settings

    def cutoff(self, now: datetime) -> datetime | None:
        days = self.settings.retention_days
        return None if days is None else now - timedelta(days=days)

    def purge(self, now: datetime) -> PurgeResult:
        """Delete everything past the cutoff. A no-op when ``RETENTION_DAYS`` is unset."""
        result = PurgeResult(cutoff=self.cutoff(now))
        if result.cutoff is None:
            return result
        while self._purge_delivery_batch(result.cutoff, result):
            pass
        while self._purge_event_batch(result.cutoff, result):
            pass
        if result.deliveries or result.events:
            logger.info(
                "retention purged %d deliveries, %d attempts, %d events older than %s",
                result.deliveries,
                result.attempts,
                result.events,
                result.cutoff.isoformat(),
            )
        return result

    # -- batches -----------------------------------------------------------------------------
    def _expired_deliveries(self, cutoff: datetime):
        return (Delivery.status == "succeeded", Delivery.delivered_at < cutoff)

    def _purge_delivery_batch(self, cutoff: datetime, result: PurgeResult) -> bool:
        """Delete one batch; returns True while a full batch was deleted (more may remain)."""
        batch = self.settings.retention_batch_size
        with self._sessions() as db:
            ids = list(
                db.scalars(
                    select(Delivery.id)
                    .where(*self._expired_deliveries(cutoff))
                    .order_by(Delivery.delivered_at)
                    .limit(batch)
                    .with_for_update(skip_locked=True)
                ).all()
            )
            if not ids:
                return False
            result.attempts += db.execute(
                delete(DeliveryAttempt)
                .where(DeliveryAttempt.delivery_id.in_(ids))
                .execution_options(synchronize_session=False)
            ).rowcount
            # Conditions are repeated so a row replayed since the SELECT is left alone (SQLite
            # has no row locks; on PostgreSQL the lock already guarantees it).
            deleted = db.execute(
                delete(Delivery)
                .where(Delivery.id.in_(ids), *self._expired_deliveries(cutoff))
                .execution_options(synchronize_session=False)
            ).rowcount
            db.commit()
        result.deliveries += deleted
        result.batches += 1
        return deleted == batch

    def _purge_event_batch(self, cutoff: datetime, result: PurgeResult) -> bool:
        batch = self.settings.retention_batch_size
        orphaned = (
            Event.created_at < cutoff,
            ~exists().where(Delivery.event_id == Event.id),
        )
        with self._sessions() as db:
            ids = list(
                db.scalars(
                    select(Event.id)
                    .where(*orphaned)
                    .order_by(Event.created_at)
                    .limit(batch)
                    .with_for_update(skip_locked=True, of=Event)
                ).all()
            )
            if not ids:
                return False
            deleted = db.execute(
                delete(Event)
                .where(Event.id.in_(ids), *orphaned)
                .execution_options(synchronize_session=False)
            ).rowcount
            db.commit()
        result.events += deleted
        result.batches += 1
        return deleted == batch
