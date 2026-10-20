"""Publishing: persist the event once, fan out one Delivery per matching active endpoint."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from webhook_relay.core.matching import matches_any
from webhook_relay.models import Delivery, Endpoint, Event, utc_now
from webhook_relay.schemas import EventPublish
from webhook_relay.services.errors import NotFoundError

logger = logging.getLogger(__name__)


class EventService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def publish(self, data: EventPublish, now: datetime | None = None) -> tuple[Event, bool]:
        """Return (event, duplicate). Duplicate ids are acknowledged without new deliveries."""
        event_id = data.id or f"evt_{uuid.uuid4().hex[:20]}"
        existing = self.get(event_id, missing_ok=True)
        if existing is not None:
            logger.info("duplicate event id=%s ignored", event_id)
            return existing, True

        now = now or utc_now()
        event = Event(
            id=event_id, type=data.type, payload=data.data, source=data.source, created_at=now
        )
        self.db.add(event)
        self.db.flush()

        endpoints = self.db.scalars(select(Endpoint).where(Endpoint.is_active.is_(True))).all()
        fanout = 0
        for endpoint in endpoints:
            if not matches_any(event.type, endpoint.event_types or []):
                continue
            self.db.add(
                Delivery(
                    event_id=event.id,
                    endpoint_id=endpoint.id,
                    status="pending",
                    next_attempt_at=now,
                    created_at=now,
                )
            )
            fanout += 1
        self.db.flush()
        logger.info("event published id=%s type=%s deliveries=%s", event.id, event.type, fanout)
        return self.get(event.id), False  # type: ignore[return-value]

    def get(self, event_id: str, missing_ok: bool = False) -> Event | None:
        event = self.db.scalar(
            select(Event).options(selectinload(Event.deliveries)).where(Event.id == event_id)
        )
        if event is None and not missing_ok:
            raise NotFoundError(f"event {event_id} not found")
        return event

    def list(self, event_type: str | None, limit: int, offset: int) -> tuple[list[Event], int]:
        stmt = select(Event).options(selectinload(Event.deliveries))
        count = select(func.count(Event.id))
        if event_type:
            stmt = stmt.where(Event.type == event_type)
            count = count.where(Event.type == event_type)
        stmt = stmt.order_by(Event.created_at.desc()).limit(limit).offset(offset)
        return list(self.db.scalars(stmt).all()), int(self.db.scalar(count) or 0)
