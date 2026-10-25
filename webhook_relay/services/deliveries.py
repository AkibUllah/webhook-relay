from __future__ import annotations

from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from webhook_relay.models import Delivery, utc_now
from webhook_relay.services.errors import NotFoundError


class DeliveryService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def _base(self):
        return select(Delivery).options(
            selectinload(Delivery.attempts),
            selectinload(Delivery.event),
            selectinload(Delivery.endpoint),
        )

    def get(self, delivery_id: str) -> Delivery:
        delivery = self.db.scalar(self._base().where(Delivery.id == delivery_id))
        if delivery is None:
            raise NotFoundError(f"delivery {delivery_id} not found")
        return delivery

    def list(
        self,
        *,
        status: str | None = None,
        endpoint_id: str | None = None,
        event_id: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[Delivery], int]:
        stmt = self._base()
        count = select(func.count(Delivery.id))
        if status:
            stmt, count = (
                stmt.where(Delivery.status == status),
                count.where(Delivery.status == status),
            )
        if endpoint_id:
            stmt = stmt.where(Delivery.endpoint_id == endpoint_id)
            count = count.where(Delivery.endpoint_id == endpoint_id)
        if event_id:
            stmt = stmt.where(Delivery.event_id == event_id)
            count = count.where(Delivery.event_id == event_id)
        stmt = stmt.order_by(Delivery.created_at.desc()).limit(limit).offset(offset)
        return list(self.db.scalars(stmt).all()), int(self.db.scalar(count) or 0)

    def dead_letters(self, limit: int, offset: int) -> tuple[list[Delivery], int]:
        return self.list(status="dead", limit=limit, offset=offset)

    def replay(self, delivery_id: str, now: datetime | None = None) -> Delivery:
        """Re-queue a delivery immediately. Works for dead, succeeded or pending deliveries."""
        delivery = self.get(delivery_id)
        delivery.status = "pending"
        delivery.attempt_count = 0
        delivery.replay_count += 1
        delivery.next_attempt_at = now or utc_now()
        delivery.dead_lettered_at = None
        delivery.last_error = None
        delivery.claimed_by = None
        delivery.claimed_until = None
        self.db.flush()
        return delivery

    def recent(self, limit: int = 25) -> list[Delivery]:
        return list(
            self.db.scalars(self._base().order_by(Delivery.updated_at.desc()).limit(limit)).all()
        )
