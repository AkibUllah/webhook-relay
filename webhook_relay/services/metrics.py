from __future__ import annotations

from datetime import datetime

from sqlalchemy import case, func, select
from sqlalchemy.orm import Session

from webhook_relay.models import Delivery, DeliveryAttempt, Endpoint, Event
from webhook_relay.services.dispatcher import WorkerStats


def collect_metrics(db: Session, worker: WorkerStats, now: datetime) -> dict:
    by_status = {
        status: int(count)
        for status, count in db.execute(
            select(Delivery.status, func.count(Delivery.id)).group_by(Delivery.status)
        ).all()
    }
    attempts_total = int(db.scalar(select(func.count(DeliveryAttempt.id))) or 0)
    attempts_ok = int(
        db.scalar(
            select(
                func.coalesce(func.sum(case((DeliveryAttempt.succeeded.is_(True), 1), else_=0)), 0)
            )
        )
        or 0
    )
    due_now = int(
        db.scalar(
            select(func.count(Delivery.id)).where(
                Delivery.status == "pending", Delivery.next_attempt_at <= now
            )
        )
        or 0
    )
    return {
        "events_total": int(db.scalar(select(func.count(Event.id))) or 0),
        "endpoints_total": int(db.scalar(select(func.count(Endpoint.id))) or 0),
        "endpoints_active": int(
            db.scalar(select(func.count(Endpoint.id)).where(Endpoint.is_active.is_(True))) or 0
        ),
        "endpoints_circuit_open": int(
            db.scalar(select(func.count(Endpoint.id)).where(Endpoint.circuit_state == "open")) or 0
        ),
        "deliveries_by_status": {
            s: by_status.get(s, 0) for s in ("pending", "delivering", "succeeded", "dead")
        },
        "attempts_total": attempts_total,
        "attempts_succeeded": attempts_ok,
        "success_rate": round(attempts_ok / attempts_total, 4) if attempts_total else 0.0,
        "dead_letters": by_status.get("dead", 0),
        "due_now": due_now,
        "worker": worker.as_dict(),
    }
