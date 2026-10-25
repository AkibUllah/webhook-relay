from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, HttpUrl, field_validator

from webhook_relay.core.matching import is_valid_event_type, is_valid_filter
from webhook_relay.models import Delivery, DeliveryAttempt, Endpoint, Event, as_utc


class ErrorBody(BaseModel):
    code: str
    message: str


class ErrorResponse(BaseModel):
    error: ErrorBody


# -- endpoints -----------------------------------------------------------------------
def clean_filters(value: list[str] | None) -> list[str] | None:
    if value is None:
        return None
    cleaned = sorted({v.strip().lower() for v in value if v.strip()})
    for pattern in cleaned:
        if not is_valid_filter(pattern):
            raise ValueError(f"invalid event filter {pattern!r}")
    return cleaned


class EndpointCreate(BaseModel):
    url: HttpUrl
    secret: str | None = Field(default=None, min_length=16, max_length=128)
    description: str | None = Field(default=None, max_length=500)
    event_types: list[str] = Field(default_factory=list, max_length=50)
    is_active: bool = True

    @field_validator("event_types")
    @classmethod
    def _filters(cls, value: list[str]) -> list[str]:
        return clean_filters(value) or []


class EndpointUpdate(BaseModel):
    url: HttpUrl | None = None
    description: str | None = Field(default=None, max_length=500)
    event_types: list[str] | None = Field(default=None, max_length=50)
    is_active: bool | None = None
    rotate_secret: bool = False

    @field_validator("event_types")
    @classmethod
    def _filters(cls, value: list[str] | None) -> list[str] | None:
        return clean_filters(value)


class EndpointOut(BaseModel):
    id: str
    url: str
    description: str | None
    event_types: list[str]
    is_active: bool
    circuit_state: str
    consecutive_failures: int
    circuit_opened_at: datetime | None
    created_at: datetime
    secret: str | None = Field(default=None, description="Only returned on create/rotate")

    @classmethod
    def from_model(cls, ep: Endpoint, reveal_secret: bool = False) -> EndpointOut:
        return cls(
            id=ep.id,
            url=ep.url,
            description=ep.description,
            event_types=list(ep.event_types or []),
            is_active=ep.is_active,
            circuit_state=ep.circuit_state,
            consecutive_failures=ep.consecutive_failures,
            circuit_opened_at=as_utc(ep.circuit_opened_at),
            created_at=as_utc(ep.created_at),  # type: ignore[arg-type]
            secret=ep.secret if reveal_secret else None,
        )


# -- events --------------------------------------------------------------------------
class EventPublish(BaseModel):
    id: str | None = Field(
        default=None,
        min_length=1,
        max_length=128,
        description="Client-supplied idempotency id. Re-publishing the same id is a no-op.",
    )
    type: str = Field(min_length=1, max_length=128, examples=["payment.captured"])
    data: dict[str, Any] = Field(default_factory=dict)
    source: str | None = Field(default=None, max_length=120)

    @field_validator("type")
    @classmethod
    def _type(cls, value: str) -> str:
        value = value.strip().lower()
        if not is_valid_event_type(value):
            raise ValueError("event type must look like 'domain.action' (lowercase, dots)")
        return value


class EventOut(BaseModel):
    id: str
    type: str
    data: dict[str, Any]
    source: str | None
    created_at: datetime
    duplicate: bool = False
    deliveries: list[DeliverySummary] = Field(default_factory=list)

    @classmethod
    def from_model(cls, ev: Event, duplicate: bool = False) -> EventOut:
        return cls(
            id=ev.id,
            type=ev.type,
            data=dict(ev.payload),
            source=ev.source,
            created_at=as_utc(ev.created_at),  # type: ignore[arg-type]
            duplicate=duplicate,
            deliveries=[DeliverySummary.from_model(d) for d in ev.deliveries],
        )


# -- deliveries ----------------------------------------------------------------------
DeliveryStatus = Literal["pending", "delivering", "succeeded", "dead"]


class AttemptOut(BaseModel):
    attempt_number: int
    status_code: int | None
    error: str | None
    response_excerpt: str | None
    duration_ms: int
    succeeded: bool
    created_at: datetime

    @classmethod
    def from_model(cls, a: DeliveryAttempt) -> AttemptOut:
        return cls(
            attempt_number=a.attempt_number,
            status_code=a.status_code,
            error=a.error,
            response_excerpt=a.response_excerpt,
            duration_ms=a.duration_ms,
            succeeded=a.succeeded,
            created_at=as_utc(a.created_at),  # type: ignore[arg-type]
        )


class DeliverySummary(BaseModel):
    id: str
    event_id: str
    endpoint_id: str
    status: str
    attempt_count: int
    replay_count: int
    next_attempt_at: datetime | None
    last_status_code: int | None
    last_error: str | None
    created_at: datetime
    delivered_at: datetime | None
    dead_lettered_at: datetime | None
    claimed_by: str | None = None
    claimed_until: datetime | None = None

    @classmethod
    def from_model(cls, d: Delivery) -> DeliverySummary:
        return cls(
            id=d.id,
            event_id=d.event_id,
            endpoint_id=d.endpoint_id,
            status=d.status,
            attempt_count=d.attempt_count,
            replay_count=d.replay_count,
            next_attempt_at=as_utc(d.next_attempt_at),
            last_status_code=d.last_status_code,
            last_error=d.last_error,
            created_at=as_utc(d.created_at),  # type: ignore[arg-type]
            delivered_at=as_utc(d.delivered_at),
            dead_lettered_at=as_utc(d.dead_lettered_at),
            claimed_by=d.claimed_by,
            claimed_until=as_utc(d.claimed_until),
        )


class DeliveryOut(DeliverySummary):
    event_type: str
    endpoint_url: str
    attempts: list[AttemptOut] = Field(default_factory=list)

    @classmethod
    def from_model(cls, d: Delivery) -> DeliveryOut:
        base = DeliverySummary.from_model(d).model_dump()
        return cls(
            **base,
            event_type=d.event.type,
            endpoint_url=d.endpoint.url,
            attempts=[AttemptOut.from_model(a) for a in d.attempts],
        )


class Page(BaseModel):
    items: list[Any]
    total: int
    limit: int
    offset: int


class MetricsOut(BaseModel):
    events_total: int
    endpoints_total: int
    endpoints_active: int
    endpoints_circuit_open: int
    deliveries_by_status: dict[str, int]
    attempts_total: int
    attempts_succeeded: int
    success_rate: float
    dead_letters: int
    due_now: int
    worker: dict[str, Any]


class HealthOut(BaseModel):
    status: Literal["ok", "degraded"]
    version: str
    database: Literal["ok", "error"]
    worker_running: bool
