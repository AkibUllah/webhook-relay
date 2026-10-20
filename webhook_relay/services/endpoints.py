from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from webhook_relay.core.circuit import CircuitState
from webhook_relay.core.signing import generate_secret
from webhook_relay.models import Endpoint
from webhook_relay.schemas import EndpointCreate, EndpointUpdate
from webhook_relay.services.errors import NotFoundError


class EndpointService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def create(self, data: EndpointCreate) -> Endpoint:
        endpoint = Endpoint(
            url=str(data.url),
            secret=data.secret or generate_secret(),
            description=data.description,
            event_types=data.event_types,
            is_active=data.is_active,
        )
        self.db.add(endpoint)
        self.db.flush()
        return endpoint

    def get(self, endpoint_id: str) -> Endpoint:
        endpoint = self.db.get(Endpoint, endpoint_id)
        if endpoint is None:
            raise NotFoundError(f"endpoint {endpoint_id} not found")
        return endpoint

    def list(self, limit: int, offset: int) -> tuple[list[Endpoint], int]:
        stmt = select(Endpoint).order_by(Endpoint.created_at).limit(limit).offset(offset)
        total = int(self.db.scalar(select(func.count(Endpoint.id))) or 0)
        return list(self.db.scalars(stmt).all()), total

    def update(self, endpoint_id: str, data: EndpointUpdate) -> tuple[Endpoint, bool]:
        endpoint = self.get(endpoint_id)
        if data.url is not None:
            endpoint.url = str(data.url)
        if data.description is not None:
            endpoint.description = data.description
        if data.event_types is not None:
            endpoint.event_types = data.event_types
        if data.is_active is not None:
            endpoint.is_active = data.is_active
        rotated = False
        if data.rotate_secret:
            endpoint.secret = generate_secret()
            rotated = True
        self.db.flush()
        return endpoint, rotated

    def delete(self, endpoint_id: str) -> None:
        endpoint = self.get(endpoint_id)
        # Keep history: deactivate rather than cascade-delete deliveries.
        endpoint.is_active = False
        self.db.flush()

    def reset_circuit(self, endpoint_id: str) -> Endpoint:
        endpoint = self.get(endpoint_id)
        endpoint.circuit_state = CircuitState.CLOSED.value
        endpoint.consecutive_failures = 0
        endpoint.circuit_opened_at = None
        self.db.flush()
        return endpoint
