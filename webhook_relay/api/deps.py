from __future__ import annotations

import hmac
from collections.abc import Generator

from fastapi import Depends, Header, Request
from sqlalchemy.orm import Session

from webhook_relay.config import Settings
from webhook_relay.services.deliveries import DeliveryService
from webhook_relay.services.endpoints import EndpointService
from webhook_relay.services.errors import UnauthorizedError
from webhook_relay.services.events import EventService


def get_settings_dep(request: Request) -> Settings:
    return request.app.state.settings


def get_db(request: Request) -> Generator[Session, None, None]:
    session: Session = request.app.state.session_factory()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def require_api_key(
    settings: Settings = Depends(get_settings_dep),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> None:
    if not settings.api_key:
        return
    if not x_api_key or not hmac.compare_digest(x_api_key, settings.api_key):
        raise UnauthorizedError("missing or invalid X-API-Key")


def get_event_service(db: Session = Depends(get_db)) -> EventService:
    return EventService(db)


def get_endpoint_service(db: Session = Depends(get_db)) -> EndpointService:
    return EndpointService(db)


def get_delivery_service(db: Session = Depends(get_db)) -> DeliveryService:
    return DeliveryService(db)
