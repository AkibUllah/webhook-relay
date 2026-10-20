from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Response
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from webhook_relay.api.deps import get_db, get_event_service, require_api_key
from webhook_relay.schemas import ErrorResponse, EventOut, EventPublish
from webhook_relay.services.events import EventService

router = APIRouter(
    prefix="/v1/events",
    tags=["events"],
    dependencies=[Depends(require_api_key)],
    responses={401: {"model": ErrorResponse}, 404: {"model": ErrorResponse}},
)


@router.post(
    "",
    response_model=EventOut,
    status_code=201,
    responses={200: {"model": EventOut, "description": "Duplicate id; existing event returned"}},
    summary="Publish an event (fans out to matching endpoints)",
)
def publish_event(
    payload: EventPublish,
    db: Session = Depends(get_db),
    service: EventService = Depends(get_event_service),
) -> JSONResponse:
    event, duplicate = service.publish(payload)
    db.commit()
    body = EventOut.from_model(event, duplicate=duplicate).model_dump(mode="json")
    return JSONResponse(body, status_code=200 if duplicate else 201)


@router.get("", response_model=list[EventOut], summary="List events")
def list_events(
    response: Response,
    service: EventService = Depends(get_event_service),
    type: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> list[EventOut]:
    items, total = service.list(type, limit, offset)
    response.headers["X-Total-Count"] = str(total)
    return [EventOut.from_model(e) for e in items]


@router.get("/{event_id}", response_model=EventOut, summary="Fetch an event with its deliveries")
def get_event(event_id: str, service: EventService = Depends(get_event_service)) -> EventOut:
    return EventOut.from_model(service.get(event_id))  # type: ignore[arg-type]
