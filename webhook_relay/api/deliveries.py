from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Request, Response
from sqlalchemy.orm import Session

from webhook_relay.api.deps import get_db, get_delivery_service, require_api_key
from webhook_relay.schemas import DeliveryOut, ErrorResponse
from webhook_relay.services.deliveries import DeliveryService

router = APIRouter(
    prefix="/v1",
    tags=["deliveries"],
    dependencies=[Depends(require_api_key)],
    responses={401: {"model": ErrorResponse}, 404: {"model": ErrorResponse}},
)


@router.get(
    "/deliveries", response_model=list[DeliveryOut], summary="List deliveries with attempts"
)
def list_deliveries(
    response: Response,
    service: DeliveryService = Depends(get_delivery_service),
    status: str | None = Query(default=None, pattern="^(pending|delivering|succeeded|dead)$"),
    endpoint_id: str | None = Query(default=None),
    event_id: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> list[DeliveryOut]:
    items, total = service.list(
        status=status, endpoint_id=endpoint_id, event_id=event_id, limit=limit, offset=offset
    )
    response.headers["X-Total-Count"] = str(total)
    return [DeliveryOut.from_model(d) for d in items]


@router.get("/deliveries/{delivery_id}", response_model=DeliveryOut, summary="Fetch a delivery")
def get_delivery(
    delivery_id: str, service: DeliveryService = Depends(get_delivery_service)
) -> DeliveryOut:
    return DeliveryOut.from_model(service.get(delivery_id))


@router.post(
    "/deliveries/{delivery_id}/replay",
    response_model=DeliveryOut,
    status_code=202,
    summary="Re-queue a delivery for immediate re-send",
)
def replay_delivery(
    delivery_id: str,
    request: Request,
    db: Session = Depends(get_db),
    service: DeliveryService = Depends(get_delivery_service),
) -> DeliveryOut:
    delivery = service.replay(delivery_id, now=request.app.state.clock())
    db.commit()
    return DeliveryOut.from_model(delivery)


@router.get(
    "/dead-letters", response_model=list[DeliveryOut], summary="Deliveries that exhausted retries"
)
def dead_letters(
    response: Response,
    service: DeliveryService = Depends(get_delivery_service),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> list[DeliveryOut]:
    items, total = service.dead_letters(limit, offset)
    response.headers["X-Total-Count"] = str(total)
    return [DeliveryOut.from_model(d) for d in items]
