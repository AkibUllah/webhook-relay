from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Response
from sqlalchemy.orm import Session

from webhook_relay.api.deps import get_db, get_endpoint_service, require_api_key
from webhook_relay.schemas import EndpointCreate, EndpointOut, EndpointUpdate, ErrorResponse
from webhook_relay.services.endpoints import EndpointService

router = APIRouter(
    prefix="/v1/endpoints",
    tags=["endpoints"],
    dependencies=[Depends(require_api_key)],
    responses={401: {"model": ErrorResponse}, 404: {"model": ErrorResponse}},
)


@router.post("", status_code=201, response_model=EndpointOut, summary="Register an endpoint")
def create_endpoint(
    payload: EndpointCreate,
    db: Session = Depends(get_db),
    service: EndpointService = Depends(get_endpoint_service),
) -> EndpointOut:
    endpoint = service.create(payload)
    db.commit()
    return EndpointOut.from_model(endpoint, reveal_secret=True)


@router.get("", response_model=list[EndpointOut], summary="List endpoints")
def list_endpoints(
    response: Response,
    service: EndpointService = Depends(get_endpoint_service),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> list[EndpointOut]:
    items, total = service.list(limit, offset)
    response.headers["X-Total-Count"] = str(total)
    return [EndpointOut.from_model(e) for e in items]


@router.get("/{endpoint_id}", response_model=EndpointOut, summary="Fetch an endpoint")
def get_endpoint(
    endpoint_id: str, service: EndpointService = Depends(get_endpoint_service)
) -> EndpointOut:
    return EndpointOut.from_model(service.get(endpoint_id))


@router.patch(
    "/{endpoint_id}",
    response_model=EndpointOut,
    summary="Update filters, URL, status or rotate secret",
)
def update_endpoint(
    endpoint_id: str,
    payload: EndpointUpdate,
    db: Session = Depends(get_db),
    service: EndpointService = Depends(get_endpoint_service),
) -> EndpointOut:
    endpoint, rotated = service.update(endpoint_id, payload)
    db.commit()
    return EndpointOut.from_model(endpoint, reveal_secret=rotated)


@router.delete("/{endpoint_id}", status_code=204, summary="Deactivate an endpoint (history kept)")
def delete_endpoint(
    endpoint_id: str,
    db: Session = Depends(get_db),
    service: EndpointService = Depends(get_endpoint_service),
) -> Response:
    service.delete(endpoint_id)
    db.commit()
    return Response(status_code=204)


@router.post(
    "/{endpoint_id}/reset-circuit", response_model=EndpointOut, summary="Close the circuit breaker"
)
def reset_circuit(
    endpoint_id: str,
    db: Session = Depends(get_db),
    service: EndpointService = Depends(get_endpoint_service),
) -> EndpointOut:
    endpoint = service.reset_circuit(endpoint_id)
    db.commit()
    return EndpointOut.from_model(endpoint)
