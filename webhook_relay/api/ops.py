from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import text
from sqlalchemy.orm import Session

from webhook_relay import __version__
from webhook_relay.api.deps import get_db
from webhook_relay.core.backoff import describe_schedule
from webhook_relay.models import utc_now
from webhook_relay.schemas import HealthOut, MetricsOut
from webhook_relay.services.deliveries import DeliveryService
from webhook_relay.services.endpoints import EndpointService
from webhook_relay.services.metrics import collect_metrics

router = APIRouter(tags=["ops"])
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent.parent / "templates"))


@router.get("/health", response_model=HealthOut, summary="Liveness/readiness")
def health(request: Request) -> HealthOut:
    database = "ok"
    try:
        with request.app.state.engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception:  # pragma: no cover
        database = "error"
    dispatcher = request.app.state.dispatcher
    return HealthOut(
        status="ok" if database == "ok" else "degraded",
        version=__version__,
        database=database,
        worker_running=bool(dispatcher.stats.running),
    )


@router.get("/metrics", response_model=MetricsOut, summary="JSON counters")
def metrics(request: Request, db: Session = Depends(get_db)) -> MetricsOut:
    data = collect_metrics(db, request.app.state.dispatcher.stats, utc_now())
    return MetricsOut(**data)


@router.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
def dashboard(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    endpoints, _ = EndpointService(db).list(limit=100, offset=0)
    deliveries = DeliveryService(db).recent(limit=30)
    data = collect_metrics(db, request.app.state.dispatcher.stats, utc_now())
    settings = request.app.state.settings
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "endpoints": endpoints,
            "deliveries": deliveries,
            "metrics": data,
            "version": __version__,
            "schedule": describe_schedule(settings.schedule),
            "max_attempts": settings.max_attempts,
        },
    )
