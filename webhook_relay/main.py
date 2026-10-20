"""FastAPI application factory. The delivery worker runs as an asyncio task inside the app
(WORKER_ENABLED=true) or separately via `python -m webhook_relay.worker`."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from webhook_relay import __version__
from webhook_relay.api import deliveries, endpoints, events, ops
from webhook_relay.config import Settings, get_settings
from webhook_relay.database import init_db, make_engine, make_session_factory
from webhook_relay.logging_config import configure_logging
from webhook_relay.services.dispatcher import Dispatcher
from webhook_relay.services.errors import ServiceError

logger = logging.getLogger("webhook_relay")


def create_app(
    settings: Settings | None = None, *, transport: httpx.AsyncBaseTransport | None = None
) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    engine = make_engine(settings.database_url)
    session_factory = make_session_factory(engine)
    dispatcher = Dispatcher(session_factory, settings, transport=transport)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        init_db(engine)
        task: asyncio.Task | None = None
        if settings.worker_enabled:
            task = asyncio.create_task(dispatcher.run_forever(), name="delivery-worker")
        logger.info("webhook-relay %s started (worker=%s)", __version__, settings.worker_enabled)
        yield
        if task is not None:
            dispatcher.stop()
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=5)
        engine.dispose()

    app = FastAPI(
        title="webhook-relay",
        version=__version__,
        description="Reliable webhook delivery: fan-out, HMAC signatures, exponential backoff, "
        "per-endpoint circuit breakers, dead letters and replay.",
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.engine = engine
    app.state.session_factory = session_factory
    app.state.dispatcher = dispatcher

    @app.exception_handler(ServiceError)
    async def _service_error(_: Request, exc: ServiceError) -> JSONResponse:
        return JSONResponse(
            {"error": {"code": exc.code, "message": exc.message}}, status_code=exc.status_code
        )

    @app.exception_handler(RequestValidationError)
    async def _validation(_: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            {
                "error": {
                    "code": "validation_error",
                    "message": "Invalid request",
                    "details": jsonable_encoder(exc.errors()),
                }
            },
            status_code=422,
        )

    app.include_router(ops.router)
    app.include_router(endpoints.router)
    app.include_router(events.router)
    app.include_router(deliveries.router)
    return app


app = create_app()
