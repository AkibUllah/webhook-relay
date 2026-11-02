"""Standalone worker: `python -m webhook_relay.worker` (or `webhook-relay-worker`).

Run the API with WORKER_ENABLED=false and scale this process independently.

`--purge-once` applies retention a single time and exits, for cron-style scheduling instead
of (or alongside) the hourly purge inside the worker loop.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sys

from webhook_relay.config import get_settings
from webhook_relay.database import init_db, make_engine, make_session_factory
from webhook_relay.logging_config import configure_logging
from webhook_relay.models import utc_now
from webhook_relay.services.dispatcher import Dispatcher
from webhook_relay.services.retention import RetentionService

logger = logging.getLogger("webhook_relay.worker")


async def _run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    engine = make_engine(settings.database_url)
    init_db(engine)
    dispatcher = Dispatcher(make_session_factory(engine), settings)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, dispatcher.stop)
        except NotImplementedError:  # pragma: no cover - Windows
            pass
    await dispatcher.run_forever()
    engine.dispose()
    logger.info("worker exited: %s", dispatcher.stats.as_dict())


def purge_once(days: int | None = None) -> int:
    """Run retention once and print the result as JSON. Exit code 2 when retention is off."""
    settings = get_settings()
    if days is not None:
        settings = settings.model_copy(update={"retention_days": days})
    configure_logging(settings.log_level)
    if settings.retention_days is None:
        print("retention is disabled: set RETENTION_DAYS or pass --days N", file=sys.stderr)
        return 2
    engine = make_engine(settings.database_url)
    init_db(engine)
    try:
        result = RetentionService(make_session_factory(engine), settings).purge(utc_now())
    finally:
        engine.dispose()
    print(json.dumps(result.as_dict()))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="webhook-relay-worker", description=__doc__.split("\n")[0]
    )
    parser.add_argument(
        "--purge-once", action="store_true", help="apply retention once and exit (for cron)"
    )
    parser.add_argument(
        "--days", type=int, help="with --purge-once: override RETENTION_DAYS for this run"
    )
    args = parser.parse_args(argv)
    if args.days is not None and (not args.purge_once or args.days < 1):
        parser.error("--days needs --purge-once and a value of at least 1")
    if args.purge_once:
        return purge_once(args.days)
    asyncio.run(_run())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
