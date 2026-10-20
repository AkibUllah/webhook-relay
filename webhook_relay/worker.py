"""Standalone worker: `python -m webhook_relay.worker` (or `webhook-relay-worker`).

Run the API with WORKER_ENABLED=false and scale this process independently.
"""

from __future__ import annotations

import asyncio
import logging
import signal

from webhook_relay.config import get_settings
from webhook_relay.database import init_db, make_engine, make_session_factory
from webhook_relay.logging_config import configure_logging
from webhook_relay.services.dispatcher import Dispatcher

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


def main() -> int:
    asyncio.run(_run())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
