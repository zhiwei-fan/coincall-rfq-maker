"""CLI entry point.

Parses arguments, loads settings (failing fast and clearly if API_KEY/
API_SECRET are missing), wires the whole runtime under one
`asyncio.TaskGroup`, and installs SIGINT/SIGTERM handlers that flip a
shutdown event so the TaskGroup unwinds cleanly.
"""

import argparse
import asyncio
import logging
import signal
import sys
from collections.abc import Sequence

from pydantic import ValidationError

from coincall_rfq_maker.adapters.rest import CoincallError, CoincallRestClient
from coincall_rfq_maker.adapters.ws import CoincallWsClient
from coincall_rfq_maker.marketdata.service import MarketDataService
from coincall_rfq_maker.observability import setup_logging
from coincall_rfq_maker.orchestration import Orchestrator
from coincall_rfq_maker.persistence.store import PersistenceStore
from coincall_rfq_maker.pricing.engine import BlackScholesModel
from coincall_rfq_maker.quoting.lifecycle import QuoteLifecycle
from coincall_rfq_maker.risk.gate import RiskGate
from coincall_rfq_maker.settings import Settings

logger = logging.getLogger(__name__)

RECONCILE_INTERVAL_SECONDS = 60.0


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="rfq-maker", description="Coincall option RFQ market maker."
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "Override DRY_RUN from settings. In dry-run (the default), quotes are "
            "computed and logged but never submitted to the exchange."
        ),
    )
    return parser.parse_args(argv)


def load_settings_or_exit() -> Settings:
    try:
        return Settings()  # type: ignore[call-arg]
    except ValidationError as exc:
        sys.stderr.write(
            "Configuration error: API_KEY and API_SECRET must be set (via environment "
            f"or .env) before starting rfq-maker.\n{exc}\n"
        )
        raise SystemExit(1) from exc


async def _dispatch_loop(events: "asyncio.Queue[object]", orchestrator: Orchestrator) -> None:
    while True:
        try:
            event = await events.get()
        except asyncio.QueueShutDown:
            return
        try:
            await orchestrator.handle_event(event)
        finally:
            events.task_done()


async def _shutdown_queue_on_signal(
    shutdown: asyncio.Event, events: "asyncio.Queue[object]"
) -> None:
    await shutdown.wait()
    events.shutdown()


async def _quote_refresh_loop(
    orchestrator: Orchestrator, shutdown: asyncio.Event, interval_seconds: float
) -> None:
    while not shutdown.is_set():
        try:
            async with asyncio.timeout(interval_seconds):
                await shutdown.wait()
            return
        except TimeoutError:
            await orchestrator.reprice_all_active()


async def _reconcile_loop(
    orchestrator: Orchestrator, shutdown: asyncio.Event, interval_seconds: float
) -> None:
    while not shutdown.is_set():
        try:
            async with asyncio.timeout(interval_seconds):
                await shutdown.wait()
            return
        except TimeoutError:
            await orchestrator.reconcile_with_exchange()


def _log_task_failures(exceptions: Sequence[BaseException]) -> None:
    for exc in exceptions:
        logger.error("Task failed: %s", exc, exc_info=exc)


async def run_async(settings: Settings) -> None:
    setup_logging(settings.log_level)
    logger.info("Starting rfq-maker (dry_run=%s)", settings.dry_run)

    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown.set)

    events: asyncio.Queue[object] = asyncio.Queue()
    api_secret = settings.api_secret.get_secret_value()

    async with (
        CoincallRestClient(settings.api_key, api_secret, settings.rest_base_url) as rest,
        PersistenceStore(settings.db_path) as persistence,
    ):
        market_data = MarketDataService(rest, events, settings.price_move_threshold)
        pricing_model = BlackScholesModel(
            bid_vol=settings.bid_vol,
            ask_vol=settings.ask_vol,
            risk_free_rate=settings.risk_free_rate,
        )
        risk_gate = RiskGate(
            max_quote_notional_usd=settings.max_quote_notional_usd,
            max_leg_qty=settings.max_leg_qty,
            min_time_to_expiry_hours=settings.min_time_to_expiry_hours,
            stale_market_data_seconds=settings.stale_market_data_seconds,
        )
        quote_lifecycle = QuoteLifecycle(rest, dry_run=settings.dry_run)
        orchestrator = Orchestrator(
            rest, market_data, pricing_model, risk_gate, quote_lifecycle, persistence
        )
        ws_client = CoincallWsClient(settings.ws_url, settings.api_key, api_secret, events)

        if settings.cancel_all_on_start:
            try:
                await quote_lifecycle.cancel_all()
            except CoincallError as exc:
                sys.stderr.write(f"Startup error: failed to cancel all quotes: {exc}\n")
                raise SystemExit(1) from exc

        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(ws_client.run(shutdown), name="ws-client")
                tg.create_task(
                    market_data.run(shutdown, settings.pricing_refresh_seconds), name="market-data"
                )
                tg.create_task(_dispatch_loop(events, orchestrator), name="dispatcher")
                tg.create_task(_shutdown_queue_on_signal(shutdown, events), name="queue-shutdown")
                tg.create_task(
                    _quote_refresh_loop(orchestrator, shutdown, settings.quote_refresh_seconds),
                    name="quote-refresh",
                )
                tg.create_task(
                    _reconcile_loop(orchestrator, shutdown, RECONCILE_INTERVAL_SECONDS),
                    name="reconciler",
                )
        except* Exception as eg:
            _log_task_failures(eg.exceptions)
            raise

    logger.info("rfq-maker stopped")


def run() -> None:
    """Console-script entry point (`rfq-maker`)."""
    args = parse_args()
    settings = load_settings_or_exit()
    if args.dry_run is not None:
        settings = settings.model_copy(update={"dry_run": args.dry_run})

    try:
        asyncio.run(run_async(settings))
    except KeyboardInterrupt:
        pass
    except ExceptionGroup:
        logger.error("rfq-maker exited due to a task failure")
        sys.exit(1)


if __name__ == "__main__":
    run()
