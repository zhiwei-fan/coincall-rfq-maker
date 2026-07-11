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

from coincall_rfq_maker.core.adapters.rest import CoincallError, CoincallRestClient
from coincall_rfq_maker.core.clock import get_timestamp_ms
from coincall_rfq_maker.events import ReconcileTick, RepriceTick
from coincall_rfq_maker.marketdata.instruments import InstrumentCatalog
from coincall_rfq_maker.marketdata.service import MarketDataService
from coincall_rfq_maker.observability import setup_logging
from coincall_rfq_maker.orchestration import Orchestrator, TransientOutageGate
from coincall_rfq_maker.persistence.store import PersistenceStore
from coincall_rfq_maker.pricing.engine import BlackScholesModel
from coincall_rfq_maker.quoting.lifecycle import QuoteLifecycle
from coincall_rfq_maker.risk.gate import NullExposureProvider, RiskGate
from coincall_rfq_maker.settings import MakerSettings
from coincall_rfq_maker.ws import CoincallWsClient

logger = logging.getLogger(__name__)

RECONCILE_INTERVAL_SECONDS = 60.0
_RECONCILER_STALL_FACTOR = 3
_FLATTEN_RETRY_INITIAL_SECONDS = 1.0
_FLATTEN_RETRY_CAP_SECONDS = 30.0


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


def load_settings_or_exit() -> MakerSettings:
    try:
        return MakerSettings()  # type: ignore[call-arg]
    except ValidationError as exc:
        details = "\n".join(
            f"- {'.'.join(str(part) for part in error['loc']) or 'settings'}: "
            f"{error['msg']} ({error['type']})"
            for error in exc.errors(include_input=False)
        )
        sys.stderr.write(
            "Configuration error: API_KEY and API_SECRET must be set (via environment "
            f"or .env) before starting rfq-maker.\n{details}\n"
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
        except asyncio.QueueShutDown:
            raise
        except Exception:
            logger.exception("Unhandled exception while dispatching %s", type(event).__name__)
        finally:
            events.task_done()


async def _shutdown_queue_on_signal(
    shutdown: asyncio.Event, events: "asyncio.Queue[object]"
) -> None:
    await shutdown.wait()
    events.shutdown()


async def _quote_refresh_loop(
    events: "asyncio.Queue[object]", shutdown: asyncio.Event, interval_seconds: float
) -> None:
    await _tick_loop(events, shutdown, interval_seconds, RepriceTick())


async def _reconcile_loop(
    events: "asyncio.Queue[object]", shutdown: asyncio.Event, interval_seconds: float
) -> None:
    await _tick_loop(events, shutdown, interval_seconds, ReconcileTick())


def _reconciler_is_stalled(now_ms: int, baseline_ms: int, interval_seconds: float) -> bool:
    """A reconcile cycle should complete every interval; tolerate _RECONCILER_STALL_FACTOR of them.

    The factor is the whole knob: too small and a merely busy dispatcher cries stall, too large and
    a wedged reconciler -- the sole quiet-book recovery signal -- goes unreported for many minutes.
    """
    return now_ms - baseline_ms > _RECONCILER_STALL_FACTOR * interval_seconds * 1000


async def _reconciler_watchdog(orchestrator: Orchestrator, shutdown: asyncio.Event) -> None:
    started_ms = get_timestamp_ms()
    while not shutdown.is_set():
        try:
            async with asyncio.timeout(RECONCILE_INTERVAL_SECONDS):
                await shutdown.wait()
            return
        except TimeoutError:
            last = orchestrator.reconciler_last_cycle_ms
            baseline = last if last is not None else started_ms
            now_ms = get_timestamp_ms()
            if _reconciler_is_stalled(now_ms, baseline, RECONCILE_INTERVAL_SECONDS):
                # A busy serial dispatcher must never turn this observation into a process-lifetime
                # halt. This is deliberately ERROR-only; external monitoring escalates.
                logger.error(
                    "RECONCILER STALLED: no completed reconcile cycle for %.0fs -- sole "
                    "quiet-book recovery signal (backoff may latch); investigate the dispatcher",
                    (now_ms - baseline) / 1000,
                )


async def _tick_loop(
    events: "asyncio.Queue[object]",
    shutdown: asyncio.Event,
    interval_seconds: float,
    event: object,
) -> None:
    while not shutdown.is_set():
        try:
            async with asyncio.timeout(interval_seconds):
                await shutdown.wait()
            return
        except TimeoutError:
            try:
                await events.put(event)
            except asyncio.QueueShutDown:
                return


async def _enqueue_startup_backfill(events: "asyncio.Queue[object]") -> None:
    await events.put(ReconcileTick())


async def _cancel_all_on_graceful_stop(quote_lifecycle: QuoteLifecycle) -> None:
    try:
        await quote_lifecycle.cancel_all()
    except Exception as exc:
        logger.error(
            "Shutdown cancel-all failed; quotes may remain live on the exchange: %s",
            exc,
            exc_info=exc,
        )
        sys.stderr.write(
            "WARNING: failed to cancel all quotes during graceful shutdown; "
            f"quotes may remain live on the exchange: {exc}\n"
        )


def _flatten_retry_delay(attempt: int) -> float:
    """Exponential backoff for flatten retries, capped.

    The cap matters: retries are unbounded, so without it the delay doubles without limit and a
    tripped process would eventually wait hours between attempts to cancel its live quotes.
    """
    return min(_FLATTEN_RETRY_CAP_SECONDS, _FLATTEN_RETRY_INITIAL_SECONDS * 2 ** (attempt - 1))


async def _flatten_on_kill_switch(
    trip_event: asyncio.Event,
    shutdown: asyncio.Event,
    quote_lifecycle: QuoteLifecycle,
    risk_gate: RiskGate,
) -> None:
    trip_wait = asyncio.create_task(trip_event.wait())
    shutdown_wait = asyncio.create_task(shutdown.wait())
    try:
        await asyncio.wait((trip_wait, shutdown_wait), return_when=asyncio.FIRST_COMPLETED)
    finally:
        for task in (trip_wait, shutdown_wait):
            if not task.done():
                task.cancel()
        await asyncio.gather(trip_wait, shutdown_wait, return_exceptions=True)

    if not trip_event.is_set():
        return

    failure_count = risk_gate.consecutive_failures
    logger.error("Kill-switch flatten starting after %d consecutive API failures", failure_count)
    # Invariant: a tripped process must never idle with live quotes on the exchange while a
    # retryable flatten is still available.
    # Retries are unbounded: once halted, flattening is the only useful work left.
    attempt = 1
    while True:
        try:
            await quote_lifecycle.cancel_all()
        except Exception as exc:
            delay = _flatten_retry_delay(attempt)
            logger.error(
                "KILL-SWITCH FLATTEN attempt %d FAILED; retrying in %.0fs; "
                "quotes may remain live: %s",
                attempt,
                delay,
                exc,
                exc_info=exc,
            )
            try:
                async with asyncio.timeout(delay):
                    await shutdown.wait()
            except TimeoutError:
                pass

            if shutdown.is_set():
                logger.error(
                    "Kill-switch flatten retry loop stopping for shutdown; "
                    "graceful-stop cancel-all takes over"
                )
                return

            attempt += 1
        else:
            logger.error(
                "Kill-switch flatten completed after %d consecutive API failures (attempt %d)",
                failure_count,
                attempt,
            )
            return


def _log_task_failures(exceptions: Sequence[BaseException]) -> None:
    for exc in exceptions:
        logger.error("Task failed: %s", exc, exc_info=exc)


def _all_shutdown_race_failures(exceptions: Sequence[BaseException]) -> bool:
    return all(isinstance(exc, asyncio.QueueShutDown) for exc in exceptions)


def _needs_exposure_ack(dry_run: bool, exposure_provider: object) -> bool:
    """Only a live process wired to the placeholder provider needs operator acknowledgment."""
    return not dry_run and isinstance(exposure_provider, NullExposureProvider)


async def run_async(settings: MakerSettings) -> None:
    setup_logging(settings.log_level, settings.log_file)
    logger.info("Starting rfq-maker (dry_run=%s)", settings.dry_run)

    exposure_provider = NullExposureProvider()
    if _needs_exposure_ack(settings.dry_run, exposure_provider):
        if not settings.allow_no_exposure_limits:
            sys.stderr.write(
                "Live mode requires ALLOW_NO_EXPOSURE_LIMITS=true because position/greek risk "
                "is not enforced.\n"
            )
            raise SystemExit(2)
        logger.warning(
            "LIVE MODE WITHOUT EXPOSURE LIMITS: ALLOW_NO_EXPOSURE_LIMITS=true acknowledged; "
            "position/greek risk is NOT enforced"
        )

    shutdown = asyncio.Event()
    kill_switch_trip = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown.set)

    events: asyncio.Queue[object] = asyncio.Queue()
    maker_credentials = settings.credentials()
    api_secret = maker_credentials.api_secret.get_secret_value()

    async with (
        CoincallRestClient(maker_credentials.api_key, api_secret, settings.rest_base_url) as rest,
        PersistenceStore(settings.db_path) as persistence,
    ):
        market_data = MarketDataService(rest, events, settings.price_move_threshold)
        instrument_catalog = InstrumentCatalog(rest)
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
            exposure_provider=exposure_provider,
            on_trip=kill_switch_trip.set,
        )
        quote_lifecycle = QuoteLifecycle(rest, dry_run=settings.dry_run, api_reporter=risk_gate)
        outage_gate = TransientOutageGate()
        orchestrator = Orchestrator(
            rest,
            market_data,
            pricing_model,
            risk_gate,
            quote_lifecycle,
            instrument_catalog,
            persistence,
            outage_gate,
        )
        ws_client = CoincallWsClient(
            settings.ws_url,
            maker_credentials.api_key,
            api_secret,
            events,
            heartbeat_interval_seconds=settings.heartbeat_interval_seconds,
        )

        if settings.cancel_all_on_start:
            try:
                await quote_lifecycle.cancel_all()
            except CoincallError as exc:
                sys.stderr.write(f"Startup error: failed to cancel all quotes: {exc}\n")
                raise SystemExit(1) from exc
        else:
            logger.warning(
                "CANCEL_ALL_ON_START=false: pre-existing exchange quotes will be adopted or "
                "50012-conflict-resolved on first reconcile; startup ordering may produce "
                "transient duplicate-quote conflicts (supported config is true)"
            )

        await _enqueue_startup_backfill(events)

        task_failure_group: ExceptionGroup[Exception] | None = None
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(ws_client.run(shutdown), name="ws-client")
                tg.create_task(
                    market_data.run(shutdown, settings.pricing_refresh_seconds), name="market-data"
                )
                tg.create_task(_dispatch_loop(events, orchestrator), name="dispatcher")
                tg.create_task(_shutdown_queue_on_signal(shutdown, events), name="queue-shutdown")
                tg.create_task(
                    _quote_refresh_loop(events, shutdown, settings.quote_refresh_seconds),
                    name="quote-refresh",
                )
                tg.create_task(
                    _reconcile_loop(events, shutdown, RECONCILE_INTERVAL_SECONDS),
                    name="reconciler",
                )
                tg.create_task(
                    _reconciler_watchdog(orchestrator, shutdown),
                    name="reconciler-watchdog",
                )
                tg.create_task(
                    _flatten_on_kill_switch(kill_switch_trip, shutdown, quote_lifecycle, risk_gate),
                    name="kill-switch-flatten",
                )
        except* Exception as eg:
            if shutdown.is_set() and _all_shutdown_race_failures(eg.exceptions):
                logger.debug("Ignoring shutdown-race task failures: %s", eg.exceptions)
            else:
                _log_task_failures(eg.exceptions)
                task_failure_group = eg
        finally:
            if settings.cancel_all_on_stop:
                await _cancel_all_on_graceful_stop(quote_lifecycle)

        if task_failure_group is not None:
            raise task_failure_group

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
