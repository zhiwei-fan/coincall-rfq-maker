import asyncio
import logging.handlers
from pathlib import Path
from types import TracebackType
from typing import Any, ClassVar, Self

import aiosqlite
import pytest

from coincall_rfq_maker import cli
from coincall_rfq_maker.core.adapters.rest import (
    CoincallConnectivityError,
    CoincallRequestError,
)
from coincall_rfq_maker.events import ReconcileTick, RepriceTick, TradeExecuted
from coincall_rfq_maker.settings import MakerSettings


class FakeRestContext:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        pass


class CountingRestContext(FakeRestContext):
    constructor_calls = 0

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        type(self).constructor_calls += 1


class FakePersistenceContext:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        pass


class FailingQuoteLifecycle:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    async def cancel_all(self) -> None:
        raise CoincallRequestError("cancel-all unavailable")


class RecordingQuoteLifecycle:
    instances: ClassVar[list["RecordingQuoteLifecycle"]] = []
    fail_on_cancel_all: ClassVar[bool] = False
    cancel_all_exception: ClassVar[Exception | None] = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.cancel_all_calls = 0
        type(self).instances.append(self)

    async def cancel_all(self) -> None:
        self.cancel_all_calls += 1
        if type(self).cancel_all_exception is not None:
            raise type(self).cancel_all_exception
        if type(self).fail_on_cancel_all:
            raise CoincallRequestError("shutdown cancel-all unavailable")


class FlakyFlattenQuoteLifecycle:
    def __init__(self) -> None:
        self.cancel_all_calls = 0

    async def cancel_all(self) -> None:
        self.cancel_all_calls += 1
        if self.cancel_all_calls <= 2:
            raise CoincallConnectivityError("cancel-all temporarily unavailable")


class ShutdownAfterFailedFlattenQuoteLifecycle:
    def __init__(self, shutdown: asyncio.Event) -> None:
        self.shutdown = shutdown
        self.cancel_all_calls = 0

    async def cancel_all(self) -> None:
        self.cancel_all_calls += 1
        self.shutdown.set()
        raise CoincallConnectivityError("cancel-all temporarily unavailable")


class FakeRiskGate:
    consecutive_failures = 3


class RealExposureProviderStub:
    def current_exposure(self) -> object:
        return object()


class SignalShutdownWsClient:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    async def run(self, shutdown: asyncio.Event) -> None:
        shutdown.set()


class StopAwareWsClient:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    async def run(self, shutdown: asyncio.Event) -> None:
        await shutdown.wait()


class StopAwareMarketData:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    async def run(self, shutdown: asyncio.Event, interval_seconds: float) -> None:
        await shutdown.wait()


class FailingMarketData:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    async def run(self, shutdown: asyncio.Event, interval_seconds: float) -> None:
        raise RuntimeError("market-data task failed")


class ShutdownRaceMarketData:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    async def run(self, shutdown: asyncio.Event, interval_seconds: float) -> None:
        await shutdown.wait()
        raise asyncio.QueueShutDown


class NoopOrchestrator:
    reconciler_last_cycle_ms = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    async def handle_event(self, event: object) -> None:
        pass


class AuditEnqueueingOrchestrator:
    reconciler_last_cycle_ms = None
    enqueued: ClassVar[asyncio.Event]

    def __init__(self, *args: object, **kwargs: object) -> None:
        self._audit_outbox = args[6]

    async def handle_event(self, event: object) -> None:
        self._audit_outbox.enqueue_fill(  # type: ignore[union-attr]
            TradeExecuted(
                block_trade_id="block-1",
                quote_id="quote-1",
                request_id="rfq-1",
                filled_price=100.0,
                filled_quantity=1.0,
                fill_time_ms=1,
            ),
            now_ms=2,
        )
        type(self).enqueued.set()


class ShutdownAfterAuditWsClient:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    async def run(self, shutdown: asyncio.Event) -> None:
        await AuditEnqueueingOrchestrator.enqueued.wait()
        shutdown.set()


class FailsFirstOrchestrator:
    def __init__(self) -> None:
        self.events: list[object] = []

    async def handle_event(self, event: object) -> None:
        self.events.append(event)
        if len(self.events) == 1:
            raise RuntimeError("bad event")


class ShutdownOnPutQueue:
    def __init__(self) -> None:
        self.puts = 0

    async def put(self, event: object) -> None:
        self.puts += 1
        raise asyncio.QueueShutDown


class WatchdogRiskGate:
    def __init__(self) -> None:
        self.kill_switch_tripped = False
        self.failures_recorded = 0


class WatchdogOrchestrator:
    def __init__(self, last_cycle_ms: int, shutdown: asyncio.Event) -> None:
        self._last_cycle_ms = last_cycle_ms
        self._shutdown = shutdown
        self.risk_gate = WatchdogRiskGate()

    @property
    def reconciler_last_cycle_ms(self) -> int:
        self._shutdown.set()
        return self._last_cycle_ms


def test_load_settings_error_does_not_print_secret_value(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path,
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("API_KEY", raising=False)
    monkeypatch.setenv("API_SECRET", "SUPERSECRETVALUE")

    with pytest.raises(SystemExit):
        cli.load_settings_or_exit()

    stderr = capsys.readouterr().err
    assert "API_KEY" in stderr
    assert "Field required" in stderr
    assert "SUPERSECRETVALUE" not in stderr
    assert "input_value" not in stderr


@pytest.mark.asyncio
async def test_startup_cancel_all_failure_exits_cleanly(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(cli, "CoincallRestClient", FakeRestContext)
    monkeypatch.setattr(cli, "PersistenceStore", FakePersistenceContext)
    monkeypatch.setattr(cli, "QuoteLifecycle", FailingQuoteLifecycle)
    settings = MakerSettings(API_KEY="key", API_SECRET="secret", CANCEL_ALL_ON_START=True)

    with pytest.raises(SystemExit) as exc_info:
        await cli.run_async(settings)

    assert exc_info.value.code == 1
    assert capsys.readouterr().err == (
        "Startup error: failed to cancel all quotes: cancel-all unavailable\n"
    )


@pytest.mark.asyncio
async def test_live_mode_with_null_exposure_provider_requires_ack_before_exchange_io(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    CountingRestContext.constructor_calls = 0
    monkeypatch.setattr(cli, "setup_logging", lambda *args: None)
    monkeypatch.setattr(cli, "CoincallRestClient", CountingRestContext)
    settings = MakerSettings(
        API_KEY="key",
        API_SECRET="secret",
        DRY_RUN=False,
        CANCEL_ALL_ON_START=False,
        CANCEL_ALL_ON_STOP=False,
    )

    # Bounded: if the refusal guard is ever broken, run_async starts the real
    # TaskGroup and would await forever -- the timeout turns that into a fast
    # failure instead of a hung test run (observed with a gate mutant, 2026-07-11).
    with pytest.raises(SystemExit) as exc_info:
        async with asyncio.timeout(5):
            await cli.run_async(settings)

    assert exc_info.value.code == 2
    assert "ALLOW_NO_EXPOSURE_LIMITS=true" in capsys.readouterr().err
    assert CountingRestContext.constructor_calls == 0


@pytest.mark.asyncio
async def test_live_mode_acknowledges_missing_exposure_limits_once(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(cli, "setup_logging", lambda *args: None)
    monkeypatch.setattr(cli, "CoincallRestClient", FakeRestContext)
    monkeypatch.setattr(cli, "PersistenceStore", FakePersistenceContext)
    monkeypatch.setattr(cli, "QuoteLifecycle", RecordingQuoteLifecycle)
    monkeypatch.setattr(cli, "CoincallWsClient", SignalShutdownWsClient)
    monkeypatch.setattr(cli, "MarketDataService", StopAwareMarketData)
    monkeypatch.setattr(cli, "Orchestrator", NoopOrchestrator)
    settings = MakerSettings(
        API_KEY="key",
        API_SECRET="secret",
        DRY_RUN=False,
        ALLOW_NO_EXPOSURE_LIMITS=True,
        CANCEL_ALL_ON_START=False,
        CANCEL_ALL_ON_STOP=False,
    )

    with caplog.at_level(logging.WARNING, logger="coincall_rfq_maker.cli"):
        await cli.run_async(settings)

    assert (
        sum("LIVE MODE WITHOUT EXPOSURE LIMITS" in record.message for record in caplog.records) == 1
    )


@pytest.mark.asyncio
async def test_dry_run_needs_no_exposure_acknowledgment_or_warning(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(cli, "setup_logging", lambda *args: None)
    monkeypatch.setattr(cli, "CoincallRestClient", FakeRestContext)
    monkeypatch.setattr(cli, "PersistenceStore", FakePersistenceContext)
    monkeypatch.setattr(cli, "QuoteLifecycle", RecordingQuoteLifecycle)
    monkeypatch.setattr(cli, "CoincallWsClient", SignalShutdownWsClient)
    monkeypatch.setattr(cli, "MarketDataService", StopAwareMarketData)
    monkeypatch.setattr(cli, "Orchestrator", NoopOrchestrator)
    settings = MakerSettings(
        API_KEY="key",
        API_SECRET="secret",
        CANCEL_ALL_ON_START=False,
        CANCEL_ALL_ON_STOP=False,
    )

    with caplog.at_level(logging.WARNING, logger="coincall_rfq_maker.cli"):
        await cli.run_async(settings)

    assert "LIVE MODE WITHOUT EXPOSURE LIMITS" not in caplog.text


def test_only_null_exposure_provider_needs_live_acknowledgment() -> None:
    assert cli._needs_exposure_ack(False, cli.NullExposureProvider())
    assert not cli._needs_exposure_ack(True, cli.NullExposureProvider())
    assert not cli._needs_exposure_ack(False, RealExposureProviderStub())


@pytest.mark.asyncio
async def test_run_async_uses_settings_log_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    log_file = tmp_path / "configured.log"
    monkeypatch.setattr(cli, "CoincallRestClient", FakeRestContext)
    monkeypatch.setattr(cli, "PersistenceStore", FakePersistenceContext)
    monkeypatch.setattr(cli, "QuoteLifecycle", RecordingQuoteLifecycle)
    monkeypatch.setattr(cli, "CoincallWsClient", SignalShutdownWsClient)
    monkeypatch.setattr(cli, "MarketDataService", StopAwareMarketData)
    monkeypatch.setattr(cli, "Orchestrator", NoopOrchestrator)
    settings = MakerSettings(
        API_KEY="key",
        API_SECRET="secret",
        CANCEL_ALL_ON_START=False,
        CANCEL_ALL_ON_STOP=False,
        LOG_FILE=str(log_file),
    )

    await cli.run_async(settings)

    file_handlers = [
        handler
        for handler in logging.getLogger().handlers
        if isinstance(handler, logging.handlers.RotatingFileHandler)
    ]
    assert len(file_handlers) == 1
    assert Path(file_handlers[0].baseFilename) == log_file


@pytest.mark.asyncio
async def test_signal_shutdown_cancel_all_on_stop_failure_exits_cleanly(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    RecordingQuoteLifecycle.instances = []
    RecordingQuoteLifecycle.fail_on_cancel_all = True
    RecordingQuoteLifecycle.cancel_all_exception = None
    monkeypatch.setattr(cli, "CoincallRestClient", FakeRestContext)
    monkeypatch.setattr(cli, "PersistenceStore", FakePersistenceContext)
    monkeypatch.setattr(cli, "QuoteLifecycle", RecordingQuoteLifecycle)
    monkeypatch.setattr(cli, "CoincallWsClient", SignalShutdownWsClient)
    monkeypatch.setattr(cli, "MarketDataService", StopAwareMarketData)
    monkeypatch.setattr(cli, "Orchestrator", NoopOrchestrator)
    settings = MakerSettings(
        API_KEY="key",
        API_SECRET="secret",
        CANCEL_ALL_ON_START=False,
        CANCEL_ALL_ON_STOP=True,
    )

    await cli.run_async(settings)

    assert len(RecordingQuoteLifecycle.instances) == 1
    assert RecordingQuoteLifecycle.instances[0].cancel_all_calls == 1
    assert (
        "WARNING: failed to cancel all quotes during graceful shutdown" in capsys.readouterr().err
    )


@pytest.mark.asyncio
async def test_signal_shutdown_cancel_all_on_stop_false_does_not_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    RecordingQuoteLifecycle.instances = []
    RecordingQuoteLifecycle.fail_on_cancel_all = False
    RecordingQuoteLifecycle.cancel_all_exception = None
    monkeypatch.setattr(cli, "CoincallRestClient", FakeRestContext)
    monkeypatch.setattr(cli, "PersistenceStore", FakePersistenceContext)
    monkeypatch.setattr(cli, "QuoteLifecycle", RecordingQuoteLifecycle)
    monkeypatch.setattr(cli, "CoincallWsClient", SignalShutdownWsClient)
    monkeypatch.setattr(cli, "MarketDataService", StopAwareMarketData)
    monkeypatch.setattr(cli, "Orchestrator", NoopOrchestrator)
    settings = MakerSettings(
        API_KEY="key",
        API_SECRET="secret",
        CANCEL_ALL_ON_START=False,
        CANCEL_ALL_ON_STOP=False,
    )

    await cli.run_async(settings)

    assert len(RecordingQuoteLifecycle.instances) == 1
    assert RecordingQuoteLifecycle.instances[0].cancel_all_calls == 0


@pytest.mark.asyncio
async def test_signal_shutdown_taskgroup_shutdown_race_attempts_cancel_all_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    RecordingQuoteLifecycle.instances = []
    RecordingQuoteLifecycle.fail_on_cancel_all = False
    RecordingQuoteLifecycle.cancel_all_exception = None
    monkeypatch.setattr(cli, "CoincallRestClient", FakeRestContext)
    monkeypatch.setattr(cli, "PersistenceStore", FakePersistenceContext)
    monkeypatch.setattr(cli, "QuoteLifecycle", RecordingQuoteLifecycle)
    monkeypatch.setattr(cli, "CoincallWsClient", SignalShutdownWsClient)
    monkeypatch.setattr(cli, "MarketDataService", ShutdownRaceMarketData)
    monkeypatch.setattr(cli, "Orchestrator", NoopOrchestrator)
    settings = MakerSettings(
        API_KEY="key",
        API_SECRET="secret",
        CANCEL_ALL_ON_START=False,
        CANCEL_ALL_ON_STOP=True,
    )

    await cli.run_async(settings)

    assert len(RecordingQuoteLifecycle.instances) == 1
    assert RecordingQuoteLifecycle.instances[0].cancel_all_calls == 1


@pytest.mark.asyncio
async def test_taskgroup_failure_without_signal_attempts_cancel_all_and_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    RecordingQuoteLifecycle.instances = []
    RecordingQuoteLifecycle.fail_on_cancel_all = False
    RecordingQuoteLifecycle.cancel_all_exception = None
    monkeypatch.setattr(cli, "CoincallRestClient", FakeRestContext)
    monkeypatch.setattr(cli, "PersistenceStore", FakePersistenceContext)
    monkeypatch.setattr(cli, "QuoteLifecycle", RecordingQuoteLifecycle)
    monkeypatch.setattr(cli, "CoincallWsClient", StopAwareWsClient)
    monkeypatch.setattr(cli, "MarketDataService", FailingMarketData)
    monkeypatch.setattr(cli, "Orchestrator", NoopOrchestrator)
    settings = MakerSettings(
        API_KEY="key",
        API_SECRET="secret",
        CANCEL_ALL_ON_START=False,
        CANCEL_ALL_ON_STOP=True,
    )

    with pytest.raises(ExceptionGroup) as exc_info:
        await cli.run_async(settings)

    assert len(RecordingQuoteLifecycle.instances) == 1
    assert RecordingQuoteLifecycle.instances[0].cancel_all_calls == 1
    assert any(isinstance(exc, RuntimeError) for exc in exc_info.value.exceptions)


@pytest.mark.asyncio
async def test_shutdown_cancel_all_non_coincall_failure_exits_cleanly(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    RecordingQuoteLifecycle.instances = []
    RecordingQuoteLifecycle.fail_on_cancel_all = False
    RecordingQuoteLifecycle.cancel_all_exception = ValueError("malformed cancel-all response")
    monkeypatch.setattr(cli, "CoincallRestClient", FakeRestContext)
    monkeypatch.setattr(cli, "PersistenceStore", FakePersistenceContext)
    monkeypatch.setattr(cli, "QuoteLifecycle", RecordingQuoteLifecycle)
    monkeypatch.setattr(cli, "CoincallWsClient", SignalShutdownWsClient)
    monkeypatch.setattr(cli, "MarketDataService", StopAwareMarketData)
    monkeypatch.setattr(cli, "Orchestrator", NoopOrchestrator)
    settings = MakerSettings(
        API_KEY="key",
        API_SECRET="secret",
        CANCEL_ALL_ON_START=False,
        CANCEL_ALL_ON_STOP=True,
    )

    await cli.run_async(settings)

    assert len(RecordingQuoteLifecycle.instances) == 1
    assert RecordingQuoteLifecycle.instances[0].cancel_all_calls == 1
    stderr = capsys.readouterr().err
    assert "WARNING: failed to cancel all quotes during graceful shutdown" in stderr
    assert "quotes may remain live on the exchange: malformed cancel-all response" in stderr
    RecordingQuoteLifecycle.cancel_all_exception = None


@pytest.mark.asyncio
async def test_kill_switch_flatten_retries_until_cancel_all_succeeds(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(cli, "_FLATTEN_RETRY_INITIAL_SECONDS", 0.001)
    monkeypatch.setattr(cli, "_FLATTEN_RETRY_CAP_SECONDS", 0.001)
    trip_event = asyncio.Event()
    shutdown = asyncio.Event()
    quote_lifecycle = FlakyFlattenQuoteLifecycle()
    trip_event.set()

    with caplog.at_level(logging.ERROR):
        async with asyncio.timeout(1.0):
            await cli._flatten_on_kill_switch(
                trip_event,
                shutdown,
                quote_lifecycle,  # type: ignore[arg-type]
                FakeRiskGate(),  # type: ignore[arg-type]
            )

    assert quote_lifecycle.cancel_all_calls == 3
    assert (
        "Kill-switch flatten completed after 3 consecutive API failures (attempt 3)" in caplog.text
    )


@pytest.mark.asyncio
async def test_kill_switch_flatten_stops_retrying_when_shutdown_is_set(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(cli, "_FLATTEN_RETRY_INITIAL_SECONDS", 0.001)
    monkeypatch.setattr(cli, "_FLATTEN_RETRY_CAP_SECONDS", 0.001)
    trip_event = asyncio.Event()
    shutdown = asyncio.Event()
    quote_lifecycle = ShutdownAfterFailedFlattenQuoteLifecycle(shutdown)
    trip_event.set()

    with caplog.at_level(logging.ERROR):
        async with asyncio.timeout(1.0):
            await cli._flatten_on_kill_switch(
                trip_event,
                shutdown,
                quote_lifecycle,  # type: ignore[arg-type]
                FakeRiskGate(),  # type: ignore[arg-type]
            )

    assert quote_lifecycle.cancel_all_calls == 1
    assert "Kill-switch flatten retry loop stopping for shutdown" in caplog.text


@pytest.mark.asyncio
async def test_kill_switch_flatten_does_not_cancel_without_a_trip() -> None:
    trip_event = asyncio.Event()
    shutdown = asyncio.Event()
    quote_lifecycle = FlakyFlattenQuoteLifecycle()
    shutdown.set()

    async with asyncio.timeout(1.0):
        await cli._flatten_on_kill_switch(
            trip_event,
            shutdown,
            quote_lifecycle,  # type: ignore[arg-type]
            FakeRiskGate(),  # type: ignore[arg-type]
        )

    assert quote_lifecycle.cancel_all_calls == 0


@pytest.mark.asyncio
async def test_quote_refresh_loop_enqueues_reprice_tick() -> None:
    queue: asyncio.Queue[object] = asyncio.Queue()
    shutdown = asyncio.Event()
    task = asyncio.create_task(cli._quote_refresh_loop(queue, shutdown, 0.001))

    event = await asyncio.wait_for(queue.get(), timeout=1.0)
    shutdown.set()
    await asyncio.wait_for(task, timeout=1.0)

    assert isinstance(event, RepriceTick)


@pytest.mark.asyncio
async def test_quote_refresh_loop_exits_cleanly_when_queue_shutdown_races_put() -> None:
    queue = ShutdownOnPutQueue()

    await asyncio.wait_for(
        cli._quote_refresh_loop(queue, asyncio.Event(), 0.001),  # type: ignore[arg-type]
        timeout=1.0,
    )

    assert queue.puts == 1


@pytest.mark.asyncio
async def test_reconcile_loop_enqueues_reconcile_tick() -> None:
    queue: asyncio.Queue[object] = asyncio.Queue()
    shutdown = asyncio.Event()
    task = asyncio.create_task(cli._reconcile_loop(queue, shutdown, 0.001))

    event = await asyncio.wait_for(queue.get(), timeout=1.0)
    shutdown.set()
    await asyncio.wait_for(task, timeout=1.0)

    assert isinstance(event, ReconcileTick)


@pytest.mark.asyncio
async def test_reconcile_loop_exits_cleanly_when_queue_shutdown_races_put() -> None:
    queue = ShutdownOnPutQueue()

    await asyncio.wait_for(
        cli._reconcile_loop(queue, asyncio.Event(), 0.001),  # type: ignore[arg-type]
        timeout=1.0,
    )

    assert queue.puts == 1


@pytest.mark.asyncio
async def test_reconciler_watchdog_logs_stall_without_recording_a_risk_failure(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    shutdown = asyncio.Event()
    orchestrator = WatchdogOrchestrator(last_cycle_ms=800_000, shutdown=shutdown)
    monkeypatch.setattr(cli, "RECONCILE_INTERVAL_SECONDS", 0.001)
    monkeypatch.setattr(cli, "get_timestamp_ms", lambda: 1_000_000)

    with caplog.at_level(logging.ERROR, logger="coincall_rfq_maker.cli"):
        await asyncio.wait_for(cli._reconciler_watchdog(orchestrator, shutdown), timeout=1.0)  # type: ignore[arg-type]

    assert "RECONCILER STALLED" in caplog.text
    assert orchestrator.risk_gate.kill_switch_tripped is False
    assert orchestrator.risk_gate.failures_recorded == 0


@pytest.mark.asyncio
async def test_reconciler_watchdog_is_quiet_for_a_fresh_cycle(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    shutdown = asyncio.Event()
    orchestrator = WatchdogOrchestrator(last_cycle_ms=999_999, shutdown=shutdown)
    monkeypatch.setattr(cli, "RECONCILE_INTERVAL_SECONDS", 0.001)
    monkeypatch.setattr(cli, "get_timestamp_ms", lambda: 1_000_000)

    with caplog.at_level(logging.ERROR, logger="coincall_rfq_maker.cli"):
        await asyncio.wait_for(cli._reconciler_watchdog(orchestrator, shutdown), timeout=1.0)  # type: ignore[arg-type]

    assert "RECONCILER STALLED" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_all_on_start", [False, True])
async def test_run_async_warns_only_when_startup_cancel_all_is_disabled(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    cancel_all_on_start: bool,
) -> None:
    monkeypatch.setattr(cli, "setup_logging", lambda *args: None)
    monkeypatch.setattr(cli, "CoincallRestClient", FakeRestContext)
    monkeypatch.setattr(cli, "PersistenceStore", FakePersistenceContext)
    monkeypatch.setattr(cli, "QuoteLifecycle", RecordingQuoteLifecycle)
    monkeypatch.setattr(cli, "CoincallWsClient", SignalShutdownWsClient)
    monkeypatch.setattr(cli, "MarketDataService", StopAwareMarketData)
    monkeypatch.setattr(cli, "Orchestrator", NoopOrchestrator)
    settings = MakerSettings(
        API_KEY="key",
        API_SECRET="secret",
        CANCEL_ALL_ON_START=cancel_all_on_start,
        CANCEL_ALL_ON_STOP=False,
    )

    with caplog.at_level(logging.WARNING, logger="coincall_rfq_maker.cli"):
        await cli.run_async(settings)

    warning = "CANCEL_ALL_ON_START=false: pre-existing exchange quotes"
    assert (warning in caplog.text) is (not cancel_all_on_start)


@pytest.mark.asyncio
async def test_run_async_drains_outbox_to_sqlite_before_closing_store(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(cli, "setup_logging", lambda *args: None)
    monkeypatch.setattr(cli, "CoincallRestClient", FakeRestContext)
    monkeypatch.setattr(cli, "QuoteLifecycle", RecordingQuoteLifecycle)
    monkeypatch.setattr(cli, "CoincallWsClient", ShutdownAfterAuditWsClient)
    monkeypatch.setattr(cli, "MarketDataService", StopAwareMarketData)
    monkeypatch.setattr(cli, "Orchestrator", AuditEnqueueingOrchestrator)
    AuditEnqueueingOrchestrator.enqueued = asyncio.Event()
    db_path = tmp_path / "audit.db"
    settings = MakerSettings(
        API_KEY="key",
        API_SECRET="secret",
        DB_PATH=str(db_path),
        CANCEL_ALL_ON_START=False,
        CANCEL_ALL_ON_STOP=False,
    )

    async with asyncio.timeout(2.0):
        await cli.run_async(settings)

    async with aiosqlite.connect(db_path) as connection:
        cursor = await connection.execute("SELECT block_trade_id FROM fills ORDER BY id")
        rows = await cursor.fetchall()
    assert rows == [("block-1",)]


@pytest.mark.asyncio
async def test_startup_backfill_tick_is_enqueued_before_timer_events() -> None:
    queue: asyncio.Queue[object] = asyncio.Queue()

    await cli._enqueue_startup_backfill(queue)
    await queue.put(RepriceTick())

    assert isinstance(await queue.get(), ReconcileTick)


@pytest.mark.asyncio
async def test_dispatch_loop_logs_bad_event_and_processes_next() -> None:
    queue: asyncio.Queue[object] = asyncio.Queue()
    orchestrator = FailsFirstOrchestrator()
    first = object()
    second = object()
    await queue.put(first)
    await queue.put(second)

    task = asyncio.create_task(cli._dispatch_loop(queue, orchestrator))  # type: ignore[arg-type]
    await asyncio.wait_for(queue.join(), timeout=1.0)
    queue.shutdown()
    await asyncio.wait_for(task, timeout=1.0)

    assert orchestrator.events == [first, second]


def test_flatten_retry_delay_backs_off_and_stays_capped() -> None:
    # Retries are unbounded, so an uncapped delay would double forever: attempt 20 would
    # wait ~6 days before trying to cancel the live quotes of a halted process.
    assert cli._flatten_retry_delay(1) == cli._FLATTEN_RETRY_INITIAL_SECONDS
    assert cli._flatten_retry_delay(2) == 2 * cli._FLATTEN_RETRY_INITIAL_SECONDS
    assert cli._flatten_retry_delay(3) == 4 * cli._FLATTEN_RETRY_INITIAL_SECONDS
    assert cli._flatten_retry_delay(20) == cli._FLATTEN_RETRY_CAP_SECONDS
    assert cli._flatten_retry_delay(200) == cli._FLATTEN_RETRY_CAP_SECONDS


def test_reconciler_stall_threshold_is_three_intervals() -> None:
    # The tolerated silence is exactly _RECONCILER_STALL_FACTOR intervals. Without this the
    # factor is unpinned: the stall tests use a stamp so old that any factor fires.
    interval = 60.0
    three_intervals_ms = int(3 * interval * 1000)
    now = 10_000_000
    assert not cli._reconciler_is_stalled(now, now - (three_intervals_ms - 1), interval)
    assert not cli._reconciler_is_stalled(now, now - three_intervals_ms, interval)
    assert cli._reconciler_is_stalled(now, now - (three_intervals_ms + 1), interval)
