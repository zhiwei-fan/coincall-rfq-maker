import asyncio
from types import TracebackType
from typing import Any, ClassVar, Self

import pytest

from coincall_rfq_maker import cli
from coincall_rfq_maker.adapters.rest import CoincallRequestError
from coincall_rfq_maker.events import ReconcileTick, RepriceTick
from coincall_rfq_maker.settings import Settings


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
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    async def handle_event(self, event: object) -> None:
        pass


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
    settings = Settings(API_KEY="key", API_SECRET="secret", CANCEL_ALL_ON_START=True)

    with pytest.raises(SystemExit) as exc_info:
        await cli.run_async(settings)

    assert exc_info.value.code == 1
    assert capsys.readouterr().err == (
        "Startup error: failed to cancel all quotes: cancel-all unavailable\n"
    )


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
    settings = Settings(
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
    settings = Settings(
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
    settings = Settings(
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
    settings = Settings(
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
    settings = Settings(
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
