import asyncio
from types import TracebackType
from typing import Any, Self

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


class ShutdownOnPutQueue:
    def __init__(self) -> None:
        self.puts = 0

    async def put(self, event: object) -> None:
        self.puts += 1
        raise asyncio.QueueShutDown


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
