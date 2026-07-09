import asyncio
from typing import Any

import pytest

from coincall_rfq_maker.events import PricesRefreshed
from coincall_rfq_maker.marketdata.service import MarketDataService


class FakeFuturesRest:
    def __init__(self, prices: dict[str, float]) -> None:
        self._prices = prices

    async def get_symbol_info(self, symbol: str | None = None) -> dict[str, Any]:
        assert symbol is not None
        return {"code": 0, "data": {"symbol": symbol, "indexPrice": self._prices[symbol]}}


class BlockingFuturesRest:
    def __init__(self, prices: dict[str, float]) -> None:
        self._prices = prices
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def get_symbol_info(self, symbol: str | None = None) -> dict[str, Any]:
        assert symbol is not None
        self.started.set()
        await self.release.wait()
        return {"code": 0, "data": {"symbol": symbol, "indexPrice": self._prices[symbol]}}


@pytest.mark.asyncio
async def test_first_fetch_emits_prices_refreshed() -> None:
    rest = FakeFuturesRest({"BTCUSD": 50_000.0})
    queue: asyncio.Queue[object] = asyncio.Queue()
    service = MarketDataService(rest, queue, price_move_threshold=0.001)  # type: ignore[arg-type]
    service.track("BTCUSD")

    await service.refresh_once(now_ms=1_000)

    event = queue.get_nowait()
    assert isinstance(event, PricesRefreshed)
    assert event.underlyings == ("BTCUSD",)
    assert service.get_price("BTCUSD") == 50_000.0


@pytest.mark.asyncio
async def test_small_move_below_threshold_does_not_emit() -> None:
    rest = FakeFuturesRest({"BTCUSD": 50_000.0})
    queue: asyncio.Queue[object] = asyncio.Queue()
    service = MarketDataService(  # type: ignore[arg-type]
        rest, queue, price_move_threshold=0.001, force_refresh_seconds=1_000_000
    )
    service.track("BTCUSD")
    await service.refresh_once(now_ms=1_000)
    queue.get_nowait()  # drain initial emission

    rest._prices["BTCUSD"] = 50_010.0  # 0.02% move, below 0.1% threshold
    await service.refresh_once(now_ms=2_000)
    assert queue.empty()


@pytest.mark.asyncio
async def test_move_above_threshold_emits() -> None:
    rest = FakeFuturesRest({"BTCUSD": 50_000.0})
    queue: asyncio.Queue[object] = asyncio.Queue()
    service = MarketDataService(  # type: ignore[arg-type]
        rest, queue, price_move_threshold=0.001, force_refresh_seconds=1_000_000
    )
    service.track("BTCUSD")
    await service.refresh_once(now_ms=1_000)
    queue.get_nowait()

    rest._prices["BTCUSD"] = 50_100.0  # 0.2% move, above 0.1% threshold
    await service.refresh_once(now_ms=2_000)
    event = queue.get_nowait()
    assert isinstance(event, PricesRefreshed)


@pytest.mark.asyncio
async def test_force_refresh_after_elapsed_time_even_without_move() -> None:
    rest = FakeFuturesRest({"BTCUSD": 50_000.0})
    queue: asyncio.Queue[object] = asyncio.Queue()
    service = MarketDataService(  # type: ignore[arg-type]
        rest, queue, price_move_threshold=0.001, force_refresh_seconds=120.0
    )
    service.track("BTCUSD")
    await service.refresh_once(now_ms=0)
    queue.get_nowait()

    await service.refresh_once(now_ms=121_000)  # 121s later, no price change
    event = queue.get_nowait()
    assert isinstance(event, PricesRefreshed)


@pytest.mark.asyncio
async def test_untrack_clears_cached_price() -> None:
    rest = FakeFuturesRest({"BTCUSD": 50_000.0})
    queue: asyncio.Queue[object] = asyncio.Queue()
    service = MarketDataService(rest, queue)  # type: ignore[arg-type]
    service.track("BTCUSD")
    await service.refresh_once(now_ms=1_000)
    service.untrack("BTCUSD")
    await service.refresh_once(now_ms=2_000)
    assert service.get_price("BTCUSD") is None


@pytest.mark.asyncio
async def test_untrack_during_refresh_does_not_resurrect_price() -> None:
    rest = BlockingFuturesRest({"BTCUSD": 50_000.0})
    queue: asyncio.Queue[object] = asyncio.Queue()
    service = MarketDataService(rest, queue)  # type: ignore[arg-type]
    service.track("BTCUSD")

    refresh = asyncio.create_task(service.refresh_once(now_ms=1_000))
    await asyncio.wait_for(rest.started.wait(), timeout=1.0)
    service.untrack("BTCUSD")
    rest.release.set()
    await asyncio.wait_for(refresh, timeout=1.0)

    assert service.get_price("BTCUSD") is None
    assert queue.empty()


@pytest.mark.asyncio
async def test_age_seconds_is_infinite_when_never_priced() -> None:
    rest = FakeFuturesRest({})
    queue: asyncio.Queue[object] = asyncio.Queue()
    service = MarketDataService(rest, queue)  # type: ignore[arg-type]
    assert service.age_seconds("BTCUSD") == float("inf")
