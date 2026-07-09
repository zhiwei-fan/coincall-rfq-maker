"""Underlying price cache actor.

Single-writer owner of the tracked-underlying set and price cache: `track`
and `untrack` only enqueue commands, and the service run loop drains those
commands before mutating `_tracked`, `_prices`, or related bookkeeping. Emits
`PricesRefreshed` when a tracked underlying moves past `price_move_threshold`
or `force_refresh_seconds` has elapsed since the last emission for it —
behavior salvaged from the old `main.py` design notes ("Refresh all pricing
on every 0.1% move, or 2 minutes elapsed").
"""

import asyncio
import logging
import time
from dataclasses import dataclass

from coincall_rfq_maker.adapters.rest import CoincallRestClient
from coincall_rfq_maker.adapters.schemas import SymbolInfoPayload
from coincall_rfq_maker.events import PricesRefreshed

logger = logging.getLogger(__name__)

FORCE_REFRESH_SECONDS = 120.0


@dataclass(frozen=True, slots=True)
class PriceSnapshot:
    price: float
    fetched_at_ms: int


@dataclass(frozen=True, slots=True)
class _TrackUnderlying:
    underlying: str


@dataclass(frozen=True, slots=True)
class _UntrackUnderlying:
    underlying: str


_Command = _TrackUnderlying | _UntrackUnderlying


class MarketDataService:
    """Fetches and caches underlying index/mark prices for tracked instruments."""

    def __init__(
        self,
        rest_client: CoincallRestClient,
        event_queue: "asyncio.Queue[object]",
        price_move_threshold: float = 0.001,
        force_refresh_seconds: float = FORCE_REFRESH_SECONDS,
    ) -> None:
        self._rest = rest_client
        self._queue = event_queue
        self._threshold = price_move_threshold
        self._force_refresh_seconds = force_refresh_seconds
        self._prices: dict[str, PriceSnapshot] = {}
        self._tracked: set[str] = set()
        self._last_emitted_ms: dict[str, int] = {}
        self._commands: asyncio.Queue[_Command] = asyncio.Queue()

    def track(self, underlying: str) -> None:
        self._commands.put_nowait(_TrackUnderlying(underlying))

    def untrack(self, underlying: str) -> None:
        self._commands.put_nowait(_UntrackUnderlying(underlying))

    def get_price(self, underlying: str) -> float | None:
        snapshot = self._prices.get(underlying)
        return snapshot.price if snapshot else None

    def age_seconds(self, underlying: str, now_ms: int | None = None) -> float:
        """Seconds since this underlying was last successfully priced (inf if never)."""
        snapshot = self._prices.get(underlying)
        if snapshot is None:
            return float("inf")
        now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
        return (now_ms - snapshot.fetched_at_ms) / 1000

    async def refresh_once(self, now_ms: int | None = None) -> None:
        """Fetch prices for all tracked underlyings; emit PricesRefreshed for movers."""
        self._drain_commands()
        if not self._tracked:
            return
        now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
        moved: list[str] = []

        for underlying in tuple(self._tracked):
            if underlying not in self._tracked:
                continue
            try:
                response = await self._rest.get_symbol_info(underlying)
                info = SymbolInfoPayload.model_validate(response.get("data") or {})
            except Exception:
                logger.exception("Failed to fetch symbol info for %s", underlying)
                self._drain_commands()
                continue

            self._drain_commands()
            if underlying not in self._tracked:
                continue

            price = info.underlying_price
            if price is None:
                logger.warning("No index/mark price in symbol info for %s", underlying)
                continue

            previous = self._prices.get(underlying)
            self._prices[underlying] = PriceSnapshot(price=price, fetched_at_ms=now_ms)

            if previous is None:
                moved.append(underlying)
                continue

            moved_fraction = abs(price - previous.price) / previous.price if previous.price else 1.0
            last_emitted = self._last_emitted_ms.get(underlying, previous.fetched_at_ms)
            elapsed = (now_ms - last_emitted) / 1000
            if moved_fraction >= self._threshold or elapsed >= self._force_refresh_seconds:
                moved.append(underlying)

        if moved:
            self._drain_commands()
            moved = [underlying for underlying in moved if underlying in self._tracked]
        if moved:
            for underlying in moved:
                self._last_emitted_ms[underlying] = now_ms
            try:
                await self._queue.put(
                    PricesRefreshed(underlyings=tuple(moved), timestamp_ms=now_ms)
                )
            except asyncio.QueueShutDown:
                logger.debug("Market-data event queue shut down; stopping refresh emission")

    async def run(self, shutdown: asyncio.Event, interval_seconds: float) -> None:
        """Refresh on a fixed interval until `shutdown` is set."""
        while not shutdown.is_set():
            await self.refresh_once()
            try:
                async with asyncio.timeout(interval_seconds):
                    await shutdown.wait()
                return
            except TimeoutError:
                continue

    def _drain_commands(self) -> None:
        while True:
            try:
                command = self._commands.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                match command:
                    case _TrackUnderlying(underlying=underlying):
                        self._tracked.add(underlying)
                    case _UntrackUnderlying(underlying=underlying):
                        self._tracked.discard(underlying)
                        self._prices.pop(underlying, None)
                        self._last_emitted_ms.pop(underlying, None)
            finally:
                self._commands.task_done()
