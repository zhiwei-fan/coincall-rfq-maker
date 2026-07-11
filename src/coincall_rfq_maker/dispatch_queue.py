"""Priority event channel for the maker's single dispatcher."""

import asyncio
from typing import Protocol

from coincall_rfq_maker.events import (
    QuoteUpdated,
    ReconcileTick,
    RepriceTick,
    RfqTerminated,
    TradeExecuted,
)


class EventChannel(Protocol):
    """The queue surface shared by all dispatcher event producers and its consumer."""

    async def put(self, event: object) -> None: ...

    async def get(self) -> object: ...

    def shutdown(self) -> None: ...

    def task_done(self) -> None: ...


class PriorityEventQueue:
    """Two-lane FIFO for the single dispatcher: urgent settlement events preempt
    repricing, and idempotent timer ticks coalesce. Single-consumer.
    """

    URGENT_TYPES: tuple[type, ...] = (RfqTerminated, TradeExecuted, QuoteUpdated)
    COALESCING_TYPES: tuple[type, ...] = (RepriceTick, ReconcileTick)

    def __init__(self) -> None:
        self._urgent: asyncio.Queue[object] = asyncio.Queue()
        self._normal: asyncio.Queue[object] = asyncio.Queue()
        self._pending_coalescing_types: set[type] = set()
        self._urgent_ready = asyncio.Event()
        self._normal_ready = asyncio.Event()
        self._shutdown = False
        # The dispatcher is the sole consumer and calls task_done before its next get.
        self._last_served: asyncio.Queue[object] | None = None

    async def put(self, event: object) -> None:
        """Enqueue an event, dropping duplicate pending level-trigger ticks."""
        if self._shutdown:
            raise asyncio.QueueShutDown

        event_type = type(event)
        if event_type in self.COALESCING_TYPES and event_type in self._pending_coalescing_types:
            return

        queue = self._urgent if isinstance(event, self.URGENT_TYPES) else self._normal
        await queue.put(event)
        if event_type in self.COALESCING_TYPES:
            self._pending_coalescing_types.add(event_type)
        (self._urgent_ready if queue is self._urgent else self._normal_ready).set()

    async def get(self) -> object:
        """Return an urgent event when ready, otherwise the next normal event."""
        while True:
            # Clear before checking both lanes so an enqueue cannot be missed between a check and
            # the subsequent wait. A concurrent put either leaves its event set or supplies an item.
            self._urgent_ready.clear()
            self._normal_ready.clear()
            try:
                return self._serve(self._urgent.get_nowait(), self._urgent)
            except (asyncio.QueueEmpty, asyncio.QueueShutDown):
                pass
            try:
                return self._serve(self._normal.get_nowait(), self._normal)
            except (asyncio.QueueEmpty, asyncio.QueueShutDown):
                pass

            if self._shutdown:
                raise asyncio.QueueShutDown
            urgent_wait = asyncio.create_task(self._urgent_ready.wait())
            normal_wait = asyncio.create_task(self._normal_ready.wait())
            try:
                await asyncio.wait((urgent_wait, normal_wait), return_when=asyncio.FIRST_COMPLETED)
            finally:
                for task in (urgent_wait, normal_wait):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(urgent_wait, normal_wait, return_exceptions=True)

    def shutdown(self) -> None:
        """Reject new events while allowing both lanes to drain."""
        self._shutdown = True
        self._urgent.shutdown()
        self._normal.shutdown()
        self._urgent_ready.set()
        self._normal_ready.set()

    def task_done(self) -> None:
        """Acknowledge the item served by the most recent get (safe for one consumer)."""
        if self._last_served is None:
            raise ValueError("task_done() called too many times")
        self._last_served.task_done()

    def _serve(self, event: object, queue: asyncio.Queue[object]) -> object:
        self._last_served = queue
        event_type = type(event)
        if event_type in self.COALESCING_TYPES:
            self._pending_coalescing_types.discard(event_type)
        return event
