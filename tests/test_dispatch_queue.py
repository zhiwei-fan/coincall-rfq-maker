"""Behavioural tests for dispatcher event scheduling."""

import asyncio

import pytest

from coincall_rfq_maker.dispatch_queue import PriorityEventQueue
from coincall_rfq_maker.events import (
    PricesRefreshed,
    ReconcileTick,
    RepriceTick,
    RfqReceived,
    TradeExecuted,
)


async def _put(queue: PriorityEventQueue, event: object) -> None:
    async with asyncio.timeout(1.0):
        await queue.put(event)


async def _get(queue: PriorityEventQueue) -> object:
    async with asyncio.timeout(1.0):
        return await queue.get()


@pytest.mark.asyncio
async def test_urgent_event_preempts_pending_normal_events() -> None:
    queue = PriorityEventQueue()
    trade = TradeExecuted("trade-1", "quote-1")

    await _put(queue, RepriceTick())
    await _put(queue, PricesRefreshed(("BTCUSD",), 1))
    await _put(queue, trade)

    assert await _get(queue) is trade
    queue.task_done()


@pytest.mark.asyncio
async def test_normal_lane_preserves_fifo_order() -> None:
    queue = PriorityEventQueue()
    first = RfqReceived(rfq=object())
    second = RfqReceived(rfq=object())

    await _put(queue, first)
    await _put(queue, second)

    assert await _get(queue) is first
    queue.task_done()
    assert await _get(queue) is second
    queue.task_done()


@pytest.mark.asyncio
async def test_reprice_ticks_coalesce_until_dequeued_then_accept_a_new_tick() -> None:
    queue = PriorityEventQueue()

    await _put(queue, RepriceTick())
    await _put(queue, RepriceTick())
    await _put(queue, RepriceTick())

    assert isinstance(await _get(queue), RepriceTick)
    queue.task_done()
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.01):
            await queue.get()

    await _put(queue, RepriceTick())
    assert isinstance(await _get(queue), RepriceTick)
    queue.task_done()


@pytest.mark.asyncio
async def test_reconcile_and_reprice_coalesce_independently_without_coalescing_prices() -> None:
    queue = PriorityEventQueue()
    first_prices = PricesRefreshed(("BTCUSD",), 1)
    second_prices = PricesRefreshed(("ETHUSD",), 2)

    await _put(queue, RepriceTick())
    await _put(queue, RepriceTick())
    await _put(queue, first_prices)
    await _put(queue, ReconcileTick())
    await _put(queue, ReconcileTick())
    await _put(queue, second_prices)

    expected = (RepriceTick, first_prices, ReconcileTick, second_prices)
    for expected_event in expected:
        event = await _get(queue)
        if isinstance(expected_event, type):
            assert isinstance(event, expected_event)
        else:
            assert event is expected_event
        queue.task_done()


@pytest.mark.asyncio
async def test_non_coalescing_prices_refreshed_events_are_all_preserved() -> None:
    queue = PriorityEventQueue()
    first = PricesRefreshed(("BTCUSD",), 1)
    second = PricesRefreshed(("BTCUSD",), 2)

    await _put(queue, first)
    await _put(queue, second)

    assert await _get(queue) is first
    queue.task_done()
    assert await _get(queue) is second
    queue.task_done()


@pytest.mark.asyncio
async def test_shutdown_drains_both_lanes_then_get_raises_queue_shutdown() -> None:
    queue = PriorityEventQueue()
    normal = RepriceTick()
    urgent = TradeExecuted("trade-1", "quote-1")

    await _put(queue, normal)
    await _put(queue, urgent)
    queue.shutdown()

    assert await _get(queue) is urgent
    queue.task_done()
    assert await _get(queue) is normal
    queue.task_done()
    async with asyncio.timeout(1.0):
        with pytest.raises(asyncio.QueueShutDown):
            await queue.get()


@pytest.mark.asyncio
async def test_get_blocks_until_a_put_makes_an_event_ready() -> None:
    queue = PriorityEventQueue()
    event = RepriceTick()
    get_task = asyncio.create_task(queue.get())

    async with asyncio.timeout(1.0):
        await asyncio.sleep(0)
    assert not get_task.done()

    await _put(queue, event)
    async with asyncio.timeout(1.0):
        assert await get_task is event
    queue.task_done()
