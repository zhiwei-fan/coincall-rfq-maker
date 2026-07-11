import asyncio
import logging

import pytest

from coincall_rfq_maker.domain.quote import Quote, QuoteLeg, QuoteStage
from coincall_rfq_maker.domain.rfq import Rfq, RfqLeg, RfqStatus, Side
from coincall_rfq_maker.events import TradeExecuted
from coincall_rfq_maker.orchestration import Orchestrator
from coincall_rfq_maker.persistence import outbox as outbox_module
from coincall_rfq_maker.persistence.outbox import (
    AUDIT_DRAIN_TIMEOUT_SECONDS,
    AUDIT_OUTBOX_CAPACITY,
    AuditOutbox,
)

INSTRUMENT = "BTCUSD-21AUG25-120000-C"


def make_rfq(request_id: str = "rfq-1") -> Rfq:
    return Rfq(
        request_id=request_id,
        status=RfqStatus.ACTIVE,
        legs=(RfqLeg(instrument_name=INSTRUMENT, side=Side.BUY, quantity="1"),),
        create_time_ms=0,
        expiry_time_ms=1_000,
    )


def make_quote(request_id: str = "rfq-1") -> Quote:
    return Quote(
        request_id=request_id,
        stage=QuoteStage.OPEN,
        legs=(QuoteLeg(instrument_name=INSTRUMENT, price=100.0),),
        create_time_ms=0,
        quote_id=f"quote-{request_id}",
    )


def make_fill() -> TradeExecuted:
    return TradeExecuted(
        block_trade_id="block-1",
        quote_id="quote-rfq-1",
        request_id="rfq-1",
        filled_price=100.0,
        filled_quantity=1.0,
        fill_time_ms=1,
    )


class RecordingStore:
    def __init__(self, poison_request_id: str | None = None) -> None:
        self.records: list[str] = []
        self.poison_request_id = poison_request_id

    async def record_rfq(self, rfq: Rfq, now_ms: int) -> None:
        self.records.append(f"rfq:{rfq.request_id}")

    async def record_quote(
        self, quote: Quote, market_snapshot: dict[str, float] | None, now_ms: int
    ) -> None:
        self.records.append(f"quote:{quote.request_id}")
        if quote.request_id == self.poison_request_id:
            raise RuntimeError("poison quote")

    async def record_fill(self, event: TradeExecuted, now_ms: int) -> None:
        self.records.append(f"fill:{event.request_id}")


async def stop_writer(task: asyncio.Task[None], shutdown: asyncio.Event) -> None:
    shutdown.set()
    async with asyncio.timeout(1.0):
        await task


@pytest.mark.asyncio
async def test_writer_continues_after_poison_record(caplog: pytest.LogCaptureFixture) -> None:
    store = RecordingStore(poison_request_id="poison")
    outbox = AuditOutbox(store)  # type: ignore[arg-type]
    shutdown = asyncio.Event()
    writer = asyncio.create_task(outbox.run(shutdown))
    outbox.enqueue_rfq(make_rfq("good-first"), 1)
    outbox.enqueue_quote(make_quote("poison"), None, 2)
    outbox.enqueue_fill(make_fill(), 3)

    with caplog.at_level(logging.ERROR, logger="coincall_rfq_maker.persistence.outbox"):
        async with asyncio.timeout(1.0):
            await outbox.drain()

    await stop_writer(writer, shutdown)

    assert store.records == ["rfq:good-first", "quote:poison", "fill:rfq-1"]
    assert "Failed to write audit quote record" in caplog.text


@pytest.mark.asyncio
async def test_writer_preserves_enqueue_order() -> None:
    store = RecordingStore()
    outbox = AuditOutbox(store)  # type: ignore[arg-type]
    shutdown = asyncio.Event()
    writer = asyncio.create_task(outbox.run(shutdown))
    outbox.enqueue_rfq(make_rfq(), 1)
    outbox.enqueue_quote(make_quote(), None, 2)
    outbox.enqueue_fill(make_fill(), 3)

    async with asyncio.timeout(1.0):
        await outbox.drain()
    await stop_writer(writer, shutdown)

    assert store.records == ["rfq:rfq-1", "quote:rfq-1", "fill:rfq-1"]


@pytest.mark.asyncio
async def test_full_outbox_drops_without_blocking_dispatcher(
    caplog: pytest.LogCaptureFixture,
) -> None:
    assert AUDIT_OUTBOX_CAPACITY == 1000
    outbox = AuditOutbox(RecordingStore(), capacity=3)  # type: ignore[arg-type]
    outbox.enqueue_rfq(make_rfq("one"), 1)
    outbox.enqueue_rfq(make_rfq("two"), 2)
    outbox.enqueue_rfq(make_rfq("three"), 3)
    orchestrator = Orchestrator(
        rest_client=object(),  # type: ignore[arg-type]
        market_data=object(),  # type: ignore[arg-type]
        pricing_model=object(),  # type: ignore[arg-type]
        risk_gate=object(),  # type: ignore[arg-type]
        quote_lifecycle=object(),  # type: ignore[arg-type]
        instrument_catalog=object(),  # type: ignore[arg-type]
        audit_outbox=outbox,
    )

    with caplog.at_level(logging.ERROR, logger="coincall_rfq_maker.persistence.outbox"):
        async with asyncio.timeout(0.1):
            await orchestrator.handle_event(make_fill())

    assert "audit outbox full; dropping fill record (dropped_total=1)" in caplog.text


class HangingStore(RecordingStore):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()

    async def record_rfq(self, rfq: Rfq, now_ms: int) -> None:
        self.started.set()
        await asyncio.Event().wait()


@pytest.mark.asyncio
async def test_drain_is_bounded_when_store_hangs(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    assert AUDIT_DRAIN_TIMEOUT_SECONDS == 5.0
    monkeypatch.setattr(outbox_module, "AUDIT_DRAIN_TIMEOUT_SECONDS", 0.01)
    store = HangingStore()
    outbox = AuditOutbox(store)  # type: ignore[arg-type]
    writer = asyncio.create_task(outbox.run(asyncio.Event()))
    outbox.enqueue_rfq(make_rfq(), 1)
    async with asyncio.timeout(1.0):
        await store.started.wait()

    with caplog.at_level(logging.ERROR, logger="coincall_rfq_maker.persistence.outbox"):
        async with asyncio.timeout(0.1):
            await outbox.drain()

    writer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await writer
    assert "audit outbox drain timed out; 1 unwritten records" in caplog.text
