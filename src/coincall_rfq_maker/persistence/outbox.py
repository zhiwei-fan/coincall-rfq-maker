"""Best-effort asynchronous audit-write outbox."""

import asyncio
import logging
from dataclasses import dataclass
from typing import Literal, cast

from coincall_rfq_maker.domain.quote import Quote
from coincall_rfq_maker.domain.rfq import Rfq
from coincall_rfq_maker.events import TradeExecuted
from coincall_rfq_maker.persistence.store import PersistenceStore

logger = logging.getLogger(__name__)

AUDIT_OUTBOX_CAPACITY = 1000
AUDIT_DRAIN_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True, slots=True)
class _RfqRecord:
    rfq: Rfq
    now_ms: int
    kind: Literal["rfq"] = "rfq"


@dataclass(frozen=True, slots=True)
class _QuoteRecord:
    quote: Quote
    market_snapshot: dict[str, float] | None
    now_ms: int
    kind: Literal["quote"] = "quote"


@dataclass(frozen=True, slots=True)
class _FillRecord:
    event: TradeExecuted
    now_ms: int
    kind: Literal["fill"] = "fill"


_AuditRecord = _RfqRecord | _QuoteRecord | _FillRecord


class AuditOutbox:
    """Single-producer dispatcher and single-consumer writer audit outbox.

    Audit persistence is non-authoritative: its deliberate degradation must never
    interrupt trading-state transitions.
    """

    def __init__(self, store: PersistenceStore, capacity: int = AUDIT_OUTBOX_CAPACITY) -> None:
        self._store = store
        self._queue: asyncio.Queue[_AuditRecord] = asyncio.Queue(maxsize=capacity)
        self._dropped_total = 0
        self._in_flight = 0

    def enqueue_rfq(self, rfq: Rfq, now_ms: int) -> None:
        self._enqueue(_RfqRecord(rfq, now_ms))

    def enqueue_quote(
        self, quote: Quote, market_snapshot: dict[str, float] | None, now_ms: int
    ) -> None:
        self._enqueue(_QuoteRecord(quote, market_snapshot, now_ms))

    def enqueue_fill(self, event: TradeExecuted, now_ms: int) -> None:
        self._enqueue(_FillRecord(event, now_ms))

    def _enqueue(self, record: _AuditRecord) -> None:
        try:
            self._queue.put_nowait(record)
        except asyncio.QueueFull:
            self._dropped_total += 1
            logger.error(
                "audit outbox full; dropping %s record (dropped_total=%d)",
                record.kind,
                self._dropped_total,
            )
        except asyncio.QueueShutDown:
            self._dropped_total += 1
            logger.error(
                "audit outbox shut down; dropping %s record (dropped_total=%d)",
                record.kind,
                self._dropped_total,
            )

    async def run(self, shutdown: asyncio.Event) -> None:
        """Write queued records until shutdown closes and drains the queue."""
        shutdown_wait = asyncio.create_task(shutdown.wait())
        get_task = asyncio.create_task(self._queue.get())
        queue_shutdown = False
        try:
            while True:
                if queue_shutdown:
                    done, _ = await asyncio.wait((get_task,), return_when=asyncio.FIRST_COMPLETED)
                else:
                    done, _ = await asyncio.wait(
                        (get_task, cast("asyncio.Task[_AuditRecord]", shutdown_wait)),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                if not queue_shutdown and shutdown_wait in done:
                    self._queue.shutdown()
                    queue_shutdown = True
                if get_task not in done:
                    continue
                try:
                    record = get_task.result()
                except asyncio.QueueShutDown:
                    return
                try:
                    self._in_flight += 1
                    await self._write(record)
                except Exception:
                    logger.exception("Failed to write audit %s record", record.kind)
                finally:
                    self._in_flight -= 1
                    self._queue.task_done()
                get_task = asyncio.create_task(self._queue.get())
        finally:
            for task in (shutdown_wait, get_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(shutdown_wait, get_task, return_exceptions=True)

    async def _write(self, record: _AuditRecord) -> None:
        match record:
            case _RfqRecord(rfq=rfq, now_ms=now_ms):
                await self._store.record_rfq(rfq, now_ms)
            case _QuoteRecord(quote=quote, market_snapshot=snapshot, now_ms=now_ms):
                await self._store.record_quote(quote, snapshot, now_ms)
            case _FillRecord(event=event, now_ms=now_ms):
                await self._store.record_fill(event, now_ms)

    async def drain(self) -> None:
        """Wait briefly for queued and in-flight audit records to finish writing."""
        try:
            async with asyncio.timeout(AUDIT_DRAIN_TIMEOUT_SECONDS):
                await self._queue.join()
        except TimeoutError:
            logger.error(
                "audit outbox drain timed out; %d unwritten records",
                self._queue.qsize() + self._in_flight,
            )
