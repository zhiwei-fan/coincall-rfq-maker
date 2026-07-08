"""Typed events flowing through the internal `asyncio.Queue`s.

The WS reader only ever produces these (parse -> enqueue); it never mutates
service state directly. Consumers (RFQ store, quote lifecycle actor, trade
handler, market data actor) own their state and react to events.
"""

from dataclasses import dataclass

from coincall_rfq_maker.domain.quote import QuoteStage
from coincall_rfq_maker.domain.rfq import Rfq, RfqStatus


@dataclass(frozen=True, slots=True)
class RfqReceived:
    """A new or updated active RFQ arrived over the WS."""

    rfq: Rfq


@dataclass(frozen=True, slots=True)
class RfqTerminated:
    """An RFQ moved to a terminal exchange status (cancelled/expired/filled/traded away)."""

    request_id: str
    status: RfqStatus


@dataclass(frozen=True, slots=True)
class QuoteUpdated:
    """The exchange reported a state change for one of our quotes."""

    quote_id: str
    request_id: str
    stage: QuoteStage
    filled_price: float | None = None
    filled_quantity: float | None = None
    fill_time_ms: int | None = None
    block_trade_id: str | None = None


@dataclass(frozen=True, slots=True)
class TradeExecuted:
    """A block trade filled against one of our quotes."""

    block_trade_id: str
    quote_id: str
    request_id: str | None = None


@dataclass(frozen=True, slots=True)
class PricesRefreshed:
    """Underlying prices moved enough (or enough time passed) to warrant repricing."""

    underlyings: tuple[str, ...]
    timestamp_ms: int
