"""Named taker scenarios that exercise the maker end to end."""

import asyncio
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol, cast, runtime_checkable

from coincall_rfq_maker.adapters.rest import CoincallRestClient
from coincall_rfq_maker.domain.quote import QuoteStage
from coincall_rfq_maker.domain.rfq import RfqLeg, Side
from coincall_rfq_maker.marketdata.service import MarketDataService
from coincall_rfq_maker.orchestration import Orchestrator
from coincall_rfq_maker.persistence.store import PersistenceStore
from coincall_rfq_maker.pricing.engine import BlackScholesModel
from coincall_rfq_maker.quoting.lifecycle import QuoteLifecycle
from coincall_rfq_maker.risk.gate import NullExposureProvider, RiskGate
from coincall_rfq_maker.testing.fake_exchange import FakeExchange

SCENARIO_NAMES = (
    "quote_new_rfq",
    "requote_on_price_move",
    "taker_executes",
    "rfq_cancelled",
)

_FIXTURE_EXPIRY_DAYS = 365
_MONTH_CODES = (
    "JAN",
    "FEB",
    "MAR",
    "APR",
    "MAY",
    "JUN",
    "JUL",
    "AUG",
    "SEP",
    "OCT",
    "NOV",
    "DEC",
)


class ScenarioFailure(Exception):
    """Raised when a taker scenario observes an unexpected maker state."""


class TakerOperations(Protocol):
    async def create_rfq(self, legs: tuple[RfqLeg, ...]) -> str: ...
    async def execute_quote(self, quote_id: str) -> str: ...
    async def cancel_rfq(self, request_id: str) -> None: ...


@runtime_checkable
class PriceController(Protocol):
    def set_underlying_price(self, underlying: str, price: float) -> None: ...


@dataclass(slots=True)
class AssertionContext:
    events: "asyncio.Queue[object]"
    orchestrator: Orchestrator
    market_data: MarketDataService
    quote_lifecycle: QuoteLifecycle
    persistence: PersistenceStore


@dataclass(slots=True)
class FakeHarness:
    exchange: FakeExchange
    context: AssertionContext


@asynccontextmanager
async def fake_harness(db_path: str) -> AsyncIterator[FakeHarness]:
    events: asyncio.Queue[object] = asyncio.Queue()
    exchange = FakeExchange(events, {"BTCUSD": 100_000.0})
    rest = cast(CoincallRestClient, exchange)

    async with PersistenceStore(db_path) as persistence:
        market_data = MarketDataService(rest, events, price_move_threshold=0.001)
        pricing_model = BlackScholesModel(bid_vol=0.20, ask_vol=2.00, risk_free_rate=0.05)
        risk_gate = RiskGate(
            max_quote_notional_usd=1_000_000.0,
            max_leg_qty=100.0,
            min_time_to_expiry_hours=1.0,
            stale_market_data_seconds=30.0,
            exposure_provider=NullExposureProvider(),
        )
        quote_lifecycle = QuoteLifecycle(rest, dry_run=False)
        orchestrator = Orchestrator(
            rest,
            market_data,
            pricing_model,
            risk_gate,
            quote_lifecycle,
            persistence,
        )
        yield FakeHarness(
            exchange=exchange,
            context=AssertionContext(
                events=events,
                orchestrator=orchestrator,
                market_data=market_data,
                quote_lifecycle=quote_lifecycle,
                persistence=persistence,
            ),
        )


async def run_scenario(name: str, taker: TakerOperations, context: AssertionContext) -> None:
    if name == "quote_new_rfq":
        await _quote_new_rfq(taker, context)
    elif name == "requote_on_price_move":
        if not isinstance(taker, PriceController):
            raise ScenarioFailure("requote_on_price_move requires price-control support")
        await _requote_on_price_move(taker, context)
    elif name == "taker_executes":
        await _taker_executes(taker, context)
    elif name == "rfq_cancelled":
        await _rfq_cancelled(taker, context)
    else:
        raise ValueError(f"Unknown scenario {name!r}")


async def _quote_new_rfq(taker: TakerOperations, context: AssertionContext) -> None:
    request_id = await _create_and_quote(taker, context)
    quote = _require(
        context.quote_lifecycle.get_for_rfq(request_id),
        f"RFQ {request_id} has no local quote",
    )
    _check(quote.stage is QuoteStage.OPEN, f"quote for RFQ {request_id} is {quote.stage}")
    _check(quote.quote_id is not None, f"quote for RFQ {request_id} has no quote_id")


async def _requote_on_price_move(taker: TakerOperations, context: AssertionContext) -> None:
    price_controller = cast(PriceController, taker)
    request_id = await _create_and_quote(taker, context)
    first = _require(
        context.quote_lifecycle.get_for_rfq(request_id),
        f"RFQ {request_id} has no first quote",
    )
    first_quote_id = _require(first.quote_id, f"first quote for RFQ {request_id} has no quote_id")
    _check(first.stage is QuoteStage.OPEN, f"first quote for RFQ {request_id} is {first.stage}")

    price_controller.set_underlying_price("BTCUSD", 125_000.0)
    await _refresh_and_drain(context)

    second = _require(
        context.quote_lifecycle.get_for_rfq(request_id),
        f"RFQ {request_id} has no second quote",
    )
    second_quote_id = _require(
        second.quote_id,
        f"second quote for RFQ {request_id} has no quote_id",
    )
    _check(second.stage is QuoteStage.OPEN, f"second quote for RFQ {request_id} is {second.stage}")
    _check(
        second_quote_id != first_quote_id,
        f"requote reused quote_id {second_quote_id} for RFQ {request_id}",
    )
    _check(
        second.legs[0].price != first.legs[0].price,
        f"requote kept price {second.legs[0].price} for RFQ {request_id}",
    )

    old = _require(
        context.quote_lifecycle.get_by_quote_id(first_quote_id),
        f"old quote {first_quote_id} missing from lifecycle",
    )
    _check(old.stage is QuoteStage.CANCELLED, f"old quote {first_quote_id} is {old.stage}")


async def _taker_executes(taker: TakerOperations, context: AssertionContext) -> None:
    request_id = await _create_and_quote(taker, context)
    quote = _require(
        context.quote_lifecycle.get_for_rfq(request_id),
        f"RFQ {request_id} has no executable quote",
    )
    quote_id = _require(quote.quote_id, f"quote for RFQ {request_id} has no quote_id")

    block_trade_id = await taker.execute_quote(quote_id)
    await _drain_events(context)

    filled = _require(
        context.quote_lifecycle.get_for_rfq(request_id),
        f"RFQ {request_id} has no filled quote",
    )
    _check(filled.stage is QuoteStage.FILLED, f"quote {quote_id} is {filled.stage}")
    _check(
        filled.filled_price == quote.legs[0].price,
        f"quote {quote_id} filled at {filled.filled_price}, expected {quote.legs[0].price}",
    )
    _check(filled.filled_quantity == 1.0, f"quote {quote_id} filled {filled.filled_quantity}")
    _check(
        filled.block_trade_id == block_trade_id,
        f"quote {quote_id} block trade {filled.block_trade_id}, expected {block_trade_id}",
    )

    quote_history = await context.persistence.fetch_quote_history(request_id)
    fill_history = await context.persistence.fetch_fills()
    _check(
        quote_history[-1]["stage"] == QuoteStage.FILLED.value,
        f"last quote history stage is {quote_history[-1]['stage']}",
    )
    _check(
        fill_history[-1]["block_trade_id"] == block_trade_id,
        f"last fill block trade is {fill_history[-1]['block_trade_id']}",
    )
    _check(
        fill_history[-1]["quote_id"] == quote_id,
        f"last fill quote is {fill_history[-1]['quote_id']}",
    )
    _check(
        fill_history[-1]["request_id"] == request_id,
        f"last fill RFQ is {fill_history[-1]['request_id']}",
    )


async def _rfq_cancelled(taker: TakerOperations, context: AssertionContext) -> None:
    request_id = await _create_and_quote(taker, context)
    quote = _require(
        context.quote_lifecycle.get_for_rfq(request_id),
        f"RFQ {request_id} has no cancellable quote",
    )
    quote_id = _require(quote.quote_id, f"quote for RFQ {request_id} has no quote_id")

    await taker.cancel_rfq(request_id)
    await _drain_events(context)

    _check(
        context.orchestrator.rfq_store.get(request_id) is None,
        f"cancelled RFQ {request_id} was not evicted",
    )
    cancelled = _require(
        context.quote_lifecycle.get_by_quote_id(quote_id),
        f"cancelled quote {quote_id} missing from lifecycle",
    )
    _check(cancelled.stage is QuoteStage.CANCELLED, f"quote {quote_id} is {cancelled.stage}")


async def _create_and_quote(taker: TakerOperations, context: AssertionContext) -> str:
    request_id = await taker.create_rfq(_default_legs())
    await _refresh_and_drain(context)
    return request_id


async def _refresh_and_drain(context: AssertionContext) -> None:
    await _drain_events(context)
    await context.market_data.refresh_once()
    await _drain_events(context)


async def _drain_events(context: AssertionContext) -> None:
    while True:
        try:
            event = context.events.get_nowait()
        except asyncio.QueueEmpty:
            return
        try:
            await context.orchestrator.handle_event(event)
        finally:
            context.events.task_done()


def _default_legs() -> tuple[RfqLeg, ...]:
    return (RfqLeg(instrument_name=_fixture_instrument(), side=Side.BUY, quantity="1"),)


def scenario_names(selected: str) -> Sequence[str]:
    return SCENARIO_NAMES if selected == "all" else (selected,)


def _fixture_instrument(now: datetime | None = None) -> str:
    reference = now if now is not None else datetime.now(UTC)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=UTC)
    expiry = reference + timedelta(days=_FIXTURE_EXPIRY_DAYS)
    expiry_code = f"{expiry.day:02d}{_MONTH_CODES[expiry.month - 1]}{expiry.year % 100:02d}"
    return f"BTCUSD-{expiry_code}-100000-C"


def _check(condition: bool, message: str) -> None:
    if not condition:
        raise ScenarioFailure(message)


def _require[T](value: T | None, message: str) -> T:
    if value is None:
        raise ScenarioFailure(message)
    return value
