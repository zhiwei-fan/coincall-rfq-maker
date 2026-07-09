from typing import Any

import pytest

from coincall_rfq_maker.adapters.rest import (
    CoincallAmbiguousError,
    CoincallApiError,
    CoincallRequestError,
)
from coincall_rfq_maker.domain.quote import Quote, QuoteStage
from coincall_rfq_maker.domain.rfq import Rfq, RfqLeg, RfqStatus, Side
from coincall_rfq_maker.events import QuoteUpdated, ReconcileTick, RepriceTick, RfqTerminated
from coincall_rfq_maker.orchestration import Orchestrator
from coincall_rfq_maker.pricing.engine import LegPrice
from coincall_rfq_maker.quoting.lifecycle import QuoteLifecycle
from coincall_rfq_maker.quoting.strategy import QuoteIntent, QuoteLegIntent
from coincall_rfq_maker.risk.gate import RiskDecision, RiskGate

INSTRUMENT = "BTCUSD-21AUG25-120000-C"


class FakeRest:
    def __init__(self, open_request_ids: list[str] | None = None) -> None:
        self._open_request_ids = open_request_ids
        self.get_rfq_error: Exception | None = None
        self.create_calls: list[tuple[str, list[dict[str, str]]]] = []
        self.cancel_calls: list[str] = []
        self.create_error: Exception | None = None
        self.cancel_error: Exception | None = None
        self.quote_list_response: dict[str, Any] = {"code": 0, "data": []}
        self.quote_list_calls: list[dict[str, Any]] = []
        self._next_quote_id = 1

    async def get_rfq_list(self, **kwargs: Any) -> dict[str, Any]:
        if self.get_rfq_error is not None:
            raise self.get_rfq_error
        return {
            "code": 0,
            "data": {"rfqList": [{"requestId": rid} for rid in self._open_request_ids or []]},
        }

    async def create_quote(self, request_id: str, legs: list[dict[str, str]]) -> dict[str, Any]:
        self.create_calls.append((request_id, legs))
        if self.create_error is not None:
            raise self.create_error
        quote_id = f"q-{self._next_quote_id}"
        self._next_quote_id += 1
        return {"code": 0, "data": {"quoteId": quote_id}}

    async def cancel_quote(self, quote_id: str) -> dict[str, Any]:
        self.cancel_calls.append(quote_id)
        if self.cancel_error is not None:
            raise self.cancel_error
        return {"code": 0, "data": {}}

    async def get_quote_list(self, **kwargs: Any) -> dict[str, Any]:
        self.quote_list_calls.append(kwargs)
        return self.quote_list_response


class FakeMarketData:
    def __init__(self, price: float | None = None) -> None:
        self.tracked: set[str] = set()
        self.price = price

    def track(self, underlying: str) -> None:
        self.tracked.add(underlying)

    def untrack(self, underlying: str) -> None:
        self.tracked.discard(underlying)

    def get_price(self, underlying: str) -> float | None:
        return self.price

    def age_seconds(self, underlying: str) -> float:
        return 0.0


class FakeQuoteLifecycle:
    def __init__(self) -> None:
        self.cancelled_for: list[str] = []
        self.reconcile_calls = 0
        self.reconcile_error: Exception | None = None

    async def cancel_for_rfq(self, request_id: str) -> None:
        self.cancelled_for.append(request_id)

    async def reconcile(self, intent: object) -> object:
        self.reconcile_calls += 1
        if self.reconcile_error is not None:
            raise self.reconcile_error
        return object()

    def get_by_quote_id(self, quote_id: str) -> None:
        return None

    def apply_ws_update(self, event: QuoteUpdated) -> None:
        return None

    def has_open_or_pending_quote_for_rfq(self, request_id: str) -> bool:
        return False

    def consume_ambiguous_transport_failures(self) -> int:
        return 0


class RecordingPersistence:
    def __init__(self) -> None:
        self.quotes: list[Quote] = []

    async def record_rfq(self, *args: object, **kwargs: object) -> None:
        pass

    async def record_quote(
        self, quote: Quote, market_snapshot: dict[str, float] | None, now_ms: int
    ) -> None:
        self.quotes.append(quote)


class FakePricingModel:
    def __init__(self, ask: float = 100.0) -> None:
        self.ask = ask

    def price(self, *args: object, **kwargs: object) -> LegPrice:
        return LegPrice(bid=self.ask - 1.0, ask=self.ask)


class RecordingRiskGate:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def evaluate(self, *args: object, **kwargs: object) -> RiskDecision:
        return RiskDecision(approved=True)

    def record_api_failure(self) -> None:
        self.calls.append("failure")

    def record_api_success(self) -> None:
        self.calls.append("success")


def make_rfq(request_id: str) -> Rfq:
    return Rfq(
        request_id=request_id,
        status=RfqStatus.ACTIVE,
        legs=(RfqLeg(instrument_name=INSTRUMENT, side=Side.BUY, quantity="1"),),
        create_time_ms=0,
        expiry_time_ms=4_000_000_000_000,
    )


def make_intent(request_id: str = "rfq-1", price: float = 100.0) -> QuoteIntent:
    return QuoteIntent(
        request_id=request_id,
        legs=(QuoteLegIntent(instrument_name=INSTRUMENT, price=price),),
    )


@pytest.mark.asyncio
async def test_reconciler_marks_locally_active_rfq_terminal_when_exchange_disagrees() -> None:
    rest = FakeRest(open_request_ids=["rfq-1"])  # exchange only knows about rfq-1
    market_data = FakeMarketData()
    quotes = FakeQuoteLifecycle()
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=None,  # type: ignore[arg-type]
        risk_gate=None,  # type: ignore[arg-type]
        quote_lifecycle=quotes,  # type: ignore[arg-type]
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))
    orchestrator.rfq_store.upsert(make_rfq("rfq-2"))  # exchange has already dropped this one

    await orchestrator.reconcile_with_exchange()

    assert orchestrator.rfq_store.get("rfq-1").is_terminal_status is False  # type: ignore[union-attr]
    assert orchestrator.rfq_store.get("rfq-2") is None
    assert quotes.cancelled_for == ["rfq-2"]


@pytest.mark.asyncio
async def test_reconciler_converges_when_exchange_matches_local_state() -> None:
    rest = FakeRest(open_request_ids=["rfq-1"])
    market_data = FakeMarketData()
    quotes = FakeQuoteLifecycle()
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=None,  # type: ignore[arg-type]
        risk_gate=None,  # type: ignore[arg-type]
        quote_lifecycle=quotes,  # type: ignore[arg-type]
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))

    await orchestrator.reconcile_with_exchange()

    assert quotes.cancelled_for == []
    assert orchestrator.rfq_store.get("rfq-1").is_terminal_status is False  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_cancel_failure_reverts_quote_open_skips_replacement_and_records_failure() -> None:
    rest = FakeRest()
    market_data = FakeMarketData(price=50_000.0)
    pricing_model = FakePricingModel(ask=100.0)
    risk_gate = RiskGate(
        max_quote_notional_usd=1_000_000.0,
        max_leg_qty=100.0,
        min_time_to_expiry_hours=0.0,
        stale_market_data_seconds=30.0,
        kill_switch_threshold=1,
    )
    quotes = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=pricing_model,  # type: ignore[arg-type]
        risk_gate=risk_gate,
        quote_lifecycle=quotes,
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))
    await orchestrator.reprice_all_active()
    first = quotes.get_for_rfq("rfq-1")
    assert first is not None

    pricing_model.ask = 105.0
    rest.cancel_error = CoincallApiError(500, None, "cancel failed")
    await orchestrator.reprice_all_active()

    current = quotes.get_for_rfq("rfq-1")
    assert current is not None
    assert current.stage is QuoteStage.OPEN
    assert current.quote_id == first.quote_id
    assert rest.cancel_calls == [first.quote_id]
    assert len(rest.create_calls) == 1
    assert risk_gate.kill_switch_tripped


@pytest.mark.asyncio
async def test_create_failures_feed_kill_switch_and_then_block_quoting() -> None:
    rest = FakeRest()
    market_data = FakeMarketData(price=50_000.0)
    quotes = FakeQuoteLifecycle()
    quotes.reconcile_error = CoincallApiError(503, None, "unavailable")
    risk_gate = RiskGate(
        max_quote_notional_usd=1_000_000.0,
        max_leg_qty=100.0,
        min_time_to_expiry_hours=0.0,
        stale_market_data_seconds=30.0,
        kill_switch_threshold=2,
    )
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=FakePricingModel(),  # type: ignore[arg-type]
        risk_gate=risk_gate,
        quote_lifecycle=quotes,  # type: ignore[arg-type]
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))

    await orchestrator.reprice_all_active()
    await orchestrator.reprice_all_active()
    await orchestrator.reprice_all_active()

    assert risk_gate.kill_switch_tripped
    assert quotes.reconcile_calls == 2


@pytest.mark.asyncio
async def test_resolved_ambiguous_create_records_failure_before_success() -> None:
    rest = FakeRest()
    rest.create_error = CoincallAmbiguousError("timeout")
    rest.quote_list_response = {
        "code": 0,
        "data": [{"requestId": "rfq-1", "quoteId": "exchange-q-1"}],
    }
    market_data = FakeMarketData(price=50_000.0)
    risk_gate = RecordingRiskGate()
    quotes = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=FakePricingModel(),  # type: ignore[arg-type]
        risk_gate=risk_gate,  # type: ignore[arg-type]
        quote_lifecycle=quotes,
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))

    await orchestrator.reprice_all_active()

    current = quotes.get_for_rfq("rfq-1")
    assert current is not None
    assert current.stage is QuoteStage.OPEN
    assert current.quote_id == "exchange-q-1"
    assert rest.quote_list_calls == [{"request_id": "rfq-1", "state": "OPEN"}]
    assert risk_gate.calls == ["failure", "success"]


@pytest.mark.asyncio
async def test_reconciler_records_request_error_without_raising() -> None:
    rest = FakeRest(open_request_ids=[])
    rest.get_rfq_error = CoincallRequestError("network down")
    risk_gate = RiskGate(
        max_quote_notional_usd=1_000_000.0,
        max_leg_qty=100.0,
        min_time_to_expiry_hours=0.0,
        stale_market_data_seconds=30.0,
        kill_switch_threshold=1,
    )
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=FakeMarketData(),  # type: ignore[arg-type]
        pricing_model=None,  # type: ignore[arg-type]
        risk_gate=risk_gate,
        quote_lifecycle=FakeQuoteLifecycle(),  # type: ignore[arg-type]
    )

    await orchestrator.reconcile_with_exchange()

    assert risk_gate.kill_switch_tripped


@pytest.mark.asyncio
async def test_reprice_tick_dispatch_reprices_active_rfqs() -> None:
    rest = FakeRest()
    market_data = FakeMarketData(price=50_000.0)
    quotes = FakeQuoteLifecycle()
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=FakePricingModel(),  # type: ignore[arg-type]
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=quotes,  # type: ignore[arg-type]
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))

    await orchestrator.handle_event(RepriceTick())

    assert quotes.reconcile_calls == 1


@pytest.mark.asyncio
async def test_reconcile_tick_dispatch_reconciles_exchange_state() -> None:
    rest = FakeRest(open_request_ids=[])
    quotes = FakeQuoteLifecycle()
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=FakeMarketData(),  # type: ignore[arg-type]
        pricing_model=None,  # type: ignore[arg-type]
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=quotes,  # type: ignore[arg-type]
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))

    await orchestrator.handle_event(ReconcileTick())

    assert orchestrator.rfq_store.get("rfq-1") is None
    assert quotes.cancelled_for == ["rfq-1"]


@pytest.mark.asyncio
async def test_terminal_cancel_failure_keeps_rfq_and_reconcile_retries_until_evicted() -> None:
    rest = FakeRest(open_request_ids=[])
    market_data = FakeMarketData(price=50_000.0)
    quotes = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=FakePricingModel(),  # type: ignore[arg-type]
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=quotes,
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))
    await orchestrator.reprice_all_active()
    quote = quotes.get_for_rfq("rfq-1")
    assert quote is not None
    assert quote.quote_id is not None

    rest.cancel_error = CoincallApiError(500, None, "cancel failed")
    await orchestrator.handle_event(RfqTerminated("rfq-1", RfqStatus.CANCELLED))

    retained = orchestrator.rfq_store.get("rfq-1")
    current = quotes.get_for_rfq("rfq-1")
    assert retained is not None
    assert retained.is_terminal_status
    assert current is not None
    assert current.stage is QuoteStage.OPEN
    assert rest.cancel_calls == [quote.quote_id]

    rest.cancel_error = None
    await orchestrator.handle_event(ReconcileTick())

    assert orchestrator.rfq_store.get("rfq-1") is None
    assert quotes.get_for_rfq("rfq-1").stage is QuoteStage.CANCELLED  # type: ignore[union-attr]
    assert rest.cancel_calls == [quote.quote_id, quote.quote_id]


@pytest.mark.asyncio
async def test_quote_update_for_evicted_rfq_updates_known_quote_and_is_persisted() -> None:
    rest = FakeRest()
    quotes = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    persistence = RecordingPersistence()
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=FakeMarketData(),  # type: ignore[arg-type]
        pricing_model=None,  # type: ignore[arg-type]
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=quotes,
        persistence=persistence,  # type: ignore[arg-type]
    )
    quote = await quotes.reconcile(make_intent())
    assert quote.quote_id is not None
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))
    orchestrator.rfq_store.evict("rfq-1")

    await orchestrator.handle_event(
        QuoteUpdated(
            quote_id=quote.quote_id,
            request_id="rfq-1",
            stage=QuoteStage.FILLED,
            filled_price=101.0,
            filled_quantity=1.0,
            fill_time_ms=123456,
            block_trade_id="bt-1",
        )
    )

    updated = quotes.get_by_quote_id(quote.quote_id)
    assert updated is not None
    assert updated.stage is QuoteStage.FILLED
    assert updated.filled_price == 101.0
    assert updated.filled_quantity == 1.0
    assert updated.fill_time_ms == 123456
    assert updated.block_trade_id == "bt-1"
    assert persistence.quotes == [updated]


@pytest.mark.asyncio
async def test_terminal_rfq_is_evicted_and_late_events_are_ignored() -> None:
    rest = FakeRest()
    quotes = FakeQuoteLifecycle()
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=FakeMarketData(),  # type: ignore[arg-type]
        pricing_model=None,  # type: ignore[arg-type]
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=quotes,  # type: ignore[arg-type]
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))

    await orchestrator.handle_event(RfqTerminated("rfq-1", RfqStatus.CANCELLED))
    await orchestrator.handle_event(
        QuoteUpdated(quote_id="q-late", request_id="rfq-1", stage=QuoteStage.CANCELLED)
    )
    await orchestrator.handle_event(RfqTerminated("rfq-1", RfqStatus.CANCELLED))

    assert orchestrator.rfq_store.get("rfq-1") is None
    assert quotes.cancelled_for == ["rfq-1"]
