import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

import pytest

from coincall_rfq_maker import cli, reconciler
from coincall_rfq_maker.core.adapters.rest import (
    ApiFailureKind,
    CoincallAmbiguousError,
    CoincallApiError,
    CoincallRequestError,
    _mark_exchange_io_attempted,
    _parse_quote_list,
    _parse_rfq_list,
    classify_api_failure,
)
from coincall_rfq_maker.core.adapters.schemas import (
    CreateQuoteResult,
    QuoteListSnapshot,
    QuotePayload,
    RfqListSnapshot,
)
from coincall_rfq_maker.core.clock import get_timestamp_ms
from coincall_rfq_maker.domain.instruments import parse_instrument
from coincall_rfq_maker.domain.quote import Quote, QuoteStage
from coincall_rfq_maker.domain.rfq import Rfq, RfqLeg, RfqStage, RfqStatus, Side
from coincall_rfq_maker.events import QuoteUpdated, ReconcileTick, RepriceTick, RfqTerminated
from coincall_rfq_maker.marketdata.instruments import InstrumentCatalog
from coincall_rfq_maker.orchestration import (
    Orchestrator as _Orchestrator,
)
from coincall_rfq_maker.orchestration import (
    RfqStore,
    TransientOutageGate,
)
from coincall_rfq_maker.persistence.outbox import AuditOutbox
from coincall_rfq_maker.pricing import engine as pricing_engine
from coincall_rfq_maker.pricing.engine import BlackScholesModel, LegPrice
from coincall_rfq_maker.quoting.lifecycle import QuoteLifecycle
from coincall_rfq_maker.quoting.strategy import QuoteIntent, QuoteLegIntent
from coincall_rfq_maker.reconciler import RFQ_ABSENT_FROM_OPEN_GRACE_SECONDS, Reconciler
from coincall_rfq_maker.risk.gate import ApprovedQuotePlan, RiskDecision, RiskGate

INSTRUMENT = "BTCUSD-21AUG25-120000-C"


class FakeInstrumentCatalog:
    """Exchange expiry metadata fixture for legacy orchestration test setup."""

    async def expiration_ms(self, symbol: str) -> int:
        parsed = parse_instrument(symbol)
        expiry = datetime.combine(parsed.expiry_date, datetime.min.time(), tzinfo=UTC)
        return int(expiry.replace(hour=8).timestamp() * 1000)


class Orchestrator(_Orchestrator):
    """Supply valid exchange metadata to tests unrelated to expiry resolution."""

    def __init__(self, *args: object, **kwargs: object) -> None:
        kwargs.setdefault("instrument_catalog", FakeInstrumentCatalog())
        super().__init__(*args, **kwargs)  # type: ignore[arg-type]


class FakeRest:
    def __init__(self, open_request_ids: list[str] | None = None) -> None:
        self._open_request_ids = open_request_ids
        self.rfq_list_response: dict[str, Any] | None = None
        self.rfq_list_items: list[dict[str, Any]] | None = None
        self.rfq_list_by_id_items: dict[str, list[dict[str, Any]]] = {}
        self.rfq_list_by_id_errors: dict[str, Exception] = {}
        self.rfq_list_calls: list[dict[str, Any]] = []
        self.get_rfq_error: Exception | None = None
        self.get_quote_error: Exception | None = None
        self.create_calls: list[tuple[str, list[dict[str, str]]]] = []
        self.cancel_calls: list[str] = []
        self.cancel_all_calls = 0
        self.create_error: Exception | None = None
        self.cancel_error: Exception | None = None
        self.quote_list_response: dict[str, Any] = {"code": 0, "data": []}
        self.quote_list_responses: list[dict[str, Any]] | None = None
        self.quote_list_calls: list[dict[str, Any]] = []
        self._next_quote_id = 1

    async def get_rfq_list(self, **kwargs: Any) -> RfqListSnapshot:
        _mark_exchange_io_attempted()
        self.rfq_list_calls.append(kwargs)
        if self.get_rfq_error is not None:
            raise self.get_rfq_error
        request_id = kwargs.get("request_id")
        if request_id is not None:
            if request_id in self.rfq_list_by_id_errors:
                raise self.rfq_list_by_id_errors[request_id]
            return _rfq_payloads(self.rfq_list_by_id_items.get(request_id, []))
        if self.rfq_list_response is not None:
            return _parse_rfq_list(self.rfq_list_response)
        if self.rfq_list_items is not None:
            return _rfq_payloads(self.rfq_list_items)
        return _rfq_payloads(
            [
                {"requestId": rid, "state": "ACTIVE", "legs": []}
                for rid in self._open_request_ids or []
            ]
        )

    async def create_quote(self, request_id: str, legs: list[dict[str, str]]) -> CreateQuoteResult:
        _mark_exchange_io_attempted()
        self.create_calls.append((request_id, legs))
        if self.create_error is not None:
            raise self.create_error
        quote_id = f"q-{self._next_quote_id}"
        self._next_quote_id += 1
        return CreateQuoteResult.model_validate({"quoteId": quote_id})

    async def cancel_quote(self, quote_id: str) -> dict[str, Any]:
        _mark_exchange_io_attempted()
        self.cancel_calls.append(quote_id)
        if self.cancel_error is not None:
            raise self.cancel_error
        return {"code": 0, "data": {}}

    async def cancel_all_quotes(self) -> dict[str, Any]:
        _mark_exchange_io_attempted()
        self.cancel_all_calls += 1
        return {"code": 0, "data": {}}

    async def get_quote_list(self, **kwargs: Any) -> QuoteListSnapshot:
        _mark_exchange_io_attempted()
        self.quote_list_calls.append(kwargs)
        if self.get_quote_error is not None:
            raise self.get_quote_error
        if self.quote_list_responses is not None:
            return _quote_payloads(self.quote_list_responses.pop(0))
        return _quote_payloads(self.quote_list_response)


def _rfq_payloads(items: list[dict[str, Any]]) -> RfqListSnapshot:
    return _parse_rfq_list({"code": 0, "data": {"rfqList": items}})


def _quote_payloads(response: dict[str, Any] | list[dict[str, Any]]) -> QuoteListSnapshot:
    if isinstance(response, list):
        return _parse_quote_list({"code": 0, "data": response})
    return _parse_quote_list(response)


class FakeMarketData:
    def __init__(self, price: float | None = None, age_seconds: float = 0.0) -> None:
        self.tracked: set[str] = set()
        self.price = price
        self._age_seconds = age_seconds

    def track(self, underlying: str) -> None:
        self.tracked.add(underlying)

    def untrack(self, underlying: str) -> None:
        self.tracked.discard(underlying)

    def get_price(self, underlying: str) -> float | None:
        return self.price

    def age_seconds(self, underlying: str) -> float:
        return self._age_seconds


class FakeQuoteLifecycle:
    def __init__(self, api_reporter: Any | None = None) -> None:
        self.cancelled_for: list[str] = []
        self.evicted_for: list[str] = []
        self.reconcile_calls = 0
        self.reconcile_error: Exception | None = None
        self.cancel_all_calls = 0
        self._api_reporter = api_reporter

    async def cancel_for_rfq(self, request_id: str) -> None:
        self.cancelled_for.append(request_id)

    async def withdraw_for_rfq(self, request_id: str) -> None:
        self.cancelled_for.append(request_id)

    async def settle_filled_rfq(self, request_id: str) -> None:
        return None

    async def reconcile(self, intent: object) -> object:
        self.reconcile_calls += 1
        if self.reconcile_error is not None:
            if (
                self._api_reporter is not None
                and isinstance(self.reconcile_error, (CoincallRequestError, CoincallApiError))
                and classify_api_failure(self.reconcile_error) is ApiFailureKind.PERSISTENT
            ):
                self._api_reporter.record_api_failure()
            raise self.reconcile_error
        return object()

    async def cancel_all(self) -> None:
        self.cancel_all_calls += 1

    def get_by_quote_id(self, quote_id: str) -> None:
        return None

    def get_for_rfq(self, request_id: str) -> None:
        return None

    def open_quotes(self) -> list[Quote]:
        return []

    def non_terminal_quotes(self) -> list[Quote]:
        return []

    async def cancel_exchange_quote(self, quote_id: str) -> None:
        pass

    def adopt_open_exchange_quote(
        self,
        request_id: str,
        quote_id: str,
        payload: QuotePayload | None = None,
    ) -> Quote | None:
        return None

    async def resolve_remote_quote(self, quote: Quote) -> Quote | None:
        return None

    def apply_ws_update(self, event: QuoteUpdated) -> None:
        return None

    def has_open_or_pending_quote_for_rfq(self, request_id: str) -> bool:
        return False

    def evict_for_rfq(self, request_id: str) -> None:
        self.evicted_for.append(request_id)


class RecordingAuditOutbox:
    def __init__(self) -> None:
        self.quotes: list[Quote] = []

    def enqueue_rfq(self, *args: object, **kwargs: object) -> None:
        pass

    def enqueue_quote(
        self, quote: Quote, market_snapshot: dict[str, float] | None, now_ms: int
    ) -> None:
        self.quotes.append(quote)

    def enqueue_fill(self, *args: object, **kwargs: object) -> None:
        pass


class TerminalQuoteLifecycle:
    def __init__(self) -> None:
        self.settled = False
        self.evicted_for: list[str] = []

    async def settle_filled_rfq(self, request_id: str) -> Quote:
        self.settled = True
        return Quote(
            request_id=request_id,
            stage=QuoteStage.FILLED,
            legs=(),
            create_time_ms=0,
            quote_id="quote-1",
        )

    def has_open_or_pending_quote_for_rfq(self, request_id: str) -> bool:
        return False

    def evict_for_rfq(self, request_id: str) -> None:
        self.evicted_for.append(request_id)


class FakePricingModel:
    def __init__(self, ask: float = 100.0) -> None:
        self.ask = ask

    def price(self, *args: object, **kwargs: object) -> LegPrice:
        return LegPrice(bid=self.ask - 1.0, ask=self.ask)


class UnpriceablePricingModel:
    def price(self, *args: object, **kwargs: object) -> None:
        return None


class AssertionPricingModel:
    def price(self, *args: object, **kwargs: object) -> LegPrice:
        raise AssertionError("pricing must not receive missing expiry metadata")


class StaticInstrumentCatalog:
    def __init__(self, expiration: int | None) -> None:
        self.expiration = expiration

    async def expiration_ms(self, symbol: str) -> int | None:
        assert symbol == "BTCUSD-09JUL26-56000-C"
        return self.expiration


class FailingInstrumentRest(FakeRest):
    async def get_option_instruments(self, base: str) -> tuple[object, ...]:
        assert base == "BTC"
        raise RuntimeError("exchange unavailable")


class RecordingQuoteLifecycle(QuoteLifecycle):
    def __init__(self, rest_client: object) -> None:
        super().__init__(rest_client, dry_run=False, api_reporter=NoopApiReporter())  # type: ignore[arg-type]
        self.withdrawn_for: list[str] = []

    async def withdraw_for_rfq(self, request_id: str) -> Quote | None:
        self.withdrawn_for.append(request_id)
        return await super().withdraw_for_rfq(request_id)


class RecordingRiskGate:
    def __init__(self, decision: RiskDecision | None = None) -> None:
        self.calls: list[str] = []
        self.decision = decision
        self._consecutive_failures = 0
        self._failures_total = 0
        self.kill_switch_tripped = False

    def evaluate(self, *args: object, **kwargs: object) -> RiskDecision:
        if self.decision is not None:
            return self.decision
        intent = args[1]
        assert isinstance(intent, QuoteIntent)
        return RiskDecision(
            approved=True,
            plan=ApprovedQuotePlan(intent=intent, decided_at_ms=get_timestamp_ms()),
        )

    def record_api_failure(self) -> None:
        self.calls.append("failure")
        self._consecutive_failures += 1
        self._failures_total += 1

    def record_api_success(self) -> None:
        self.calls.append("success")
        self._consecutive_failures = 0

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    @property
    def failures_total(self) -> int:
        return self._failures_total

    def trip_kill_switch(self, reason: str) -> None:
        self.kill_switch_tripped = True


def absent_rfq_reconciler(
    rest: FakeRest,
    quotes: FakeQuoteLifecycle | QuoteLifecycle | None = None,
) -> tuple[Reconciler, RfqStore, RecordingRiskGate, list[tuple[str, RfqStatus]]]:
    store = RfqStore()
    store.upsert(make_rfq("rfq-1"), received_at_ms=0)
    risk_gate = RecordingRiskGate()
    terminal_rfqs: list[tuple[str, RfqStatus]] = []

    async def on_backfill(rfq: Rfq) -> None:
        raise AssertionError(f"unexpected RFQ backfill: {rfq.request_id}")

    async def on_terminal(request_id: str, status: RfqStatus) -> None:
        terminal_rfqs.append((request_id, status))

    instance = Reconciler(
        rest_client=rest,  # type: ignore[arg-type]
        rfq_store=store,
        quote_lifecycle=quotes if quotes is not None else FakeQuoteLifecycle(),  # type: ignore[arg-type]
        risk_gate=risk_gate,  # type: ignore[arg-type]
        on_rfq_backfill=on_backfill,
        on_terminal_rfq=on_terminal,
        on_api_recovery=lambda: None,
    )
    return instance, store, risk_gate, terminal_rfqs


def make_rfq(request_id: str) -> Rfq:
    return Rfq(
        request_id=request_id,
        status=RfqStatus.ACTIVE,
        legs=(RfqLeg(instrument_name=INSTRUMENT, side=Side.BUY, quantity="1"),),
        create_time_ms=0,
        expiry_time_ms=4_000_000_000_000,
    )


def make_rfq_payload(request_id: str = "rfq-1") -> dict[str, Any]:
    return {
        "requestId": request_id,
        "state": "ACTIVE",
        "legs": [{"instrumentName": INSTRUMENT, "side": "BUY", "quantity": "1"}],
        "createTime": 1,
        "expiryTime": 4_000_000_000_000,
        "takerName": "taker",
        "counterparty": "maker",
        "updateTime": 2,
    }


def make_intent(request_id: str = "rfq-1", price: float = 100.0) -> ApprovedQuotePlan:
    return ApprovedQuotePlan(
        intent=QuoteIntent(
            request_id=request_id,
            legs=(QuoteLegIntent(instrument_name=INSTRUMENT, price=price),),
        ),
        decided_at_ms=0,
    )


def approved_plan(intent: QuoteIntent) -> ApprovedQuotePlan:
    return ApprovedQuotePlan(intent=intent, decided_at_ms=0)


class NoopApiReporter:
    def record_api_failure(self) -> None:
        pass

    def record_api_success(self) -> None:
        pass


@pytest.mark.asyncio
async def test_reconciler_resolves_absent_rfq_as_filled_not_expired() -> None:
    rest = FakeRest(open_request_ids=[])
    rest.rfq_list_by_id_items["rfq-1"] = [{"requestId": "rfq-1", "state": "FILLED", "legs": []}]
    instance, _, _, terminal_rfqs = absent_rfq_reconciler(rest)

    await instance.reconcile_with_exchange()

    assert terminal_rfqs == [("rfq-1", RfqStatus.FILLED)]


@pytest.mark.asyncio
async def test_reconciler_graces_just_received_rfq_absent_from_exchange_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rest = FakeRest(open_request_ids=[])
    quotes = FakeQuoteLifecycle()
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=FakeMarketData(),  # type: ignore[arg-type]
        pricing_model=None,  # type: ignore[arg-type]
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=quotes,  # type: ignore[arg-type]
    )
    monkeypatch.setattr(reconciler, "get_timestamp_ms", lambda: 1_000_000)
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"), received_at_ms=1_000_000)

    await orchestrator.reconcile_with_exchange()

    assert orchestrator.rfq_store.get("rfq-1") is not None
    assert quotes.cancelled_for == []


@pytest.mark.asyncio
async def test_reconciler_resolves_absent_rfq_as_cancelled() -> None:
    rest = FakeRest(open_request_ids=[])
    rest.rfq_list_by_id_items["rfq-1"] = [{"requestId": "rfq-1", "state": "CANCELLED", "legs": []}]
    instance, _, _, terminal_rfqs = absent_rfq_reconciler(rest)

    await instance.reconcile_with_exchange()

    assert terminal_rfqs == [("rfq-1", RfqStatus.CANCELLED)]


@pytest.mark.asyncio
async def test_reconciler_defers_unresolved_absent_rfq_across_cycles() -> None:
    instance, store, _, terminal_rfqs = absent_rfq_reconciler(FakeRest(open_request_ids=[]))

    await instance.reconcile_with_exchange()
    await instance.reconcile_with_exchange()

    assert terminal_rfqs == []
    current = store.get("rfq-1")
    assert current is not None
    assert current.status is RfqStatus.ACTIVE


@pytest.mark.asyncio
async def test_reconciler_defers_absent_rfq_that_is_active_by_id() -> None:
    rest = FakeRest(open_request_ids=[])
    rest.rfq_list_by_id_items["rfq-1"] = [{"requestId": "rfq-1", "state": "ACTIVE", "legs": []}]
    instance, store, _, terminal_rfqs = absent_rfq_reconciler(rest)

    await instance.reconcile_with_exchange()

    assert terminal_rfqs == []
    current = store.get("rfq-1")
    assert current is not None
    assert current.status is RfqStatus.ACTIVE


@pytest.mark.asyncio
async def test_reconciler_by_id_failure_strikes_and_defers_absent_rfq() -> None:
    rest = FakeRest(open_request_ids=[])
    rest.rfq_list_by_id_errors["rfq-1"] = CoincallApiError(400, None, "bad request")
    instance, store, risk_gate, terminal_rfqs = absent_rfq_reconciler(rest)

    await instance.reconcile_with_exchange()

    assert terminal_rfqs == []
    assert store.get("rfq-1") is not None
    assert risk_gate.failures_total == 1


@pytest.mark.asyncio
async def test_reconciler_resolves_held_quote_for_absent_rfq_in_same_cycle() -> None:
    rest = FakeRest(open_request_ids=[])
    rest.rfq_list_by_id_items["rfq-1"] = [{"requestId": "rfq-1", "state": "FILLED", "legs": []}]
    quotes = QuoteLifecycle(rest, dry_run=False, api_reporter=NoopApiReporter())  # type: ignore[arg-type]
    created = await quotes.reconcile(make_intent())
    assert created.quote_id is not None
    rest.quote_list_responses = [
        {"code": 0, "data": []},
        {
            "code": 0,
            "data": [
                {
                    "requestId": "rfq-1",
                    "quoteId": created.quote_id,
                    "state": "FILLED",
                    "filledPrice": 101.0,
                }
            ],
        },
    ]
    instance, _, _, terminal_rfqs = absent_rfq_reconciler(rest, quotes)

    await instance.reconcile_with_exchange()

    assert terminal_rfqs == [("rfq-1", RfqStatus.FILLED)]
    current = quotes.get_for_rfq("rfq-1")
    assert current is not None
    assert current.stage is QuoteStage.FILLED


@pytest.mark.asyncio
async def test_reconciler_malformed_rfq_snapshot_expires_nothing_and_records_no_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rest = FakeRest()
    rest.rfq_list_response = {"code": 0, "data": 0}
    quotes = FakeQuoteLifecycle()
    risk_gate = RecordingRiskGate()
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=FakeMarketData(),  # type: ignore[arg-type]
        pricing_model=None,  # type: ignore[arg-type]
        risk_gate=risk_gate,  # type: ignore[arg-type]
        quote_lifecycle=quotes,  # type: ignore[arg-type]
    )
    monkeypatch.setattr(reconciler, "get_timestamp_ms", lambda: 1_000_000)
    received_at_ms = 1_000_000 - int((RFQ_ABSENT_FROM_OPEN_GRACE_SECONDS + 1) * 1000)
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"), received_at_ms=received_at_ms)

    await orchestrator.reconcile_with_exchange()

    assert orchestrator.rfq_store.get("rfq-1") is not None
    assert quotes.cancelled_for == []
    assert risk_gate.calls == []


@pytest.mark.asyncio
async def test_reconciler_keeps_local_rfq_when_malformed_open_item_salvages_request_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rest = FakeRest()
    rest.rfq_list_items = [{"requestId": "rfq-1", "state": "ACTIVE", "legs": "not-a-list"}]
    quotes = FakeQuoteLifecycle()
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=FakeMarketData(),  # type: ignore[arg-type]
        pricing_model=None,  # type: ignore[arg-type]
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=quotes,  # type: ignore[arg-type]
    )
    monkeypatch.setattr(reconciler, "get_timestamp_ms", lambda: 1_000_000)
    received_at_ms = 1_000_000 - int((RFQ_ABSENT_FROM_OPEN_GRACE_SECONDS + 1) * 1000)
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"), received_at_ms=received_at_ms)

    await orchestrator.reconcile_with_exchange()

    current = orchestrator.rfq_store.get("rfq-1")
    assert current is not None
    assert current.is_terminal_status is False
    assert quotes.cancelled_for == []


@pytest.mark.asyncio
async def test_reconciler_converges_when_exchange_matches_local_state() -> None:
    rest = FakeRest(open_request_ids=["rfq-1"])
    market_data = FakeMarketData()
    quotes = FakeQuoteLifecycle()
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=None,  # type: ignore[arg-type]
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=quotes,  # type: ignore[arg-type]
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"), received_at_ms=0)

    await orchestrator.reconcile_with_exchange()

    assert quotes.cancelled_for == []
    assert orchestrator.rfq_store.get("rfq-1").is_terminal_status is False  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_reconciler_quiet_cycle_emits_one_heartbeat_per_cycle(
    caplog: pytest.LogCaptureFixture,
) -> None:
    rest = FakeRest(open_request_ids=[])
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=FakeMarketData(),  # type: ignore[arg-type]
        pricing_model=None,  # type: ignore[arg-type]
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=FakeQuoteLifecycle(),  # type: ignore[arg-type]
    )

    with caplog.at_level(logging.INFO, logger="coincall_rfq_maker.reconciler"):
        await orchestrator.reconcile_with_exchange()
        await orchestrator.reconcile_with_exchange()

    heartbeats = [
        record for record in caplog.records if record.getMessage().startswith("Reconcile cycle ok:")
    ]
    assert len(heartbeats) == 2


@pytest.mark.asyncio
async def test_reconciler_stamps_liveness_when_rfq_fetch_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rest = FakeRest(open_request_ids=[])
    rest.get_rfq_error = CoincallRequestError("network down")
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=FakeMarketData(),  # type: ignore[arg-type]
        pricing_model=None,  # type: ignore[arg-type]
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=FakeQuoteLifecycle(),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(reconciler, "get_timestamp_ms", lambda: 123_456)

    assert orchestrator.reconciler_last_cycle_ms is None
    await orchestrator.reconcile_with_exchange()
    assert orchestrator.reconciler_last_cycle_ms == 123_456


@pytest.mark.asyncio
async def test_reconciler_backfills_unknown_open_rfq_and_dedupes_next_pass() -> None:
    rest = FakeRest()
    rest.rfq_list_items = [make_rfq_payload("rfq-1"), make_rfq_payload("rfq-1")]
    market_data = FakeMarketData(price=50_000.0)
    risk_gate = RecordingRiskGate()
    quotes = QuoteLifecycle(rest, dry_run=False, api_reporter=risk_gate)  # type: ignore[arg-type]
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=FakePricingModel(),
        risk_gate=risk_gate,  # type: ignore[arg-type]
        quote_lifecycle=quotes,
    )

    await orchestrator.reconcile_with_exchange()
    await orchestrator.reconcile_with_exchange()

    rfq = orchestrator.rfq_store.get("rfq-1")
    quote = quotes.get_for_rfq("rfq-1")
    assert rfq is not None
    assert rfq.status is RfqStatus.ACTIVE
    assert rfq.stage is RfqStage.QUOTED
    assert market_data.tracked == {"BTCUSD"}
    assert len(rest.create_calls) == 1
    assert quote is not None
    assert quote.stage is QuoteStage.OPEN
    # Create plus one remote-resolution GET per cycle are real quote-operation I/O,
    # alongside one honest end-of-cycle success per pair of list fetches. The former
    # expectation of seven additionally counted the premature rfqList successes.
    assert risk_gate.calls == ["success"] * 5


@pytest.mark.asyncio
async def test_reconciler_cancels_unknown_exchange_open_quote() -> None:
    rest = FakeRest(open_request_ids=[])
    rest.quote_list_response = {
        "code": 0,
        "data": [{"requestId": "rfq-ghost", "quoteId": "q-orphan", "state": "OPEN"}],
    }
    quotes = QuoteLifecycle(rest, dry_run=False, api_reporter=NoopApiReporter())  # type: ignore[arg-type]
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=FakeMarketData(),  # type: ignore[arg-type]
        pricing_model=None,  # type: ignore[arg-type]
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=quotes,
    )

    await orchestrator.reconcile_with_exchange()

    assert rest.cancel_calls == ["q-orphan"]


def make_reconciler_risk_gate() -> RiskGate:
    return RiskGate(
        max_quote_notional_usd=1_000_000.0,
        max_leg_qty=100.0,
        min_time_to_expiry_hours=0.000001,
        stale_market_data_seconds=30.0,
        kill_switch_threshold=5,
    )


def make_orphan_reconciler(rest: FakeRest, risk_gate: RiskGate) -> tuple[Orchestrator, list[str]]:
    quotes = QuoteLifecycle(rest, dry_run=False, api_reporter=risk_gate)  # type: ignore[arg-type]
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=FakeMarketData(),  # type: ignore[arg-type]
        pricing_model=None,  # type: ignore[arg-type]
        risk_gate=risk_gate,
        quote_lifecycle=quotes,
    )
    recoveries: list[str] = []
    orchestrator._reconciler._on_api_recovery = lambda: recoveries.append("recovered")
    return orchestrator, recoveries


def orphan_quote_snapshot() -> dict[str, Any]:
    return {
        "code": 0,
        "data": [{"requestId": "rfq-ghost", "quoteId": "q-orphan", "state": "OPEN"}],
    }


@pytest.mark.asyncio
async def test_reconciler_preserves_persistent_cleanup_strike_but_recovers_outage_gate() -> None:
    rest = FakeRest(open_request_ids=[])
    rest.quote_list_response = orphan_quote_snapshot()
    rest.cancel_error = CoincallApiError(200, 51234, "cannot cancel")
    risk_gate = make_reconciler_risk_gate()
    orchestrator, recoveries = make_orphan_reconciler(rest, risk_gate)

    await orchestrator.reconcile_with_exchange()

    assert risk_gate.consecutive_failures == 1
    assert recoveries == ["recovered"]


@pytest.mark.asyncio
async def test_reconciler_clean_quiet_book_recovers_and_clears_streak() -> None:
    rest = FakeRest(open_request_ids=[])
    risk_gate = make_reconciler_risk_gate()
    orchestrator, recoveries = make_orphan_reconciler(rest, risk_gate)
    for _ in range(3):
        risk_gate.record_api_failure()

    await orchestrator.reconcile_with_exchange()

    assert recoveries == ["recovered"]
    assert risk_gate.consecutive_failures == 0


@pytest.mark.asyncio
async def test_reconciler_transient_orphan_cleanup_does_not_withhold_streak_clear() -> None:
    rest = FakeRest(open_request_ids=[])
    rest.quote_list_response = orphan_quote_snapshot()
    rest.cancel_error = CoincallApiError(200, 10000, "Try again later")
    risk_gate = make_reconciler_risk_gate()
    orchestrator, recoveries = make_orphan_reconciler(rest, risk_gate)
    risk_gate.record_api_failure()
    risk_gate.record_api_failure()

    await orchestrator.reconcile_with_exchange()

    assert risk_gate.consecutive_failures == 0
    assert recoveries == ["recovered"]


@pytest.mark.asyncio
async def test_orphan_cancel_ladder_cancel_all_then_prunes_when_orphan_vanishes() -> None:
    rest = FakeRest(open_request_ids=[])
    rest.quote_list_response = orphan_quote_snapshot()
    rest.cancel_error = CoincallApiError(200, 10000, "Try again later")
    risk_gate = make_reconciler_risk_gate()
    orchestrator, _ = make_orphan_reconciler(rest, risk_gate)

    await orchestrator.reconcile_with_exchange()
    await orchestrator.reconcile_with_exchange()
    assert rest.cancel_all_calls == 0

    await orchestrator.reconcile_with_exchange()
    assert rest.cancel_all_calls == 1
    assert orchestrator._reconciler._orphan_cancel_failures == {"q-orphan": 3}

    rest.quote_list_response = {"code": 0, "data": []}
    await orchestrator.reconcile_with_exchange()

    assert not risk_gate.kill_switch_tripped
    assert orchestrator._reconciler._orphan_cancel_failures == {}


@pytest.mark.asyncio
async def test_orphan_cancel_ladder_trips_when_orphan_remains_after_cancel_all(
    caplog: pytest.LogCaptureFixture,
) -> None:
    rest = FakeRest(open_request_ids=[])
    rest.quote_list_response = orphan_quote_snapshot()
    rest.cancel_error = CoincallApiError(200, 10000, "Try again later")
    risk_gate = make_reconciler_risk_gate()
    orchestrator, _ = make_orphan_reconciler(rest, risk_gate)

    for _ in range(4):
        await orchestrator.reconcile_with_exchange()

    assert rest.cancel_all_calls == 1
    assert risk_gate.kill_switch_tripped
    assert "orphan exchange quote q-orphan uncancellable after 4 reconcile cycles" in caplog.text


@pytest.mark.asyncio
async def test_orphan_cancel_failure_counter_is_pruned_when_orphan_is_absent() -> None:
    rest = FakeRest(open_request_ids=[])
    rest.quote_list_response = orphan_quote_snapshot()
    rest.cancel_error = CoincallApiError(200, 10000, "Try again later")
    risk_gate = make_reconciler_risk_gate()
    orchestrator, _ = make_orphan_reconciler(rest, risk_gate)

    await orchestrator.reconcile_with_exchange()
    assert orchestrator._reconciler._orphan_cancel_failures == {"q-orphan": 1}

    rest.quote_list_response = {"code": 0, "data": []}
    await orchestrator.reconcile_with_exchange()

    assert orchestrator._reconciler._orphan_cancel_failures == {}


@pytest.mark.asyncio
async def test_reconciler_dry_run_logs_unknown_exchange_open_quote_without_cancel() -> None:
    rest = FakeRest(open_request_ids=[])
    rest.quote_list_response = {
        "code": 0,
        "data": [{"requestId": "rfq-ghost", "quoteId": "q-orphan", "state": "OPEN"}],
    }
    quotes = QuoteLifecycle(rest, dry_run=True)  # type: ignore[arg-type]
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=FakeMarketData(),  # type: ignore[arg-type]
        pricing_model=None,  # type: ignore[arg-type]
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=quotes,
    )

    await orchestrator.reconcile_with_exchange()

    assert rest.cancel_calls == []


@pytest.mark.asyncio
async def test_reconciler_adopts_unknown_exchange_quote_for_local_pending_create() -> None:
    rest = FakeRest(open_request_ids=["rfq-1"])
    rest.create_error = CoincallAmbiguousError("timeout")
    rest.quote_list_response = {"code": 0, "data": []}
    quotes = QuoteLifecycle(rest, dry_run=False, api_reporter=NoopApiReporter())  # type: ignore[arg-type]
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=FakeMarketData(),  # type: ignore[arg-type]
        pricing_model=None,  # type: ignore[arg-type]
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=quotes,
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"), received_at_ms=0)
    with pytest.raises(CoincallRequestError):
        await quotes.reconcile(make_intent())
    assert quotes.get_for_rfq("rfq-1").stage is QuoteStage.PENDING_CREATE  # type: ignore[union-attr]
    assert quotes.get_for_rfq("rfq-1").quote_id is None  # type: ignore[union-attr]

    rest.quote_list_response = {
        "code": 0,
        "data": [
            {
                "requestId": "rfq-1",
                "quoteId": "exchange-q-1",
                "state": "OPEN",
                "legs": [{"instrumentName": INSTRUMENT, "price": "1.0"}],
            }
        ],
    }

    await orchestrator.reconcile_with_exchange()

    current = quotes.get_for_rfq("rfq-1")
    assert current is not None
    assert current.stage is QuoteStage.OPEN
    assert current.quote_id == "exchange-q-1"
    assert current.legs[0].price == 1.0
    assert rest.cancel_calls == []


@pytest.mark.asyncio
async def test_reconciler_resolves_pending_cancel_despite_stale_market_data() -> None:
    rest = FakeRest(open_request_ids=["rfq-1"])
    market_data = FakeMarketData(price=50_000.0, age_seconds=300.0)
    quotes = QuoteLifecycle(rest, dry_run=False, api_reporter=NoopApiReporter())  # type: ignore[arg-type]
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=FakePricingModel(),
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=quotes,
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"), received_at_ms=0)
    created = await quotes.reconcile(make_intent())
    assert created.quote_id is not None
    rest.cancel_error = CoincallAmbiguousError("timeout")
    rest.quote_list_response = {"code": 0, "data": []}
    with pytest.raises(CoincallAmbiguousError):
        await quotes.cancel_for_rfq("rfq-1")
    assert quotes.get_for_rfq("rfq-1").stage is QuoteStage.PENDING_CANCEL  # type: ignore[union-attr]
    assert market_data.age_seconds("BTCUSD") == 300.0

    rest.quote_list_responses = [
        {"code": 0, "data": []},
        {
            "code": 0,
            "data": [
                {
                    "requestId": "rfq-1",
                    "quoteId": created.quote_id,
                    "state": "FILLED",
                    "filledPrice": 101.0,
                }
            ],
        },
    ]

    await orchestrator.reconcile_with_exchange()

    current = quotes.get_for_rfq("rfq-1")
    assert current is not None
    assert current.stage is QuoteStage.FILLED
    assert current.filled_price == 101.0
    assert len(rest.create_calls) == 1
    assert rest.quote_list_calls[-2:] == [
        {"state": "OPEN"},
        {"quote_id": created.quote_id},
    ]


@pytest.mark.asyncio
async def test_reconciler_adopts_filled_exchange_quote_for_pending_create() -> None:
    rest = FakeRest(open_request_ids=["rfq-1"])
    rest.create_error = CoincallAmbiguousError("timeout")
    rest.quote_list_response = {"code": 0, "data": []}
    quotes = QuoteLifecycle(rest, dry_run=False, api_reporter=NoopApiReporter())  # type: ignore[arg-type]
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=FakeMarketData(),  # type: ignore[arg-type]
        pricing_model=None,  # type: ignore[arg-type]
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=quotes,
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"), received_at_ms=0)
    with pytest.raises(CoincallAmbiguousError):
        await quotes.reconcile(make_intent())
    pending = quotes.get_for_rfq("rfq-1")
    assert pending is not None
    assert pending.stage is QuoteStage.PENDING_CREATE
    assert pending.quote_id is None

    rest.quote_list_responses = [
        {"code": 0, "data": []},
        {
            "code": 0,
            "data": [
                {
                    "requestId": "rfq-1",
                    "quoteId": "exchange-q-1",
                    "state": "FILLED",
                    "filledPrice": 101.0,
                }
            ],
        },
    ]

    await orchestrator.reconcile_with_exchange()

    current = quotes.get_for_rfq("rfq-1")
    assert current is not None
    assert current.stage is QuoteStage.FILLED
    assert current.quote_id == "exchange-q-1"
    assert current.filled_price == 101.0
    assert len(rest.create_calls) == 1
    assert rest.cancel_calls == []
    assert rest.quote_list_calls[-2:] == [
        {"state": "OPEN"},
        {"request_id": "rfq-1"},
    ]


@pytest.mark.asyncio
async def test_reconciler_resolves_local_open_quote_absent_from_exchange_open_list() -> None:
    rest = FakeRest(open_request_ids=["rfq-1"])
    market_data = FakeMarketData(price=50_000.0)
    quotes = QuoteLifecycle(rest, dry_run=False, api_reporter=NoopApiReporter())  # type: ignore[arg-type]
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=FakePricingModel(),
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=quotes,
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))
    created = await quotes.reconcile(make_intent())
    assert created.quote_id is not None
    rest.quote_list_responses = [
        {"code": 0, "data": []},
        {
            "code": 0,
            "data": [
                {
                    "requestId": "rfq-1",
                    "quoteId": created.quote_id,
                    "state": "FILLED",
                    "filledPrice": 101.0,
                    "filledQuantity": 1.0,
                    "fillTime": 123456,
                    "blockTradeId": "bt-1",
                }
            ],
        },
    ]

    await orchestrator.reconcile_with_exchange()

    current = quotes.get_for_rfq("rfq-1")
    assert current is not None
    assert current.stage is QuoteStage.FILLED
    assert current.filled_price == 101.0
    assert current.filled_quantity == 1.0
    assert current.fill_time_ms == 123456
    assert current.block_trade_id == "bt-1"
    assert rest.quote_list_calls == [{"state": "OPEN"}, {"quote_id": created.quote_id}]


@pytest.mark.asyncio
async def test_reconciler_treats_salvaged_malformed_quote_ids_as_remote_open() -> None:
    rest = FakeRest(open_request_ids=["rfq-1"])
    market_data = FakeMarketData(price=50_000.0)
    quotes = QuoteLifecycle(rest, dry_run=False, api_reporter=NoopApiReporter())  # type: ignore[arg-type]
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=FakePricingModel(),
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=quotes,
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))
    created = await quotes.reconcile(make_intent())
    assert created.quote_id is not None
    rest.quote_list_response = {
        "code": 0,
        "data": [
            {
                "requestId": "rfq-1",
                "quoteId": created.quote_id,
                "state": "OPEN",
                "filledPrice": "not-a-number",
            },
            {
                "requestId": "rfq-ghost",
                "quoteId": "q-orphan",
                "state": "OPEN",
                "filledPrice": "not-a-number",
            },
        ],
    }

    await orchestrator.reconcile_with_exchange()

    current = quotes.get_for_rfq("rfq-1")
    assert current is not None
    assert current.stage is QuoteStage.OPEN
    assert current.quote_id == created.quote_id
    assert rest.cancel_calls == []
    assert rest.quote_list_calls == [{"state": "OPEN"}]


@pytest.mark.asyncio
async def test_reconciler_skips_malformed_resolved_quote_and_continues() -> None:
    rest = FakeRest(open_request_ids=["rfq-1", "rfq-2"])
    quotes = QuoteLifecycle(rest, dry_run=False, api_reporter=NoopApiReporter())  # type: ignore[arg-type]
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=FakeMarketData(),  # type: ignore[arg-type]
        pricing_model=None,  # type: ignore[arg-type]
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=quotes,
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))
    orchestrator.rfq_store.upsert(make_rfq("rfq-2"))
    first = await quotes.reconcile(make_intent(request_id="rfq-1"))
    second = await quotes.reconcile(make_intent(request_id="rfq-2"))
    assert first.quote_id is not None
    assert second.quote_id is not None
    rest.quote_list_responses = [
        {"code": 0, "data": []},
        {
            "code": 0,
            "data": [
                {
                    "requestId": "rfq-1",
                    "quoteId": first.quote_id,
                    "state": "FILLED",
                    "filledPrice": "not-a-number",
                }
            ],
        },
        {
            "code": 0,
            "data": [
                {
                    "requestId": "rfq-2",
                    "quoteId": second.quote_id,
                    "state": "FILLED",
                    "filledPrice": 102.0,
                }
            ],
        },
    ]

    await orchestrator.reconcile_with_exchange()

    assert quotes.get_for_rfq("rfq-1").stage is QuoteStage.OPEN  # type: ignore[union-attr]
    resolved = quotes.get_for_rfq("rfq-2")
    assert resolved is not None
    assert resolved.stage is QuoteStage.FILLED
    assert resolved.filled_price == 102.0


@pytest.mark.asyncio
async def test_persistent_cancel_failure_reverts_open_skips_replacement_and_records_failure() -> (
    None
):
    rest = FakeRest()
    market_data = FakeMarketData(price=50_000.0)
    pricing_model = FakePricingModel(ask=100.0)
    risk_gate = RiskGate(
        max_quote_notional_usd=1_000_000.0,
        max_leg_qty=100.0,
        min_time_to_expiry_hours=0.000001,
        stale_market_data_seconds=30.0,
        kill_switch_threshold=1,
    )
    quotes = QuoteLifecycle(rest, dry_run=False, api_reporter=risk_gate)  # type: ignore[arg-type]
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=pricing_model,
        risk_gate=risk_gate,
        quote_lifecycle=quotes,
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))
    await orchestrator.reprice_all_active()
    first = quotes.get_for_rfq("rfq-1")
    assert first is not None

    pricing_model.ask = 105.0
    rest.cancel_error = CoincallApiError(200, 10004, "Parameter illegal")
    await orchestrator.reprice_all_active()

    current = quotes.get_for_rfq("rfq-1")
    assert current is not None
    assert current.stage is QuoteStage.OPEN
    assert current.quote_id == first.quote_id
    assert rest.cancel_calls == [first.quote_id]
    assert len(rest.create_calls) == 1
    assert risk_gate.kill_switch_tripped


@pytest.mark.asyncio
async def test_risk_reject_cancels_existing_open_quote() -> None:
    rest = FakeRest()
    market_data = FakeMarketData(price=50_000.0)
    risk_gate = RecordingRiskGate()
    quotes = QuoteLifecycle(rest, dry_run=False, api_reporter=NoopApiReporter())  # type: ignore[arg-type]
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=FakePricingModel(),
        risk_gate=risk_gate,  # type: ignore[arg-type]
        quote_lifecycle=quotes,
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))
    await orchestrator.reprice_all_active()
    quote = quotes.get_for_rfq("rfq-1")
    assert quote is not None
    assert quote.quote_id is not None

    risk_gate.decision = RiskDecision(approved=False, reason="kill switch tripped")
    await orchestrator.reprice_all_active()

    current = quotes.get_for_rfq("rfq-1")
    assert current is not None
    assert current.stage is QuoteStage.CANCELLED
    assert rest.cancel_calls == [quote.quote_id]
    assert len(rest.create_calls) == 1


@pytest.mark.asyncio
async def test_rejected_risk_decision_never_reaches_reconcile() -> None:
    quotes = FakeQuoteLifecycle()
    orchestrator = Orchestrator(
        rest_client=FakeRest(),  # type: ignore[arg-type]
        market_data=FakeMarketData(price=50_000.0),  # type: ignore[arg-type]
        pricing_model=FakePricingModel(),
        risk_gate=RecordingRiskGate(RiskDecision(approved=False, reason="limit")),  # type: ignore[arg-type]
        quote_lifecycle=quotes,  # type: ignore[arg-type]
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))

    await orchestrator.reprice_all_active()

    assert quotes.reconcile_calls == 0


@pytest.mark.asyncio
async def test_unpriceable_leg_withdraws_existing_open_quote_without_creating() -> None:
    rest = FakeRest()
    quotes = RecordingQuoteLifecycle(rest)
    existing = await quotes.reconcile(make_intent())
    assert existing.stage is QuoteStage.OPEN
    assert existing.quote_id is not None
    rest.create_calls.clear()

    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=FakeMarketData(price=50_000.0),  # type: ignore[arg-type]
        pricing_model=UnpriceablePricingModel(),
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=quotes,
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))

    await orchestrator.reprice_all_active()

    assert rest.create_calls == []
    assert quotes.withdrawn_for == ["rfq-1"]
    assert rest.cancel_calls == [existing.quote_id]


@pytest.mark.asyncio
async def test_exchange_expiry_on_expiry_day_submits_a_quote(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: object | None = None) -> datetime:
            return datetime(2026, 7, 9, 2, tzinfo=UTC)

    monkeypatch.setattr(pricing_engine, "datetime", FrozenDateTime)
    rest = FakeRest()
    quotes = QuoteLifecycle(rest, dry_run=False, api_reporter=NoopApiReporter())  # type: ignore[arg-type]
    orchestrator = _Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=FakeMarketData(price=60_000.0),  # type: ignore[arg-type]
        pricing_model=BlackScholesModel(),
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=quotes,
        instrument_catalog=StaticInstrumentCatalog(1_783_584_000_000),  # type: ignore[arg-type]
    )
    rfq = Rfq(
        request_id="rfq-1",
        status=RfqStatus.ACTIVE,
        legs=(RfqLeg("BTCUSD-09JUL26-56000-C", Side.BUY, "1"),),
        create_time_ms=0,
        expiry_time_ms=4_000_000_000_000,
    )
    orchestrator.rfq_store.upsert(rfq)

    await orchestrator.reprice_all_active()

    assert len(rest.create_calls) == 1


@pytest.mark.asyncio
async def test_expiry_mismatch_withdraws_without_submitting() -> None:
    rest = FakeRest()
    quotes = RecordingQuoteLifecycle(rest)
    existing = await quotes.reconcile(
        approved_plan(
            QuoteIntent(
                request_id="rfq-1",
                legs=(QuoteLegIntent("BTCUSD-09JUL26-56000-C", 100.0),),
            )
        )
    )
    assert existing.quote_id is not None
    rest.create_calls.clear()
    orchestrator = _Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=FakeMarketData(price=60_000.0),  # type: ignore[arg-type]
        pricing_model=AssertionPricingModel(),  # type: ignore[arg-type]
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=quotes,
        instrument_catalog=StaticInstrumentCatalog(1_783_670_400_000),  # type: ignore[arg-type]
    )
    orchestrator.rfq_store.upsert(
        Rfq(
            request_id="rfq-1",
            status=RfqStatus.ACTIVE,
            legs=(RfqLeg("BTCUSD-09JUL26-56000-C", Side.BUY, "1"),),
            create_time_ms=0,
            expiry_time_ms=4_000_000_000_000,
        )
    )

    await orchestrator.reprice_all_active()

    assert rest.create_calls == []
    assert quotes.withdrawn_for == ["rfq-1"]
    assert rest.cancel_calls == [existing.quote_id]


@pytest.mark.asyncio
async def test_missing_exchange_expiry_metadata_never_calls_pricing_and_withdraws() -> None:
    rest = FailingInstrumentRest()
    quotes = RecordingQuoteLifecycle(rest)
    existing = await quotes.reconcile(
        approved_plan(
            QuoteIntent(
                request_id="rfq-1",
                legs=(QuoteLegIntent("BTCUSD-09JUL26-56000-C", 100.0),),
            )
        )
    )
    assert existing.quote_id is not None
    rest.create_calls.clear()
    orchestrator = _Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=FakeMarketData(price=60_000.0),  # type: ignore[arg-type]
        pricing_model=AssertionPricingModel(),  # type: ignore[arg-type]
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=quotes,
        instrument_catalog=InstrumentCatalog(rest),  # type: ignore[arg-type]
    )
    orchestrator.rfq_store.upsert(
        Rfq(
            request_id="rfq-1",
            status=RfqStatus.ACTIVE,
            legs=(RfqLeg("BTCUSD-09JUL26-56000-C", Side.BUY, "1"),),
            create_time_ms=0,
            expiry_time_ms=4_000_000_000_000,
        )
    )

    await orchestrator.reprice_all_active()

    assert rest.create_calls == []
    assert quotes.withdrawn_for == ["rfq-1"]
    assert rest.cancel_calls == [existing.quote_id]


@pytest.mark.asyncio
async def test_risk_reject_with_pending_create_cancels_quote_that_later_opened() -> None:
    rest = FakeRest()
    rest.create_error = CoincallAmbiguousError("timeout")
    rest.quote_list_response = {"code": 0, "data": []}
    market_data = FakeMarketData(price=50_000.0)
    risk_gate = RecordingRiskGate()
    quotes = QuoteLifecycle(rest, dry_run=False, api_reporter=NoopApiReporter())  # type: ignore[arg-type]
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=FakePricingModel(),
        risk_gate=risk_gate,  # type: ignore[arg-type]
        quote_lifecycle=quotes,
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))
    await orchestrator.reprice_all_active()
    pending = quotes.get_for_rfq("rfq-1")
    assert pending is not None
    assert pending.stage is QuoteStage.PENDING_CREATE

    risk_gate.decision = RiskDecision(approved=False, reason="limit")
    rest.create_error = None
    rest.quote_list_response = {
        "code": 0,
        "data": [{"requestId": "rfq-1", "quoteId": "exchange-q-1", "state": "OPEN"}],
    }

    await orchestrator.reprice_all_active()

    current = quotes.get_for_rfq("rfq-1")
    assert current is not None
    assert current.stage is QuoteStage.CANCELLED
    assert current.quote_id == "exchange-q-1"
    assert rest.cancel_calls == ["exchange-q-1"]
    assert rest.quote_list_calls == [
        {"request_id": "rfq-1", "state": "OPEN"},
        {"request_id": "rfq-1", "state": "OPEN"},
    ]


@pytest.mark.asyncio
async def test_risk_reject_with_pending_cancel_resolves_and_cancels_if_still_open() -> None:
    rest = FakeRest()
    market_data = FakeMarketData(price=50_000.0)
    risk_gate = RecordingRiskGate()
    quotes = QuoteLifecycle(rest, dry_run=False, api_reporter=NoopApiReporter())  # type: ignore[arg-type]
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=FakePricingModel(),
        risk_gate=risk_gate,  # type: ignore[arg-type]
        quote_lifecycle=quotes,
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))
    await orchestrator.reprice_all_active()
    quote = quotes.get_for_rfq("rfq-1")
    assert quote is not None
    assert quote.quote_id is not None
    rest.cancel_error = CoincallAmbiguousError("timeout")
    rest.quote_list_response = {"code": 0, "data": []}
    with pytest.raises(CoincallAmbiguousError):
        await quotes.cancel_for_rfq("rfq-1")
    assert quotes.get_for_rfq("rfq-1").stage is QuoteStage.PENDING_CANCEL  # type: ignore[union-attr]

    risk_gate.decision = RiskDecision(approved=False, reason="limit")
    rest.cancel_error = None
    rest.quote_list_response = {
        "code": 0,
        "data": [{"requestId": "rfq-1", "quoteId": quote.quote_id, "state": "OPEN"}],
    }

    await orchestrator.reprice_all_active()

    current = quotes.get_for_rfq("rfq-1")
    assert current is not None
    assert current.stage is QuoteStage.CANCELLED
    assert rest.cancel_calls == [quote.quote_id, quote.quote_id]
    assert rest.quote_list_calls[-1] == {"quote_id": quote.quote_id}


@pytest.mark.asyncio
async def test_risk_reject_does_not_log_withdrawal_for_already_terminal_quote(
    caplog: pytest.LogCaptureFixture,
) -> None:
    rest = FakeRest()
    market_data = FakeMarketData(price=50_000.0)
    risk_gate = RecordingRiskGate()
    quotes = QuoteLifecycle(rest, dry_run=False, api_reporter=NoopApiReporter())  # type: ignore[arg-type]
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=FakePricingModel(),
        risk_gate=risk_gate,  # type: ignore[arg-type]
        quote_lifecycle=quotes,
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))
    await orchestrator.reprice_all_active()
    quote = quotes.get_for_rfq("rfq-1")
    assert quote is not None
    assert quote.quote_id is not None
    quotes.apply_ws_update(
        QuoteUpdated(quote_id=quote.quote_id, request_id="rfq-1", stage=QuoteStage.FILLED)
    )

    risk_gate.decision = RiskDecision(approved=False, reason="limit")
    with caplog.at_level(logging.WARNING):
        await orchestrator.reprice_all_active()

    assert "Withdrew quote for RFQ rfq-1 after risk rejection" not in caplog.text
    assert rest.cancel_calls == []


@pytest.mark.asyncio
async def test_repeated_persistent_create_failures_trip_and_flatten_promptly() -> None:
    rest = FakeRest()
    market_data = FakeMarketData(price=50_000.0)
    trip_event = asyncio.Event()
    shutdown = asyncio.Event()
    risk_gate = RiskGate(
        max_quote_notional_usd=1_000_000.0,
        max_leg_qty=100.0,
        min_time_to_expiry_hours=0.000001,
        stale_market_data_seconds=30.0,
        kill_switch_threshold=5,
        on_trip=trip_event.set,
    )
    rest.create_error = CoincallApiError(200, 10004, "Parameter illegal")
    quotes = QuoteLifecycle(rest, dry_run=False, api_reporter=risk_gate)  # type: ignore[arg-type]
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=FakePricingModel(),
        risk_gate=risk_gate,
        quote_lifecycle=quotes,
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))
    flatten = asyncio.create_task(
        cli._flatten_on_kill_switch(trip_event, shutdown, quotes, risk_gate)
    )

    for _ in range(5):
        await orchestrator.reprice_all_active()
    await asyncio.wait_for(flatten, timeout=0.2)

    assert risk_gate.kill_switch_tripped
    assert len(rest.create_calls) == 5
    assert rest.cancel_all_calls == 1


@pytest.mark.asyncio
async def test_noop_reconcile_records_neither_gate_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now_ms = 2_100
    monkeypatch.setattr("coincall_rfq_maker.orchestration.get_timestamp_ms", lambda: now_ms)
    rest = FakeRest()
    market_data = FakeMarketData(price=50_000.0)
    risk_gate = RecordingRiskGate()
    quotes = QuoteLifecycle(rest, dry_run=False, api_reporter=risk_gate)  # type: ignore[arg-type]
    await quotes.reconcile(
        approved_plan(
            QuoteIntent(
                request_id="rfq-1",
                legs=(QuoteLegIntent(instrument_name=INSTRUMENT, price=100.0),),
            )
        )
    )
    risk_gate.calls.clear()
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=FakePricingModel(),
        risk_gate=risk_gate,  # type: ignore[arg-type]
        quote_lifecycle=quotes,
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))
    orchestrator._outage_gate.record_transient(1_000)
    cooldown_before = orchestrator._outage_gate.cooldown_until_ms

    await orchestrator.reprice_all_active()

    assert len(rest.create_calls) == 1
    assert rest.cancel_calls == []
    assert risk_gate.calls == []
    assert orchestrator._outage_gate.consecutive_failures == 1
    assert orchestrator._outage_gate.cooldown_until_ms == cooldown_before


@pytest.mark.asyncio
async def test_quiet_book_recovery_comes_from_reconciler_not_noop_reprices(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now_ms = 1_000

    monkeypatch.setattr(
        "coincall_rfq_maker.orchestration.get_timestamp_ms",
        lambda: now_ms,
    )
    rest = FakeRest(open_request_ids=["rfq-1"])
    market_data = FakeMarketData(price=50_000.0)
    risk_gate = RiskGate(
        max_quote_notional_usd=1_000_000.0,
        max_leg_qty=100.0,
        min_time_to_expiry_hours=0.000001,
        stale_market_data_seconds=30.0,
        kill_switch_threshold=2,
    )
    quotes = FakeQuoteLifecycle(api_reporter=risk_gate)
    quotes.reconcile_error = CoincallApiError(200, 10000, "Try again later")
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=FakePricingModel(),
        risk_gate=risk_gate,
        quote_lifecycle=quotes,  # type: ignore[arg-type]
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))
    risk_gate.record_api_failure()

    await orchestrator.reprice_all_active()
    for _ in range(12):
        await orchestrator.reprice_all_active()

    assert quotes.reconcile_calls == 1

    now_ms = 2_100
    quotes.reconcile_error = None
    for _ in range(12):
        await orchestrator.reprice_all_active()

    assert quotes.reconcile_calls == 13
    assert orchestrator._outage_gate.consecutive_failures == 1
    assert orchestrator._outage_gate.cooldown_until_ms == 2_000

    await orchestrator.reconcile_with_exchange()

    assert orchestrator._outage_gate.consecutive_failures == 0
    assert orchestrator._outage_gate.cooldown_until_ms == 0
    assert risk_gate.consecutive_failures == 0

    orchestrator._outage_gate.record_transient(now_ms)
    orchestrator._reconciler._on_api_recovery = lambda: None
    await orchestrator.reconcile_with_exchange()

    assert orchestrator._outage_gate.consecutive_failures == 1
    assert orchestrator._outage_gate.cooldown_until_ms > now_ms


@pytest.mark.asyncio
async def test_ambiguous_quote_failure_sets_global_cooldown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("coincall_rfq_maker.orchestration.get_timestamp_ms", lambda: 1_000)
    rest = FakeRest()
    market_data = FakeMarketData(price=50_000.0)
    quotes = FakeQuoteLifecycle()
    quotes.reconcile_error = CoincallAmbiguousError("unknown outcome")
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=FakePricingModel(),
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=quotes,  # type: ignore[arg-type]
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))

    await orchestrator.reprice_all_active()
    await orchestrator.reprice_all_active()

    assert quotes.reconcile_calls == 1
    assert orchestrator._outage_gate.cooldown_until_ms > 1_000


@pytest.mark.asyncio
async def test_unresolved_conflicting_create_does_not_set_global_cooldown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("coincall_rfq_maker.orchestration.get_timestamp_ms", lambda: 1_000)
    rest = FakeRest()
    rest.create_error = CoincallApiError(200, 50012, "Block trade quote exist")
    market_data = FakeMarketData(price=50_000.0)
    risk_gate = RiskGate(
        max_quote_notional_usd=1_000_000.0,
        max_leg_qty=100.0,
        min_time_to_expiry_hours=0.000001,
        stale_market_data_seconds=30.0,
        kill_switch_threshold=1,
    )
    outage_gate = TransientOutageGate()
    quotes = QuoteLifecycle(rest, dry_run=False, api_reporter=risk_gate)  # type: ignore[arg-type]
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=FakePricingModel(),
        risk_gate=risk_gate,
        quote_lifecycle=quotes,
        outage_gate=outage_gate,
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))

    await orchestrator.reprice_all_active()

    assert not risk_gate.kill_switch_tripped
    assert risk_gate.consecutive_failures == 0
    assert outage_gate.cooldown_until_ms == 0


@pytest.mark.asyncio
async def test_unverified_ambiguous_create_uses_cooldown_without_tripping_kill_switch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wall_now_ms = get_timestamp_ms()
    timestamps = iter((wall_now_ms - 10_000, wall_now_ms))
    monkeypatch.setattr(
        "coincall_rfq_maker.orchestration.get_timestamp_ms",
        lambda: next(timestamps),
    )
    rest = FakeRest()
    rest.create_error = CoincallAmbiguousError("timeout")
    rest.quote_list_response = {"code": 0, "data": []}
    market_data = FakeMarketData(price=50_000.0)
    risk_gate = RiskGate(
        max_quote_notional_usd=1_000_000.0,
        max_leg_qty=100.0,
        min_time_to_expiry_hours=0.000001,
        stale_market_data_seconds=30.0,
        kill_switch_threshold=1,
    )
    quotes = QuoteLifecycle(rest, dry_run=False, api_reporter=risk_gate)  # type: ignore[arg-type]
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=FakePricingModel(),
        risk_gate=risk_gate,
        quote_lifecycle=quotes,
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"), received_at_ms=wall_now_ms)

    await orchestrator.reprice_all_active()

    assert not risk_gate.kill_switch_tripped
    assert orchestrator._outage_gate.in_cooldown(wall_now_ms)
    current = quotes.get_for_rfq("rfq-1")
    assert current is not None
    assert current.stage is QuoteStage.PENDING_CREATE
    assert len(rest.create_calls) == 1


@pytest.mark.asyncio
async def test_shutdown_cancel_all_bypasses_active_outage_cooldown() -> None:
    rest = FakeRest()
    quotes = QuoteLifecycle(rest, dry_run=False, api_reporter=NoopApiReporter())  # type: ignore[arg-type]
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=FakeMarketData(price=50_000.0),  # type: ignore[arg-type]
        pricing_model=FakePricingModel(),
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=quotes,
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))
    orchestrator._outage_gate.record_transient(get_timestamp_ms() + 60_000)

    await orchestrator.reprice_all_active()
    assert rest.create_calls == []

    await cli._cancel_all_on_graceful_stop(quotes)

    assert rest.cancel_all_calls == 1


@pytest.mark.asyncio
async def test_resolved_ambiguous_create_records_success_only() -> None:
    rest = FakeRest()
    rest.create_error = CoincallAmbiguousError("timeout")
    rest.quote_list_response = {
        "code": 0,
        "data": [{"requestId": "rfq-1", "quoteId": "exchange-q-1"}],
    }
    market_data = FakeMarketData(price=50_000.0)
    risk_gate = RecordingRiskGate()
    quotes = QuoteLifecycle(rest, dry_run=False, api_reporter=risk_gate)  # type: ignore[arg-type]
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=FakePricingModel(),
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
    assert risk_gate.calls == ["success"]


@pytest.mark.asyncio
async def test_reconciler_records_request_error_without_raising() -> None:
    rest = FakeRest(open_request_ids=[])
    rest.get_rfq_error = CoincallRequestError("network down")
    risk_gate = RiskGate(
        max_quote_notional_usd=1_000_000.0,
        max_leg_qty=100.0,
        min_time_to_expiry_hours=0.000001,
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
async def test_reconciler_successful_fetches_record_api_success() -> None:
    rest = FakeRest(open_request_ids=["rfq-1"])
    risk_gate = RecordingRiskGate()
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=FakeMarketData(),  # type: ignore[arg-type]
        pricing_model=None,  # type: ignore[arg-type]
        risk_gate=risk_gate,  # type: ignore[arg-type]
        quote_lifecycle=FakeQuoteLifecycle(),  # type: ignore[arg-type]
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"), received_at_ms=0)

    await orchestrator.reconcile_with_exchange()

    assert risk_gate.calls == ["success"]


@pytest.mark.asyncio
async def test_reconciler_transient_rfq_error_does_not_record_failure() -> None:
    rest = FakeRest(open_request_ids=[])
    rest.get_rfq_error = CoincallApiError(200, 10000, "Try again later")
    risk_gate = RecordingRiskGate()
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=FakeMarketData(),  # type: ignore[arg-type]
        pricing_model=None,  # type: ignore[arg-type]
        risk_gate=risk_gate,  # type: ignore[arg-type]
        quote_lifecycle=FakeQuoteLifecycle(),  # type: ignore[arg-type]
    )

    await orchestrator.reconcile_with_exchange()

    assert risk_gate.calls == []


@pytest.mark.asyncio
async def test_reconciler_requires_both_fetches_before_recording_success() -> None:
    rest = FakeRest(open_request_ids=["rfq-1"])
    rest.get_quote_error = CoincallApiError(200, 10004, "Parameter illegal")
    risk_gate = RecordingRiskGate()
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=FakeMarketData(),  # type: ignore[arg-type]
        pricing_model=None,  # type: ignore[arg-type]
        risk_gate=risk_gate,  # type: ignore[arg-type]
        quote_lifecycle=FakeQuoteLifecycle(),  # type: ignore[arg-type]
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"), received_at_ms=0)

    await orchestrator.reconcile_with_exchange()

    assert risk_gate.calls == ["failure"]


@pytest.mark.asyncio
async def test_quote_list_only_persistent_outage_accumulates_to_trip() -> None:
    rest = FakeRest(open_request_ids=["rfq-1"])
    rest.get_quote_error = CoincallApiError(200, 10004, "Parameter illegal")
    risk_gate = RiskGate(
        max_quote_notional_usd=1_000_000.0,
        max_leg_qty=100.0,
        min_time_to_expiry_hours=0.000001,
        stale_market_data_seconds=30.0,
        kill_switch_threshold=5,
    )
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=FakeMarketData(),  # type: ignore[arg-type]
        pricing_model=None,  # type: ignore[arg-type]
        risk_gate=risk_gate,
        quote_lifecycle=FakeQuoteLifecycle(),  # type: ignore[arg-type]
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"), received_at_ms=0)

    for _ in range(5):
        await orchestrator.reconcile_with_exchange()

    assert risk_gate.consecutive_failures == 5
    assert risk_gate.kill_switch_tripped


@pytest.mark.asyncio
async def test_trip_flattens_once_with_stale_market_data_and_open_quote(
    caplog: pytest.LogCaptureFixture,
) -> None:
    rest = FakeRest()
    market_data = FakeMarketData(price=50_000.0)
    trip_event = asyncio.Event()
    shutdown = asyncio.Event()
    trip_callbacks = 0

    def signal_trip() -> None:
        nonlocal trip_callbacks
        trip_callbacks += 1
        trip_event.set()

    risk_gate = RiskGate(
        max_quote_notional_usd=1_000_000.0,
        max_leg_qty=100.0,
        min_time_to_expiry_hours=0.000001,
        stale_market_data_seconds=30.0,
        kill_switch_threshold=1,
        on_trip=signal_trip,
    )
    quotes = QuoteLifecycle(rest, dry_run=False, api_reporter=risk_gate)  # type: ignore[arg-type]
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=FakePricingModel(),
        risk_gate=risk_gate,
        quote_lifecycle=quotes,
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))
    await orchestrator.reprice_all_active()
    quote = quotes.get_for_rfq("rfq-1")
    assert quote is not None and quote.is_open
    market_data._age_seconds = 999.0
    flatten = asyncio.create_task(
        cli._flatten_on_kill_switch(trip_event, shutdown, quotes, risk_gate)
    )

    with caplog.at_level(logging.ERROR):
        risk_gate.record_api_failure()
        await asyncio.wait_for(flatten, timeout=0.2)
    risk_gate.record_api_failure()
    await asyncio.sleep(0)

    assert risk_gate.kill_switch_tripped
    assert trip_callbacks == 1
    assert rest.cancel_all_calls == 1
    assert "Kill switch TRIPPED after 1 consecutive API failures" in caplog.text
    assert "Kill-switch flatten completed after 1 consecutive API failures" in caplog.text


@pytest.mark.asyncio
async def test_trip_flatten_bypasses_active_outage_cooldown() -> None:
    rest = FakeRest()
    trip_event = asyncio.Event()
    shutdown = asyncio.Event()
    risk_gate = RiskGate(
        max_quote_notional_usd=1_000_000.0,
        max_leg_qty=100.0,
        min_time_to_expiry_hours=0.000001,
        stale_market_data_seconds=30.0,
        kill_switch_threshold=1,
        on_trip=trip_event.set,
    )
    quotes = QuoteLifecycle(rest, dry_run=False, api_reporter=risk_gate)  # type: ignore[arg-type]
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=FakeMarketData(),  # type: ignore[arg-type]
        pricing_model=FakePricingModel(),
        risk_gate=risk_gate,
        quote_lifecycle=quotes,
    )
    orchestrator._outage_gate.record_transient(get_timestamp_ms() + 60_000)
    assert orchestrator._outage_gate.in_cooldown(get_timestamp_ms())
    flatten = asyncio.create_task(
        cli._flatten_on_kill_switch(trip_event, shutdown, quotes, risk_gate)
    )

    risk_gate.record_api_failure()
    await asyncio.wait_for(flatten, timeout=0.2)

    assert orchestrator._outage_gate.consecutive_failures == 1
    assert rest.cancel_all_calls == 1


@pytest.mark.asyncio
async def test_reprice_tick_dispatch_reprices_active_rfqs() -> None:
    rest = FakeRest()
    market_data = FakeMarketData(price=50_000.0)
    quotes = FakeQuoteLifecycle()
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=FakePricingModel(),
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=quotes,  # type: ignore[arg-type]
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))

    await orchestrator.handle_event(RepriceTick())

    assert quotes.reconcile_calls == 1


@pytest.mark.asyncio
async def test_reconcile_tick_dispatch_reconciles_exchange_state() -> None:
    rest = FakeRest(open_request_ids=[])
    rest.rfq_list_by_id_items["rfq-1"] = [{"requestId": "rfq-1", "state": "CANCELLED", "legs": []}]
    quotes = FakeQuoteLifecycle()
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=FakeMarketData(),  # type: ignore[arg-type]
        pricing_model=None,  # type: ignore[arg-type]
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=quotes,  # type: ignore[arg-type]
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"), received_at_ms=0)

    await orchestrator.handle_event(ReconcileTick())

    assert orchestrator.rfq_store.get("rfq-1") is None
    assert quotes.cancelled_for == ["rfq-1"]


@pytest.mark.asyncio
async def test_terminal_cancel_failure_keeps_rfq_and_reconcile_retries_until_evicted() -> None:
    rest = FakeRest(open_request_ids=[])
    market_data = FakeMarketData(price=50_000.0)
    quotes = QuoteLifecycle(rest, dry_run=False, api_reporter=NoopApiReporter())  # type: ignore[arg-type]
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=FakePricingModel(),
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
    orchestrator._outage_gate.record_transient(get_timestamp_ms() + 60_000)
    await orchestrator.handle_event(ReconcileTick())

    assert orchestrator.rfq_store.get("rfq-1") is None
    assert quotes.get_for_rfq("rfq-1") is None
    assert rest.cancel_calls == [quote.quote_id, quote.quote_id]


@pytest.mark.asyncio
async def test_rfq_terminated_filled_resolves_remote_filled_quote_without_cancel() -> None:
    rest = FakeRest()
    market_data = FakeMarketData(price=50_000.0)
    quotes = QuoteLifecycle(rest, dry_run=False, api_reporter=NoopApiReporter())  # type: ignore[arg-type]
    audit_outbox = RecordingAuditOutbox()
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=FakePricingModel(),
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=quotes,
        audit_outbox=audit_outbox,  # type: ignore[arg-type]
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))
    await orchestrator.reprice_all_active()
    quote = quotes.get_for_rfq("rfq-1")
    assert quote is not None
    assert quote.quote_id is not None
    rest.quote_list_response = {
        "code": 0,
        "data": [
            {
                "requestId": "rfq-1",
                "quoteId": quote.quote_id,
                "state": "FILLED",
                "filledPrice": 101.0,
                "filledQuantity": 1.0,
                "fillTime": 123456,
                "blockTradeId": "bt-1",
            }
        ],
    }

    await orchestrator.handle_event(RfqTerminated("rfq-1", RfqStatus.FILLED))

    assert rest.quote_list_calls == [{"quote_id": quote.quote_id}]
    assert rest.cancel_calls == []
    assert orchestrator.rfq_store.get("rfq-1") is None
    assert quotes.get_for_rfq("rfq-1") is None
    assert audit_outbox.quotes[0].stage is QuoteStage.OPEN
    assert audit_outbox.quotes[1].stage is QuoteStage.FILLED
    assert audit_outbox.quotes[1].filled_price == 101.0


@pytest.mark.asyncio
async def test_terminal_rfq_cleanup_survives_audit_quote_write_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class FailingAuditStore:
        def __init__(self) -> None:
            self.quote_attempted = asyncio.Event()

        async def record_rfq(self, rfq: Rfq, now_ms: int) -> None:
            pass

        async def record_quote(
            self, quote: Quote, market_snapshot: dict[str, float] | None, now_ms: int
        ) -> None:
            self.quote_attempted.set()
            raise RuntimeError("disk unavailable")

        async def record_fill(self, event: object, now_ms: int) -> None:
            pass

    store = FailingAuditStore()
    audit_outbox = AuditOutbox(store)  # type: ignore[arg-type]
    shutdown = asyncio.Event()
    writer = asyncio.create_task(audit_outbox.run(shutdown))
    quotes = TerminalQuoteLifecycle()
    orchestrator = Orchestrator(
        rest_client=object(),  # type: ignore[arg-type]
        market_data=FakeMarketData(),  # type: ignore[arg-type]
        pricing_model=FakePricingModel(),
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=quotes,  # type: ignore[arg-type]
        audit_outbox=audit_outbox,
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))

    with caplog.at_level(logging.ERROR, logger="coincall_rfq_maker.persistence.outbox"):
        async with asyncio.timeout(1.0):
            await orchestrator.handle_event(RfqTerminated("rfq-1", RfqStatus.FILLED))
            await store.quote_attempted.wait()

    shutdown.set()
    async with asyncio.timeout(1.0):
        await writer

    assert quotes.settled is True
    assert quotes.evicted_for == ["rfq-1"]
    assert orchestrator.rfq_store.get("rfq-1") is None
    assert "Failed to write audit quote record" in caplog.text


@pytest.mark.asyncio
async def test_rfq_terminated_filled_cancels_remote_open_quote_and_evicts() -> None:
    rest = FakeRest()
    market_data = FakeMarketData(price=50_000.0)
    quotes = QuoteLifecycle(rest, dry_run=False, api_reporter=NoopApiReporter())  # type: ignore[arg-type]
    audit_outbox = RecordingAuditOutbox()
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=FakePricingModel(),
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=quotes,
        audit_outbox=audit_outbox,  # type: ignore[arg-type]
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))
    await orchestrator.reprice_all_active()
    quote = quotes.get_for_rfq("rfq-1")
    assert quote is not None
    assert quote.quote_id is not None
    rest.quote_list_response = {
        "code": 0,
        "data": [{"requestId": "rfq-1", "quoteId": quote.quote_id, "state": "OPEN"}],
    }

    await orchestrator.handle_event(RfqTerminated("rfq-1", RfqStatus.FILLED))

    assert rest.quote_list_calls == [{"quote_id": quote.quote_id}]
    assert rest.cancel_calls == [quote.quote_id]
    assert orchestrator.rfq_store.get("rfq-1") is None
    assert quotes.get_for_rfq("rfq-1") is None
    assert audit_outbox.quotes[-1].stage is QuoteStage.CANCELLED


@pytest.mark.asyncio
async def test_rfq_terminated_filled_with_pending_cancel_records_fill_and_evicts() -> None:
    rest = FakeRest()
    market_data = FakeMarketData(price=50_000.0)
    quotes = QuoteLifecycle(rest, dry_run=False, api_reporter=NoopApiReporter())  # type: ignore[arg-type]
    audit_outbox = RecordingAuditOutbox()
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=FakePricingModel(),
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=quotes,
        audit_outbox=audit_outbox,  # type: ignore[arg-type]
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))
    await orchestrator.reprice_all_active()
    quote = quotes.get_for_rfq("rfq-1")
    assert quote is not None
    assert quote.quote_id is not None
    rest.cancel_error = CoincallAmbiguousError("timeout")
    rest.quote_list_response = {"code": 0, "data": []}
    with pytest.raises(CoincallAmbiguousError):
        await quotes.cancel_for_rfq("rfq-1")
    assert quotes.get_for_rfq("rfq-1").stage is QuoteStage.PENDING_CANCEL  # type: ignore[union-attr]

    rest.quote_list_response = {
        "code": 0,
        "data": [
            {
                "requestId": "rfq-1",
                "quoteId": quote.quote_id,
                "state": "FILLED",
                "filledPrice": 101.0,
                "filledQuantity": 1.0,
                "fillTime": 123456,
                "blockTradeId": "bt-1",
            }
        ],
    }

    await orchestrator.handle_event(RfqTerminated("rfq-1", RfqStatus.FILLED))

    assert rest.quote_list_calls[-1] == {"quote_id": quote.quote_id}
    assert orchestrator.rfq_store.get("rfq-1") is None
    assert quotes.get_for_rfq("rfq-1") is None
    assert audit_outbox.quotes[-1].stage is QuoteStage.FILLED
    assert audit_outbox.quotes[-1].filled_price == 101.0
    assert audit_outbox.quotes[-1].filled_quantity == 1.0
    assert audit_outbox.quotes[-1].block_trade_id == "bt-1"


@pytest.mark.asyncio
async def test_rfq_terminated_filled_malformed_remote_quote_retains_for_retry() -> None:
    rest = FakeRest()
    market_data = FakeMarketData(price=50_000.0)
    quotes = QuoteLifecycle(rest, dry_run=False, api_reporter=NoopApiReporter())  # type: ignore[arg-type]
    audit_outbox = RecordingAuditOutbox()
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=FakePricingModel(),
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=quotes,
        audit_outbox=audit_outbox,  # type: ignore[arg-type]
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))
    await orchestrator.reprice_all_active()
    quote = quotes.get_for_rfq("rfq-1")
    assert quote is not None
    assert quote.quote_id is not None
    rest.cancel_error = CoincallAmbiguousError("timeout")
    rest.quote_list_response = {"code": 0, "data": []}
    with pytest.raises(CoincallAmbiguousError):
        await quotes.cancel_for_rfq("rfq-1")

    rest.quote_list_response = {
        "code": 0,
        "data": [
            {
                "requestId": "rfq-1",
                "quoteId": quote.quote_id,
                "state": "FILLED",
                "filledPrice": "not-a-number",
            }
        ],
    }

    await orchestrator.handle_event(RfqTerminated("rfq-1", RfqStatus.FILLED))

    retained = orchestrator.rfq_store.get("rfq-1")
    current = quotes.get_for_rfq("rfq-1")
    assert retained is not None
    assert retained.is_terminal_status
    assert current is not None
    assert current.stage is QuoteStage.PENDING_CANCEL
    assert audit_outbox.quotes == [quote]


@pytest.mark.asyncio
async def test_quote_update_for_evicted_rfq_updates_known_quote_and_is_persisted() -> None:
    rest = FakeRest()
    quotes = QuoteLifecycle(rest, dry_run=False, api_reporter=NoopApiReporter())  # type: ignore[arg-type]
    audit_outbox = RecordingAuditOutbox()
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=FakeMarketData(),  # type: ignore[arg-type]
        pricing_model=None,  # type: ignore[arg-type]
        risk_gate=RecordingRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=quotes,
        audit_outbox=audit_outbox,  # type: ignore[arg-type]
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
    assert audit_outbox.quotes == [updated]


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


@pytest.mark.asyncio
async def test_unknown_dispatch_event_logs_a_warning(caplog: pytest.LogCaptureFixture) -> None:
    orchestrator = object.__new__(Orchestrator)

    with caplog.at_level(logging.WARNING, logger="coincall_rfq_maker.orchestration"):
        async with asyncio.timeout(1.0):
            await orchestrator.handle_event(object())

    assert "dispatcher received unknown event type object" in caplog.text
