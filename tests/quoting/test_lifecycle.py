import asyncio
import logging
from typing import Any

import pytest

from coincall_rfq_maker.core.adapters.rest import (
    ApiFailureKind,
    CoincallAmbiguousError,
    CoincallApiError,
    CoincallRequestError,
    CoincallRestClient,
    _mark_exchange_io_attempted,
    _parse_quote_list,
    classify_api_failure,
)
from coincall_rfq_maker.core.adapters.schemas import CreateQuoteResult, QuoteListSnapshot
from coincall_rfq_maker.domain.quote import Quote, QuoteLeg, QuoteStage
from coincall_rfq_maker.events import QuoteUpdated
from coincall_rfq_maker.quoting.lifecycle import QuoteLifecycle
from coincall_rfq_maker.quoting.strategy import QuoteIntent, QuoteLegIntent
from coincall_rfq_maker.risk.gate import RiskGate
from coincall_rfq_maker.testing.fake_exchange import FakeExchange

INSTRUMENT = "BTCUSD-21AUG25-120000-C"


class FakeRestClient:
    """No network: records calls and returns scripted responses."""

    def __init__(self) -> None:
        self.create_calls: list[tuple[str, list[dict[str, str]]]] = []
        self.cancel_calls: list[str] = []
        self.cancel_all_calls = 0
        self._next_quote_id = 1
        self.create_error: Exception | None = None
        self.cancel_error: Exception | None = None
        self.quote_list_response: dict[str, Any] = {"code": 0, "data": []}
        self.quote_list_calls: list[dict[str, Any]] = []

    async def create_quote(self, request_id: str, legs: list[dict[str, str]]) -> CreateQuoteResult:
        _mark_exchange_io_attempted()
        self.create_calls.append((request_id, legs))
        if self.create_error is not None:
            raise self.create_error
        quote_id = f"q-{self._next_quote_id}"
        self._next_quote_id += 1
        return CreateQuoteResult(quote_id=quote_id)

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
        return _quote_payloads(self.quote_list_response)


class ScriptedCreateRestClient(FakeRestClient):
    def __init__(self, create_outcomes: list[Exception | CreateQuoteResult]) -> None:
        super().__init__()
        self._create_outcomes = create_outcomes
        self.operations: list[tuple[str, str]] = []

    async def create_quote(self, request_id: str, legs: list[dict[str, str]]) -> CreateQuoteResult:
        _mark_exchange_io_attempted()
        self.operations.append(("create", request_id))
        self.create_calls.append((request_id, legs))
        outcome = self._create_outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    async def cancel_quote(self, quote_id: str) -> dict[str, Any]:
        self.operations.append(("cancel", quote_id))
        return await super().cancel_quote(quote_id)


class RecordingApiReporter:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def record_api_failure(self) -> None:
        self.calls.append("failure")

    def record_api_success(self) -> None:
        self.calls.append("success")


def _quote_payloads(response: dict[str, Any]) -> QuoteListSnapshot:
    return _parse_quote_list(response)


def make_intent(price: float = 100.0) -> QuoteIntent:
    return QuoteIntent(
        request_id="rfq-1", legs=(QuoteLegIntent(instrument_name=INSTRUMENT, price=price),)
    )


def quote_payload(quote_id: str, price: float) -> dict[str, Any]:
    return {
        "code": 0,
        "data": [
            {
                "requestId": "rfq-1",
                "quoteId": quote_id,
                "state": "OPEN",
                "legs": [{"instrumentName": INSTRUMENT, "price": str(price)}],
            }
        ],
    }


@pytest.mark.asyncio
async def test_dry_run_creates_nothing_over_rest() -> None:
    rest = FakeRestClient()
    lifecycle = QuoteLifecycle(rest, dry_run=True)  # type: ignore[arg-type]
    quote = await lifecycle.reconcile(make_intent())
    assert quote.stage is QuoteStage.PENDING_CREATE
    assert rest.create_calls == []


@pytest.mark.asyncio
async def test_live_create_calls_rest_and_stores_open_quote() -> None:
    rest = FakeRestClient()
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    quote = await lifecycle.reconcile(make_intent())
    assert quote.stage is QuoteStage.OPEN
    assert quote.quote_id == "q-1"
    assert len(rest.create_calls) == 1
    assert lifecycle.get_for_rfq("rfq-1") is quote


@pytest.mark.asyncio
async def test_reconcile_is_idempotent_when_price_unchanged() -> None:
    rest = FakeRestClient()
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    first = await lifecycle.reconcile(make_intent(price=100.0))
    second = await lifecycle.reconcile(make_intent(price=100.0))
    assert first.quote_id == second.quote_id
    assert len(rest.create_calls) == 1
    assert rest.cancel_calls == []


@pytest.mark.asyncio
async def test_reconcile_replaces_when_price_changes() -> None:
    rest = FakeRestClient()
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    first = await lifecycle.reconcile(make_intent(price=100.0))
    second = await lifecycle.reconcile(make_intent(price=105.0))
    assert rest.cancel_calls == [first.quote_id]
    assert len(rest.create_calls) == 2
    assert second.quote_id == "q-2"


@pytest.mark.asyncio
async def test_cancel_failure_reverts_open_and_does_not_create_replacement() -> None:
    rest = FakeRestClient()
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    first = await lifecycle.reconcile(make_intent(price=100.0))
    rest.cancel_error = CoincallApiError(500, None, "cancel failed")

    with pytest.raises(CoincallApiError):
        await lifecycle.reconcile(make_intent(price=105.0))

    current = lifecycle.get_for_rfq("rfq-1")
    assert current is not None
    assert current.stage is QuoteStage.OPEN
    assert current.quote_id == first.quote_id
    assert rest.cancel_calls == [first.quote_id]
    assert len(rest.create_calls) == 1


@pytest.mark.asyncio
async def test_create_failure_propagates_without_synthetic_cancelled_quote() -> None:
    rest = FakeRestClient()
    rest.create_error = CoincallApiError(503, None, "unavailable")
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]

    with pytest.raises(CoincallApiError):
        await lifecycle.reconcile(make_intent())

    current = lifecycle.get_for_rfq("rfq-1")
    assert current is not None
    assert current.stage is QuoteStage.PENDING_CREATE


@pytest.mark.asyncio
async def test_conflicting_create_adopts_matching_open_exchange_quote_without_strike() -> None:
    rest = ScriptedCreateRestClient([CoincallApiError(200, 50012, "Block trade quote exist")])
    rest.quote_list_response = quote_payload("q-1", 100.0)
    risk_gate = RiskGate(
        max_quote_notional_usd=1_000_000.0,
        max_leg_qty=100.0,
        min_time_to_expiry_hours=0.0,
        stale_market_data_seconds=30.0,
    )
    lifecycle = QuoteLifecycle(rest, dry_run=False, api_reporter=risk_gate)  # type: ignore[arg-type]

    quote = await lifecycle.reconcile(make_intent())

    assert quote.stage is QuoteStage.OPEN
    assert quote.quote_id == "q-1"
    assert quote.legs == (QuoteLeg(instrument_name=INSTRUMENT, price=100.0),)
    assert risk_gate.consecutive_failures == 0


@pytest.mark.asyncio
async def test_conflicting_create_cancels_mismatch_then_recreates_once() -> None:
    rest = ScriptedCreateRestClient(
        [
            CoincallApiError(200, 50012, "Block trade quote exist"),
            CreateQuoteResult(quote_id="q-2"),
        ]
    )
    rest.quote_list_response = quote_payload("q-1", 99.0)
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]

    quote = await lifecycle.reconcile(make_intent())

    assert rest.operations == [("create", "rfq-1"), ("cancel", "q-1"), ("create", "rfq-1")]
    assert rest.cancel_calls == ["q-1"]
    assert quote.stage is QuoteStage.OPEN
    assert quote.quote_id == "q-2"
    assert quote.legs == (QuoteLeg(instrument_name=INSTRUMENT, price=100.0),)


@pytest.mark.asyncio
async def test_conflicting_create_stops_after_one_recreate_without_strike() -> None:
    conflict = CoincallApiError(200, 50012, "Block trade quote exist")
    rest = ScriptedCreateRestClient([conflict, conflict])
    rest.quote_list_response = quote_payload("q-1", 99.0)
    risk_gate = RiskGate(
        max_quote_notional_usd=1_000_000.0,
        max_leg_qty=100.0,
        min_time_to_expiry_hours=0.0,
        stale_market_data_seconds=30.0,
    )
    lifecycle = QuoteLifecycle(rest, dry_run=False, api_reporter=risk_gate)  # type: ignore[arg-type]

    with pytest.raises(CoincallApiError) as exc_info:
        await lifecycle.reconcile(make_intent())

    assert exc_info.value.code == 50012
    assert len(rest.create_calls) == 2
    assert rest.cancel_calls == ["q-1"]
    assert risk_gate.consecutive_failures == 0


@pytest.mark.asyncio
async def test_unresolved_conflicting_create_propagates_without_strike() -> None:
    rest = ScriptedCreateRestClient([CoincallApiError(200, 50012, "Block trade quote exist")])
    risk_gate = RiskGate(
        max_quote_notional_usd=1_000_000.0,
        max_leg_qty=100.0,
        min_time_to_expiry_hours=0.0,
        stale_market_data_seconds=30.0,
    )
    lifecycle = QuoteLifecycle(rest, dry_run=False, api_reporter=risk_gate)  # type: ignore[arg-type]

    with pytest.raises(CoincallApiError) as exc_info:
        await lifecycle.reconcile(make_intent())

    assert exc_info.value.code == 50012
    assert risk_gate.consecutive_failures == 0


@pytest.mark.asyncio
async def test_ambiguous_create_adopts_open_exchange_quote() -> None:
    rest = FakeRestClient()
    rest.create_error = CoincallAmbiguousError("timeout")
    rest.quote_list_response = {
        "code": 0,
        "data": [{"requestId": "rfq-1", "quoteId": "exchange-q-1"}],
    }
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]

    quote = await lifecycle.reconcile(make_intent())

    assert quote.stage is QuoteStage.OPEN
    assert quote.quote_id == "exchange-q-1"
    assert rest.quote_list_calls == [{"request_id": "rfq-1", "state": "OPEN"}]


@pytest.mark.asyncio
async def test_ambiguous_create_adopts_salvaged_quote_id_from_malformed_item() -> None:
    rest = FakeRestClient()
    rest.create_error = CoincallAmbiguousError("timeout")
    rest.quote_list_response = {
        "code": 0,
        "data": [
            {
                "requestId": "rfq-1",
                "quoteId": "exchange-q-1",
                "state": "OPEN",
                "filledPrice": "not-a-number",
            }
        ],
    }
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]

    quote = await lifecycle.reconcile(make_intent())

    assert quote.stage is QuoteStage.OPEN
    assert quote.quote_id == "exchange-q-1"
    assert lifecycle.get_by_quote_id("exchange-q-1") is quote


@pytest.mark.asyncio
async def test_ambiguous_create_does_not_adopt_empty_quote_id() -> None:
    rest = FakeRestClient()
    rest.create_error = CoincallAmbiguousError("timeout")
    rest.quote_list_response = {
        "code": 0,
        "data": [{"requestId": "rfq-1", "quoteId": "", "state": "OPEN"}],
    }
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]

    with pytest.raises(CoincallAmbiguousError) as exc_info:
        await lifecycle.reconcile(make_intent())

    assert type(exc_info.value) is CoincallAmbiguousError
    assert classify_api_failure(exc_info.value) is ApiFailureKind.AMBIGUOUS
    current = lifecycle.get_for_rfq("rfq-1")
    assert current is not None
    assert current.stage is QuoteStage.PENDING_CREATE
    assert current.quote_id is None
    assert lifecycle.get_by_quote_id("") is None


@pytest.mark.asyncio
async def test_ambiguous_create_not_listed_remains_ambiguous() -> None:
    rest = FakeRestClient()
    rest.create_error = CoincallAmbiguousError("timeout")
    rest.quote_list_response = {"code": 0, "data": []}
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]

    with pytest.raises(CoincallAmbiguousError) as exc_info:
        await lifecycle.reconcile(make_intent())

    assert type(exc_info.value) is CoincallAmbiguousError
    assert classify_api_failure(exc_info.value) is ApiFailureKind.AMBIGUOUS
    current = lifecycle.get_for_rfq("rfq-1")
    assert current is not None
    assert current.stage is QuoteStage.PENDING_CREATE


@pytest.mark.asyncio
async def test_malformed_successful_create_response_adopts_open_quote_and_resets_failure_streak(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rest = CoincallRestClient("key", "secret")
    risk_gate = RiskGate(
        max_quote_notional_usd=1_000_000.0,
        max_leg_qty=100.0,
        min_time_to_expiry_hours=0.0,
        stale_market_data_seconds=30.0,
        kill_switch_threshold=5,
    )
    for _ in range(4):
        risk_gate.record_api_failure()

    async def fake_request(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        _mark_exchange_io_attempted()
        path = _args[1]
        if path == "/open/option/blocktrade/quote/create/v1":
            return {"code": 0, "data": {"quoteId": {"not": "parseable"}}}
        if path == "/open/option/blocktrade/list-quote/v1":
            return {
                "code": 0,
                "data": [
                    {
                        "requestId": "rfq-1",
                        "quoteId": 2075207494989787138,
                        "state": "OPEN",
                    }
                ],
            }
        raise AssertionError(f"unexpected path {path}")

    monkeypatch.setattr(rest, "_request", fake_request)
    lifecycle = QuoteLifecycle(rest, dry_run=False, api_reporter=risk_gate)

    quote = await lifecycle.reconcile(make_intent())

    assert quote.stage is QuoteStage.OPEN
    assert quote.quote_id == "2075207494989787138"
    assert not risk_gate.kill_switch_tripped
    risk_gate.record_api_failure()
    assert not risk_gate.kill_switch_tripped


@pytest.mark.asyncio
async def test_malformed_successful_create_response_not_listed_never_trips_kill_switch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rest = CoincallRestClient("key", "secret")
    risk_gate = RiskGate(
        max_quote_notional_usd=1_000_000.0,
        max_leg_qty=100.0,
        min_time_to_expiry_hours=0.0,
        stale_market_data_seconds=30.0,
        kill_switch_threshold=5,
    )

    async def fake_request(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        path = _args[1]
        if path == "/open/option/blocktrade/quote/create/v1":
            return {"code": 0, "data": {"quoteId": {"not": "parseable"}}}
        if path == "/open/option/blocktrade/list-quote/v1":
            return {"code": 0, "data": []}
        raise AssertionError(f"unexpected path {path}")

    monkeypatch.setattr(rest, "_request", fake_request)
    lifecycle = QuoteLifecycle(rest, dry_run=False, api_reporter=risk_gate)

    for _ in range(5):
        with pytest.raises(CoincallAmbiguousError) as exc_info:
            await lifecycle.reconcile(make_intent())
        assert classify_api_failure(exc_info.value) is ApiFailureKind.AMBIGUOUS
        assert not risk_gate.kill_switch_tripped


@pytest.mark.asyncio
async def test_noop_withdraw_cancel_and_dry_run_cancel_all_record_no_success() -> None:
    rest = FakeRestClient()
    reporter = RecordingApiReporter()
    lifecycle = QuoteLifecycle(rest, dry_run=True, api_reporter=reporter)  # type: ignore[arg-type]

    assert await lifecycle.withdraw_for_rfq("missing-rfq") is None
    await lifecycle.cancel_for_rfq("missing-rfq")
    await lifecycle.cancel_all()

    assert reporter.calls == []
    assert rest.cancel_calls == []
    assert rest.cancel_all_calls == 0


@pytest.mark.asyncio
async def test_ambiguous_cancel_keeps_quote_open_when_exchange_still_lists_it() -> None:
    rest = FakeRestClient()
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    quote = await lifecycle.reconcile(make_intent())
    rest.cancel_error = CoincallAmbiguousError("timeout")
    rest.quote_list_response = {
        "code": 0,
        "data": [{"requestId": "rfq-1", "quoteId": quote.quote_id, "state": "OPEN"}],
    }

    with pytest.raises(CoincallAmbiguousError):
        await lifecycle.cancel_for_rfq("rfq-1")

    current = lifecycle.get_for_rfq("rfq-1")
    assert current is not None
    assert current.stage is QuoteStage.OPEN
    assert current.quote_id == quote.quote_id
    assert rest.quote_list_calls == [{"quote_id": quote.quote_id}]


@pytest.mark.asyncio
async def test_ambiguous_cancel_mirrors_filled_exchange_state() -> None:
    rest = FakeRestClient()
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    quote = await lifecycle.reconcile(make_intent())
    rest.cancel_error = CoincallAmbiguousError("timeout")
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

    await lifecycle.cancel_for_rfq("rfq-1")

    current = lifecycle.get_for_rfq("rfq-1")
    assert current is not None
    assert current.stage is QuoteStage.FILLED
    assert current.quote_id == quote.quote_id
    assert current.filled_price == 101.0
    assert current.filled_quantity == 1.0
    assert current.fill_time_ms == 123456
    assert current.block_trade_id == "bt-1"
    assert rest.quote_list_calls == [{"quote_id": quote.quote_id}]


@pytest.mark.asyncio
async def test_ambiguous_cancel_missing_quote_stays_pending_cancel() -> None:
    rest = FakeRestClient()
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    quote = await lifecycle.reconcile(make_intent())
    rest.cancel_error = CoincallAmbiguousError("timeout")
    rest.quote_list_response = {"code": 0, "data": []}

    with pytest.raises(CoincallAmbiguousError):
        await lifecycle.cancel_for_rfq("rfq-1")

    current = lifecycle.get_for_rfq("rfq-1")
    assert current is not None
    assert current.stage is QuoteStage.PENDING_CANCEL
    assert current.quote_id == quote.quote_id


@pytest.mark.asyncio
async def test_reconcile_pending_cancel_rechecks_exchange_before_create_when_still_open() -> None:
    rest = FakeRestClient()
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    quote = await lifecycle.reconcile(make_intent(price=100.0))
    rest.cancel_error = CoincallAmbiguousError("timeout")
    rest.quote_list_response = {"code": 0, "data": []}
    with pytest.raises(CoincallAmbiguousError):
        await lifecycle.cancel_for_rfq("rfq-1")
    assert lifecycle.get_for_rfq("rfq-1").stage is QuoteStage.PENDING_CANCEL  # type: ignore[union-attr]

    rest.quote_list_response = {
        "code": 0,
        "data": [{"requestId": "rfq-1", "quoteId": quote.quote_id, "state": "OPEN"}],
    }

    with pytest.raises(CoincallAmbiguousError):
        await lifecycle.reconcile(make_intent(price=105.0))

    current = lifecycle.get_for_rfq("rfq-1")
    assert current is not None
    assert current.stage is QuoteStage.OPEN
    assert current.quote_id == quote.quote_id
    assert len(rest.create_calls) == 1


@pytest.mark.asyncio
async def test_reconcile_pending_cancel_mirrors_filled_without_creating_fresh_quote() -> None:
    rest = FakeRestClient()
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    quote = await lifecycle.reconcile(make_intent(price=100.0))
    assert quote.quote_id is not None
    rest.cancel_error = CoincallAmbiguousError("timeout")
    rest.quote_list_response = {"code": 0, "data": []}
    with pytest.raises(CoincallAmbiguousError):
        await lifecycle.cancel_for_rfq("rfq-1")

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

    current = await lifecycle.reconcile(make_intent(price=105.0))

    assert current.stage is QuoteStage.FILLED
    assert current.quote_id == quote.quote_id
    assert current.filled_price == 101.0
    assert len(rest.create_calls) == 1


@pytest.mark.asyncio
async def test_reconcile_filled_existing_quote_does_not_create_replacement() -> None:
    rest = FakeRestClient()
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    quote = await lifecycle.reconcile(make_intent(price=100.0))
    assert quote.quote_id is not None
    updated = lifecycle.apply_ws_update(
        QuoteUpdated(
            quote_id=quote.quote_id,
            request_id="rfq-1",
            stage=QuoteStage.FILLED,
            filled_price=101.0,
        )
    )
    assert updated is not None

    current = await lifecycle.reconcile(make_intent(price=105.0))

    assert current.stage is QuoteStage.FILLED
    assert current.quote_id == quote.quote_id
    assert current.filled_price == 101.0
    assert len(rest.create_calls) == 1
    assert rest.cancel_calls == []


@pytest.mark.asyncio
async def test_reconcile_expired_existing_quote_still_creates_replacement() -> None:
    rest = FakeRestClient()
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    quote = await lifecycle.reconcile(make_intent(price=100.0))
    assert quote.quote_id is not None
    updated = lifecycle.apply_ws_update(
        QuoteUpdated(quote_id=quote.quote_id, request_id="rfq-1", stage=QuoteStage.EXPIRED)
    )
    assert updated is not None

    current = await lifecycle.reconcile(make_intent(price=105.0))

    assert current.stage is QuoteStage.OPEN
    assert current.quote_id == "q-2"
    assert len(rest.create_calls) == 2
    assert rest.cancel_calls == []


@pytest.mark.asyncio
async def test_reconcile_pending_create_reverifies_and_adopts_without_second_create() -> None:
    rest = FakeRestClient()
    rest.create_error = CoincallAmbiguousError("timeout")
    rest.quote_list_response = {"code": 0, "data": []}
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    with pytest.raises(CoincallRequestError):
        await lifecycle.reconcile(make_intent())
    assert lifecycle.get_for_rfq("rfq-1").stage is QuoteStage.PENDING_CREATE  # type: ignore[union-attr]

    rest.create_error = None
    rest.quote_list_response = {
        "code": 0,
        "data": [{"requestId": "rfq-1", "quoteId": "exchange-q-1"}],
    }

    quote = await lifecycle.reconcile(make_intent())

    assert quote.stage is QuoteStage.OPEN
    assert quote.quote_id == "exchange-q-1"
    assert len(rest.create_calls) == 1


@pytest.mark.asyncio
async def test_pending_create_adoption_mirrors_exchange_legs_before_repricing() -> None:
    rest = FakeRestClient()
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
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    lifecycle._store.store(
        Quote(
            request_id="rfq-1",
            stage=QuoteStage.PENDING_CREATE,
            legs=(QuoteLeg(INSTRUMENT, 2.0),),
            create_time_ms=0,
        )
    )

    quote = await lifecycle.reconcile(make_intent(price=2.0))

    assert rest.cancel_calls == ["exchange-q-1"]
    assert len(rest.create_calls) == 1
    assert quote.stage is QuoteStage.OPEN
    assert quote.quote_id == "q-1"
    assert quote.legs == (QuoteLeg(INSTRUMENT, 2.0),)


@pytest.mark.asyncio
async def test_payloadless_adoption_uses_price_unknown_sentinel_and_reprices() -> None:
    rest = FakeRestClient()
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    lifecycle._store.store(
        Quote(
            request_id="rfq-1",
            stage=QuoteStage.PENDING_CREATE,
            legs=(QuoteLeg(INSTRUMENT, 2.0),),
            create_time_ms=0,
        )
    )

    adopted = lifecycle.adopt_open_exchange_quote("rfq-1", "salvaged-q-1")

    assert adopted is not None
    assert adopted.legs == ()
    quote = await lifecycle.reconcile(make_intent(price=2.0))
    assert rest.cancel_calls == ["salvaged-q-1"]
    assert len(rest.create_calls) == 1
    assert quote.quote_id == "q-1"
    assert quote.legs == (QuoteLeg(INSTRUMENT, 2.0),)


@pytest.mark.asyncio
async def test_reconcile_pending_cancel_terminal_verification_resets_api_failure_streak() -> None:
    rest = FakeRestClient()
    risk_gate = RiskGate(
        max_quote_notional_usd=1_000_000.0,
        max_leg_qty=100.0,
        min_time_to_expiry_hours=0.0,
        stale_market_data_seconds=30.0,
        kill_switch_threshold=2,
    )
    lifecycle = QuoteLifecycle(rest, dry_run=False, api_reporter=risk_gate)  # type: ignore[arg-type]
    quote = await lifecycle.reconcile(make_intent(price=100.0))
    assert quote.quote_id is not None

    rest.cancel_error = CoincallAmbiguousError("timeout")
    rest.quote_list_response = {"code": 0, "data": []}
    with pytest.raises(CoincallAmbiguousError):
        await lifecycle.cancel_for_rfq("rfq-1")
    assert not risk_gate.kill_switch_tripped

    rest.cancel_error = None
    rest.quote_list_response = {
        "code": 0,
        "data": [{"requestId": "rfq-1", "quoteId": quote.quote_id, "state": "FILLED"}],
    }
    current = await lifecycle.reconcile(make_intent(price=105.0))
    assert current.stage is QuoteStage.FILLED

    rest.cancel_error = CoincallApiError(500, None, "cancel failed")
    with pytest.raises(CoincallApiError):
        await lifecycle.cancel_exchange_quote("q-next")
    assert not risk_gate.kill_switch_tripped


@pytest.mark.asyncio
async def test_reconcile_pending_create_adoption_resets_api_failure_streak() -> None:
    rest = FakeRestClient()
    risk_gate = RiskGate(
        max_quote_notional_usd=1_000_000.0,
        max_leg_qty=100.0,
        min_time_to_expiry_hours=0.0,
        stale_market_data_seconds=30.0,
        kill_switch_threshold=2,
    )
    rest.create_error = CoincallAmbiguousError("timeout")
    rest.quote_list_response = {"code": 0, "data": []}
    lifecycle = QuoteLifecycle(rest, dry_run=False, api_reporter=risk_gate)  # type: ignore[arg-type]
    with pytest.raises(CoincallRequestError):
        await lifecycle.reconcile(make_intent())
    assert not risk_gate.kill_switch_tripped

    rest.create_error = None
    rest.quote_list_response = {
        "code": 0,
        "data": [{"requestId": "rfq-1", "quoteId": "exchange-q-1", "state": "OPEN"}],
    }
    quote = await lifecycle.reconcile(make_intent())
    assert quote.stage is QuoteStage.OPEN
    assert quote.quote_id == "exchange-q-1"

    rest.cancel_error = CoincallApiError(500, None, "cancel failed")
    with pytest.raises(CoincallApiError):
        await lifecycle.cancel_exchange_quote("q-next")
    assert not risk_gate.kill_switch_tripped


@pytest.mark.asyncio
async def test_settle_filled_unknown_remote_quote_state_records_one_failure() -> None:
    rest = FakeRestClient()
    reporter = RecordingApiReporter()
    lifecycle = QuoteLifecycle(rest, dry_run=False, api_reporter=reporter)  # type: ignore[arg-type]
    quote = await lifecycle.reconcile(make_intent())
    assert quote.quote_id is not None
    reporter.calls.clear()
    rest.quote_list_response = {
        "code": 0,
        "data": [{"requestId": "rfq-1", "quoteId": quote.quote_id, "state": "ALIEN"}],
    }

    with pytest.raises(CoincallRequestError):
        await lifecycle.settle_filled_rfq("rfq-1")

    assert reporter.calls == ["failure"]


@pytest.mark.asyncio
async def test_ambiguous_create_failure_is_not_counted_as_persistent() -> None:
    rest = FakeRestClient()
    reporter = RecordingApiReporter()
    rest.create_error = CoincallAmbiguousError("timeout")
    rest.quote_list_response = {"code": 0, "data": []}
    lifecycle = QuoteLifecycle(rest, dry_run=False, api_reporter=reporter)  # type: ignore[arg-type]

    with pytest.raises(CoincallAmbiguousError) as exc_info:
        await lifecycle.reconcile(make_intent())

    assert classify_api_failure(exc_info.value) is ApiFailureKind.AMBIGUOUS
    assert reporter.calls == []


@pytest.mark.asyncio
async def test_withdraw_pending_create_verifies_open_quote_then_cancels() -> None:
    rest = FakeRestClient()
    rest.create_error = CoincallAmbiguousError("timeout")
    rest.quote_list_response = {"code": 0, "data": []}
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    with pytest.raises(CoincallRequestError):
        await lifecycle.reconcile(make_intent())

    rest.create_error = None
    rest.quote_list_response = {
        "code": 0,
        "data": [{"requestId": "rfq-1", "quoteId": "exchange-q-1", "state": "OPEN"}],
    }

    withdrawn = await lifecycle.withdraw_for_rfq("rfq-1")

    current = lifecycle.get_for_rfq("rfq-1")
    assert withdrawn is not None
    assert withdrawn.stage is QuoteStage.CANCELLED
    assert current is not None
    assert current.stage is QuoteStage.CANCELLED
    assert current.quote_id == "exchange-q-1"
    assert rest.cancel_calls == ["exchange-q-1"]


@pytest.mark.asyncio
async def test_withdraw_pending_cancel_cancels_again_when_remote_still_open() -> None:
    rest = FakeRestClient()
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    quote = await lifecycle.reconcile(make_intent())
    assert quote.quote_id is not None
    rest.cancel_error = CoincallAmbiguousError("timeout")
    rest.quote_list_response = {"code": 0, "data": []}
    with pytest.raises(CoincallAmbiguousError):
        await lifecycle.cancel_for_rfq("rfq-1")

    rest.cancel_error = None
    rest.quote_list_response = {
        "code": 0,
        "data": [{"requestId": "rfq-1", "quoteId": quote.quote_id, "state": "OPEN"}],
    }

    withdrawn = await lifecycle.withdraw_for_rfq("rfq-1")

    current = lifecycle.get_for_rfq("rfq-1")
    assert withdrawn is not None
    assert withdrawn.stage is QuoteStage.CANCELLED
    assert current is not None
    assert current.stage is QuoteStage.CANCELLED
    assert rest.cancel_calls == [quote.quote_id, quote.quote_id]


@pytest.mark.asyncio
async def test_settle_filled_pending_create_resolves_remote_quote_by_request_id() -> None:
    rest = FakeRestClient()
    rest.create_error = CoincallAmbiguousError("timeout")
    rest.quote_list_response = {"code": 0, "data": []}
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    with pytest.raises(CoincallRequestError):
        await lifecycle.reconcile(make_intent())

    rest.quote_list_response = {
        "code": 0,
        "data": [
            {
                "requestId": "rfq-1",
                "quoteId": "exchange-q-1",
                "state": "FILLED",
                "filledPrice": 101.0,
            }
        ],
    }

    settled = await lifecycle.settle_filled_rfq("rfq-1")

    current = lifecycle.get_for_rfq("rfq-1")
    assert settled is not None
    assert settled.stage is QuoteStage.FILLED
    assert settled.quote_id == "exchange-q-1"
    assert settled.filled_price == 101.0
    assert current is settled
    assert rest.quote_list_calls[-1] == {"request_id": "rfq-1"}


@pytest.mark.asyncio
async def test_settle_filled_remote_open_quote_is_cancelled(
    caplog: pytest.LogCaptureFixture,
) -> None:
    rest = FakeRestClient()
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    quote = await lifecycle.reconcile(make_intent())
    assert quote.quote_id is not None
    rest.quote_list_response = {
        "code": 0,
        "data": [{"requestId": "rfq-1", "quoteId": quote.quote_id, "state": "OPEN"}],
    }

    with caplog.at_level(logging.WARNING):
        settled = await lifecycle.settle_filled_rfq("rfq-1")

    current = lifecycle.get_for_rfq("rfq-1")
    assert settled is not None
    assert settled.stage is QuoteStage.CANCELLED
    assert current is not None
    assert current.stage is QuoteStage.CANCELLED
    assert rest.quote_list_calls == [{"quote_id": quote.quote_id}]
    assert rest.cancel_calls == [quote.quote_id]
    assert "terminated FILLED but quote" in caplog.text


@pytest.mark.asyncio
async def test_settle_filled_dry_run_withdraws_locally_without_rest_calls() -> None:
    rest = FakeRestClient()
    lifecycle = QuoteLifecycle(rest, dry_run=True)  # type: ignore[arg-type]
    quote = await lifecycle.reconcile(make_intent())
    assert quote.stage is QuoteStage.PENDING_CREATE

    settled = await lifecycle.settle_filled_rfq("rfq-1")

    current = lifecycle.get_for_rfq("rfq-1")
    assert settled is not None
    assert settled.stage is QuoteStage.CANCELLED
    assert current is not None
    assert current.stage is QuoteStage.CANCELLED
    assert rest.create_calls == []
    assert rest.quote_list_calls == []
    assert rest.cancel_calls == []


@pytest.mark.asyncio
async def test_adopt_open_exchange_quote_refuses_different_quote_id_displacement() -> None:
    rest = FakeRestClient()
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    quote = await lifecycle.reconcile(make_intent())

    adopted = lifecycle.adopt_open_exchange_quote("rfq-1", "exchange-q-other")

    current = lifecycle.get_for_rfq("rfq-1")
    assert adopted is None
    assert current is not None
    assert current.quote_id == quote.quote_id
    assert lifecycle.get_by_quote_id("exchange-q-other") is None


@pytest.mark.asyncio
async def test_evict_for_rfq_clears_request_and_all_quote_id_indexes() -> None:
    rest = FakeRestClient()
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    quote = await lifecycle.reconcile(make_intent())
    assert quote.quote_id is not None
    lifecycle._store._by_quote_id["stale-alias"] = quote

    lifecycle.evict_for_rfq("rfq-1")
    lifecycle.evict_for_rfq("rfq-1")

    assert lifecycle.get_for_rfq("rfq-1") is None
    assert lifecycle.get_by_quote_id(quote.quote_id) is None
    assert lifecycle.get_by_quote_id("stale-alias") is None


@pytest.mark.asyncio
async def test_cancel_for_rfq_cancels_open_quote() -> None:
    rest = FakeRestClient()
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    quote = await lifecycle.reconcile(make_intent())
    await lifecycle.cancel_for_rfq("rfq-1")
    assert rest.cancel_calls == [quote.quote_id]


@pytest.mark.asyncio
async def test_apply_ws_update_transitions_quote_to_filled() -> None:
    rest = FakeRestClient()
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    quote = await lifecycle.reconcile(make_intent())
    event = QuoteUpdated(
        quote_id=quote.quote_id or "",
        request_id="rfq-1",
        stage=QuoteStage.FILLED,
        filled_price=101.0,
    )
    updated = lifecycle.apply_ws_update(event)
    assert updated is not None
    assert updated.stage is QuoteStage.FILLED
    assert updated.filled_price == 101.0


@pytest.mark.asyncio
async def test_apply_ws_update_same_stage_is_idempotent_and_merges_fields(
    caplog: pytest.LogCaptureFixture,
) -> None:
    rest = FakeRestClient()
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    quote = await lifecycle.reconcile(make_intent())
    assert quote.quote_id is not None

    event = QuoteUpdated(
        quote_id=quote.quote_id,
        request_id="rfq-1",
        stage=QuoteStage.OPEN,
        filled_price=101.0,
        filled_quantity=2.0,
        fill_time_ms=123456,
        block_trade_id="bt-1",
    )

    with caplog.at_level(logging.WARNING):
        updated = lifecycle.apply_ws_update(event)

    assert updated is not None
    assert updated.stage is QuoteStage.OPEN
    assert updated.filled_price == 101.0
    assert updated.filled_quantity == 2.0
    assert updated.fill_time_ms == 123456
    assert updated.block_trade_id == "bt-1"
    assert updated.update_time_ms is not None
    assert "Ignoring illegal quote transition" not in caplog.text


@pytest.mark.asyncio
async def test_apply_ws_update_same_stage_without_new_fields_is_noop() -> None:
    rest = FakeRestClient()
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    quote = await lifecycle.reconcile(make_intent())
    assert quote.quote_id is not None

    updated = lifecycle.apply_ws_update(
        QuoteUpdated(quote_id=quote.quote_id, request_id="rfq-1", stage=QuoteStage.OPEN)
    )

    assert updated is None
    assert lifecycle.get_by_quote_id(quote.quote_id) is quote
    assert lifecycle.get_for_rfq("rfq-1") is quote


@pytest.mark.asyncio
async def test_apply_ws_update_same_stage_with_identical_fields_is_noop() -> None:
    rest = FakeRestClient()
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    quote = await lifecycle.reconcile(make_intent())
    assert quote.quote_id is not None
    filled = lifecycle.apply_ws_update(
        QuoteUpdated(
            quote_id=quote.quote_id,
            request_id="rfq-1",
            stage=QuoteStage.FILLED,
            filled_price=101.0,
            filled_quantity=2.0,
            fill_time_ms=123456,
            block_trade_id="bt-1",
        )
    )
    assert filled is not None

    duplicate = lifecycle.apply_ws_update(
        QuoteUpdated(
            quote_id=quote.quote_id,
            request_id="rfq-1",
            stage=QuoteStage.FILLED,
            filled_price=101.0,
            filled_quantity=2.0,
            fill_time_ms=123456,
            block_trade_id="bt-1",
        )
    )

    assert duplicate is None
    assert lifecycle.get_by_quote_id(quote.quote_id) is filled
    assert lifecycle.get_for_rfq("rfq-1") is filled


@pytest.mark.asyncio
async def test_apply_ws_update_genuine_illegal_transition_warns_and_is_ignored(
    caplog: pytest.LogCaptureFixture,
) -> None:
    rest = FakeRestClient()
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    quote = await lifecycle.reconcile(make_intent())
    assert quote.quote_id is not None
    await lifecycle.cancel_for_rfq("rfq-1")

    with caplog.at_level(logging.WARNING):
        updated = lifecycle.apply_ws_update(
            QuoteUpdated(quote_id=quote.quote_id, request_id="rfq-1", stage=QuoteStage.OPEN)
        )

    current = lifecycle.get_by_quote_id(quote.quote_id)
    assert updated is None
    assert current is not None
    assert current.stage is QuoteStage.CANCELLED
    assert "Ignoring illegal quote transition cancelled -> open" in caplog.text


@pytest.mark.asyncio
async def test_apply_ws_update_for_unknown_quote_is_ignored() -> None:
    rest = FakeRestClient()
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    event = QuoteUpdated(quote_id="ghost", request_id="rfq-x", stage=QuoteStage.OPEN)
    assert lifecycle.apply_ws_update(event) is None


@pytest.mark.asyncio
async def test_fake_exchange_unknown_symbol_raises_api_error() -> None:
    exchange = FakeExchange(asyncio.Queue())

    with pytest.raises(CoincallApiError) as exc_info:
        await exchange.get_symbol_info("ETHUSD")

    assert exc_info.value.status == 200
    assert exc_info.value.code == 400001
