from typing import Any

import pytest

from coincall_rfq_maker.adapters.rest import (
    CoincallAmbiguousError,
    CoincallApiError,
    CoincallRequestError,
)
from coincall_rfq_maker.domain.quote import QuoteStage
from coincall_rfq_maker.events import QuoteUpdated
from coincall_rfq_maker.quoting.lifecycle import QuoteLifecycle
from coincall_rfq_maker.quoting.strategy import QuoteIntent, QuoteLegIntent

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

    async def cancel_all_quotes(self) -> dict[str, Any]:
        self.cancel_all_calls += 1
        return {"code": 0, "data": {}}

    async def get_quote_list(self, **kwargs: Any) -> dict[str, Any]:
        self.quote_list_calls.append(kwargs)
        return self.quote_list_response


def make_intent(price: float = 100.0) -> QuoteIntent:
    return QuoteIntent(
        request_id="rfq-1", legs=(QuoteLegIntent(instrument_name=INSTRUMENT, price=price),)
    )


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
async def test_ambiguous_create_not_listed_is_treated_as_failure() -> None:
    rest = FakeRestClient()
    rest.create_error = CoincallAmbiguousError("timeout")
    rest.quote_list_response = {"code": 0, "data": []}
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]

    with pytest.raises(CoincallRequestError):
        await lifecycle.reconcile(make_intent())

    current = lifecycle.get_for_rfq("rfq-1")
    assert current is not None
    assert current.stage is QuoteStage.PENDING_CREATE


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
async def test_reconcile_pending_cancel_mirrors_filled_then_creates_fresh_quote() -> None:
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

    fresh = await lifecycle.reconcile(make_intent(price=105.0))

    old_quote = lifecycle.get_by_quote_id(quote.quote_id)
    assert old_quote is not None
    assert old_quote.stage is QuoteStage.FILLED
    assert old_quote.filled_price == 101.0
    assert fresh.stage is QuoteStage.OPEN
    assert fresh.quote_id == "q-2"
    assert len(rest.create_calls) == 2


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
async def test_apply_ws_update_for_unknown_quote_is_ignored() -> None:
    rest = FakeRestClient()
    lifecycle = QuoteLifecycle(rest, dry_run=False)  # type: ignore[arg-type]
    event = QuoteUpdated(quote_id="ghost", request_id="rfq-x", stage=QuoteStage.OPEN)
    assert lifecycle.apply_ws_update(event) is None
