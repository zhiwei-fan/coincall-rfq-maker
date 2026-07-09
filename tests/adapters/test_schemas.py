import pytest

from coincall_rfq_maker.adapters.rest import CoincallRequestError
from coincall_rfq_maker.adapters.schemas import (
    QuoteListSnapshot,
    QuotePayload,
    SymbolInfoPayload,
    find_remote_quote,
    find_salvaged_quote_id,
    quote_from_payload,
    with_remote_stage,
)
from coincall_rfq_maker.domain.quote import IllegalQuoteTransition, Quote, QuoteStage


def make_quote(stage: QuoteStage = QuoteStage.OPEN) -> Quote:
    return Quote(
        request_id="rfq-1",
        quote_id="q-1",
        stage=stage,
        legs=(),
        create_time_ms=1000,
        update_time_ms=1100,
        expiry_time_ms=2000,
        filled_price=99.0,
        filled_quantity=0.5,
        fill_time_ms=1200,
        block_trade_id="bt-old",
    )


def test_symbol_info_zero_index_price_falls_back_to_mark_price() -> None:
    payload = SymbolInfoPayload.model_validate(
        {"symbol": "BTCUSD", "indexPrice": 0, "markPrice": 65000}
    )

    assert payload.underlying_price == 65000


def test_symbol_info_non_positive_prices_are_absent() -> None:
    assert (
        SymbolInfoPayload.model_validate(
            {"symbol": "BTCUSD", "indexPrice": 0, "markPrice": 0}
        ).underlying_price
        is None
    )
    assert (
        SymbolInfoPayload.model_validate(
            {"symbol": "BTCUSD", "indexPrice": -1, "markPrice": -2}
        ).underlying_price
        is None
    )
    assert SymbolInfoPayload.model_validate({"symbol": "BTCUSD"}).underlying_price is None


def test_quote_from_payload_unknown_state_raises() -> None:
    payload = QuotePayload.model_validate(
        {"quoteId": "q-1", "requestId": "rfq-1", "state": "ALIEN"}
    )

    with pytest.raises(CoincallRequestError):
        quote_from_payload(make_quote(), payload)


def test_quote_from_payload_pending_create_to_filled_hops_via_open() -> None:
    payload = QuotePayload.model_validate(
        {"quoteId": "q-1", "requestId": "rfq-1", "state": "FILLED"}
    )

    quote = quote_from_payload(make_quote(QuoteStage.PENDING_CREATE), payload)

    assert quote.stage is QuoteStage.FILLED


def test_with_remote_stage_preserves_illegal_terminal_transition() -> None:
    with pytest.raises(IllegalQuoteTransition):
        with_remote_stage(make_quote(QuoteStage.FILLED), QuoteStage.OPEN)


def test_quote_from_payload_coalesces_fill_fields() -> None:
    payload = QuotePayload.model_validate(
        {
            "quoteId": "q-2",
            "requestId": "",
            "state": "FILLED",
            "updateTime": None,
            "expiryTime": 3000,
            "filledPrice": 101.0,
            "filledQuantity": None,
            "fillTime": 1300,
            "blockTradeId": "",
        }
    )

    quote = quote_from_payload(make_quote(), payload)

    assert quote.request_id == "rfq-1"
    assert quote.quote_id == "q-2"
    assert quote.update_time_ms == 1100
    assert quote.expiry_time_ms == 3000
    assert quote.filled_price == 101.0
    assert quote.filled_quantity == 0.5
    assert quote.fill_time_ms == 1300
    assert quote.block_trade_id == "bt-old"


def test_find_remote_quote_matches_request_or_quote_id() -> None:
    first = QuotePayload.model_validate({"quoteId": "q-1", "requestId": "rfq-1"})
    second = QuotePayload.model_validate({"quoteId": "q-2", "requestId": "rfq-2"})
    snapshot = QuoteListSnapshot(payloads=(first, second))

    assert find_remote_quote(snapshot, request_id="rfq-2") is second
    assert find_remote_quote(snapshot, quote_id="q-1") is first
    assert find_remote_quote(snapshot, request_id="rfq-missing") is None


def test_find_salvaged_quote_id_matches_request_id() -> None:
    snapshot = QuoteListSnapshot(malformed_id_pairs=frozenset({("rfq-1", "q-1")}))

    assert find_salvaged_quote_id(snapshot, request_id="rfq-1") == "q-1"
    assert find_salvaged_quote_id(snapshot, request_id="rfq-missing") is None
