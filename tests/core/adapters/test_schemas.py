import pytest
from pydantic import TypeAdapter, ValidationError

from coincall_rfq_maker.core.adapters.rest import CoincallRequestError
from coincall_rfq_maker.core.adapters.schemas import (
    CreateQuoteResult,
    ExecuteQuoteResult,
    OptionalWireId,
    QuoteLegPayload,
    QuoteListSnapshot,
    QuotePayload,
    RfqPayload,
    SymbolInfoPayload,
    find_remote_quote,
    find_salvaged_quote_id,
    quote_from_payload,
    with_remote_stage,
)
from coincall_rfq_maker.domain.quote import IllegalQuoteTransition, Quote, QuoteLeg, QuoteStage

INSTRUMENT = "BTCUSD-10JUL26-62000-C"

GOLDEN_CREATE_QUOTE_DATA = {
    "quoteId": 2075207494989787138,
    "requestId": 2075207481654804480,
    "description": "BTC Call 10 Jul 26 62000",
    "strategyName": "Call",
    "strategyQuantity": "0.01000000",
    "strategyPrice": "8467.04000000",
    "quoteSide": "SELL",
    "legs": [
        {
            "instrumentName": INSTRUMENT,
            "side": "SELL",
            "price": "8467",
            "quantity": "0.01",
            "ratio": "1.000000000000000000",
        }
    ],
    "createTime": 1783602996739,
    "expiryTime": 1783603296739,
    "updateTime": 1783602996739,
}


def make_quote(
    stage: QuoteStage = QuoteStage.OPEN,
    legs: tuple[QuoteLeg, ...] = (),
) -> Quote:
    return Quote(
        request_id="rfq-1",
        quote_id="q-1",
        stage=stage,
        legs=legs,
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


def test_quote_from_payload_uses_valid_payload_legs_and_preserves_local_legs_when_unknown(
    caplog: pytest.LogCaptureFixture,
) -> None:
    current = make_quote(legs=(QuoteLeg(INSTRUMENT, 2.0),))
    payload = QuotePayload.model_validate(
        {
            "quoteId": "q-2",
            "requestId": "rfq-1",
            "state": "OPEN",
            "legs": [{"instrumentName": INSTRUMENT, "price": "1.0"}],
        }
    )

    assert quote_from_payload(current, payload).legs == (QuoteLeg(INSTRUMENT, 1.0),)

    empty_legs_payload = QuotePayload.model_validate(
        {"quoteId": "q-3", "requestId": "rfq-1", "state": "OPEN", "legs": []}
    )
    assert quote_from_payload(current, empty_legs_payload).legs == current.legs

    invalid_price_payload = QuotePayload.model_validate(
        {
            "quoteId": "q-4",
            "requestId": "rfq-1",
            "state": "OPEN",
            "legs": [{"instrumentName": INSTRUMENT, "price": "abc"}],
        }
    )
    with caplog.at_level("WARNING"):
        assert quote_from_payload(current, invalid_price_payload).legs == current.legs
    assert "q-4" in caplog.text
    assert "unparseable leg prices" in caplog.text


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


def test_create_quote_result_coerces_golden_integer_quote_id() -> None:
    result = CreateQuoteResult.model_validate(GOLDEN_CREATE_QUOTE_DATA)

    assert result.quote_id == "2075207494989787138"


@pytest.mark.parametrize(
    "block_trade_id,request_id,quote_id",
    [
        (2075207494989787138, 2075207481654804480, 2075207494989787138),
        ("2075207494989787138", "2075207481654804480", "2075207494989787138"),
    ],
)
def test_execute_quote_result_parses_integer_and_string_id_dialects(
    block_trade_id: int | str,
    request_id: int | str,
    quote_id: int | str,
) -> None:
    result = ExecuteQuoteResult.model_validate(
        {
            "blockTradeId": block_trade_id,
            "requestId": request_id,
            "quoteId": quote_id,
            "legs": [
                {
                    "instrumentName": INSTRUMENT,
                    "side": "SELL",
                    "price": "8467",
                    "quantity": "0.01",
                    "tradeId": 2075207494989787139,
                    "orderId": 2075207494989787140,
                }
            ],
        }
    )

    assert result.block_trade_id == "2075207494989787138"
    assert result.request_id == "2075207481654804480"
    assert result.quote_id == "2075207494989787138"
    assert len(result.legs) == 1
    assert result.legs[0].trade_id == "2075207494989787139"
    assert result.legs[0].order_id == "2075207494989787140"
    assert result.legs[0].price == "8467"
    assert result.legs[0].quantity == "0.01"


def test_quote_payload_parses_integer_ws_quote_ids() -> None:
    payload = QuotePayload.model_validate(
        {
            "quoteId": 2075207494989787138,
            "requestId": 2075207481654804480,
            "blockTradeId": 2075207494989787141,
            "state": "OPEN",
        }
    )

    assert payload.quote_id == "2075207494989787138"
    assert payload.request_id == "2075207481654804480"
    assert payload.block_trade_id == "2075207494989787141"


def test_required_empty_wire_ids_still_fail_and_optional_empty_ids_are_absent() -> None:
    with pytest.raises(ValidationError):
        RfqPayload.model_validate({"requestId": "", "state": "ACTIVE"})
    with pytest.raises(ValidationError):
        QuotePayload.model_validate({"quoteId": ""})

    payload = QuotePayload.model_validate({"quoteId": "q-1", "requestId": "", "blockTradeId": ""})
    assert payload.request_id is None
    assert payload.block_trade_id is None


def test_amount_strings_are_pinned_and_numeric_required_amount_rejected() -> None:
    payload = QuotePayload.model_validate(
        {"quoteId": "q-1", "legs": [{"instrumentName": INSTRUMENT, "price": "8467.04000000"}]}
    )

    assert payload.legs[0].price == "8467.04000000"
    with pytest.raises(ValidationError):
        QuoteLegPayload.model_validate({"instrumentName": INSTRUMENT, "price": 8467.04})


def test_optional_wire_id_empty_string_is_absent() -> None:
    assert TypeAdapter(OptionalWireId).validate_python("") is None
