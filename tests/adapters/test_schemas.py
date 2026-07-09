from coincall_rfq_maker.adapters.schemas import SymbolInfoPayload


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
