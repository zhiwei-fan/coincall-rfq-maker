import logging
from types import TracebackType
from typing import Any

import aiohttp
import pytest

from coincall_rfq_maker.adapters.rest import CoincallAmbiguousError, CoincallRestClient
from coincall_rfq_maker.adapters.schemas import (
    ExecuteQuoteResult,
    OptionInstrument,
    QuotePayload,
    RfqCreateResult,
)
from coincall_rfq_maker.domain.rfq import Side

CREATE_RFQ_DATA = {
    "requestId": "1983130245586358272",
    "createTime": 1761650457000,
    "expiryTime": 1761650757000,
    "legs": [{"instrumentName": "BTCUSD-29OCT25-109000-C", "qty": "0.2", "side": "BUY"}],
    "state": "ACTIVE",
}

EXECUTE_QUOTE_DATA = {
    "userId": None,
    "blockTradeId": "BT123",
    "requestId": "198313",
    "quoteId": "198314",
    "role": "TAKER",
    "legs": [
        {
            "instrumentName": "BTCUSD-29OCT25-109000-C",
            "baseToken": "BTC",
            "quoteToken": "USD",
            "side": "BUY",
            "price": "0.05",
            "quantity": "0.2",
            "iv": "0.6",
            "markPrice": "0.049",
            "indexPrice": "100000",
            "orderId": "o1",
            "tradeId": "t1",
            "fee": "0.001",
        }
    ],
}

QUOTE_RECEIVED_DATA = [
    {
        "quoteId": "q1",
        "requestId": "r1",
        "userId": "u1",
        "state": "OPEN",
        "createTime": 1,
        "updateTime": 2,
        "expiryTime": 3,
        "legs": [
            {
                "instrumentName": "BTCUSD-29OCT25-109000-C",
                "side": "SELL",
                "price": "0.05",
                "quantity": "0.2",
            }
        ],
    }
]

OPTION_INSTRUMENT_DATA = [
    {
        "baseCurrency": "BTC",
        "expirationTimestamp": 1783584000000,
        "strike": 56000.0,
        "symbolName": "BTCUSD-9JUL26-56000-C",
        "isActive": True,
        "minQty": 0.01,
        "tickSize": 1,
        "startTimestamp": 1690000000000,
    }
]


class RequestRecorder:
    def __init__(self, response: dict[str, Any]) -> None:
        self.response = response
        self.calls: list[tuple[str, str, dict[str, Any] | None, bool, bool]] = []

    async def __call__(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        idempotent: bool = True,
        form_encoded: bool = False,
    ) -> dict[str, Any]:
        self.calls.append((method, path, params, idempotent, form_encoded))
        return self.response


class TransportErrorContext:
    async def __aenter__(self) -> object:
        raise aiohttp.ClientConnectionError("connection dropped after possible receipt")

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        return False


class TransportErrorPostSession:
    def __init__(self) -> None:
        self.post_attempts = 0

    def post(self, *args: Any, **kwargs: Any) -> TransportErrorContext:
        self.post_attempts += 1
        return TransportErrorContext()


def test_rfq_create_result_parses_fixture_aliases_and_ignores_extras() -> None:
    result = RfqCreateResult.model_validate(CREATE_RFQ_DATA)

    assert result.request_id == "1983130245586358272"
    assert result.state == "ACTIVE"
    assert result.create_time == 1761650457000
    assert result.expiry_time == 1761650757000


def test_execute_quote_result_parses_fixture_aliases_and_ignores_extras() -> None:
    result = ExecuteQuoteResult.model_validate(EXECUTE_QUOTE_DATA)

    assert result.block_trade_id == "BT123"
    assert result.request_id == "198313"
    assert result.quote_id == "198314"
    assert result.role == "TAKER"
    assert len(result.legs) == 1
    assert result.legs[0].instrument_name == "BTCUSD-29OCT25-109000-C"
    assert result.legs[0].side is Side.BUY
    assert result.legs[0].price == "0.05"
    assert result.legs[0].quantity == "0.2"
    assert result.legs[0].iv == "0.6"
    assert result.legs[0].order_id == "o1"
    assert result.legs[0].trade_id == "t1"
    assert result.legs[0].fee == "0.001"


def test_option_instrument_parses_fixture_aliases() -> None:
    result = OptionInstrument.model_validate(OPTION_INSTRUMENT_DATA[0])

    assert result.symbol_name == "BTCUSD-9JUL26-56000-C"
    assert result.base_currency == "BTC"
    assert result.strike == 56000.0
    assert result.expiration_timestamp == 1783584000000
    assert result.is_active is True
    assert result.min_qty == 0.01
    assert result.tick_size == 1.0
    assert result.start_timestamp == 1690000000000


def test_quote_payload_parses_quotes_received_fixture_and_ignores_user_id() -> None:
    result = QuotePayload.model_validate(QUOTE_RECEIVED_DATA[0])

    assert result.quote_id == "q1"
    assert result.request_id == "r1"
    assert result.state == "OPEN"
    assert result.create_time == 1
    assert result.update_time == 2
    assert result.expiry_time == 3
    assert len(result.legs) == 1
    assert result.legs[0].instrument_name == "BTCUSD-29OCT25-109000-C"
    assert result.legs[0].side is Side.SELL
    assert result.legs[0].price == "0.05"
    assert result.legs[0].quantity == "0.2"


@pytest.mark.asyncio
async def test_create_rfq_posts_expected_path_params_and_parses_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = CoincallRestClient("key", "secret")
    recorder = RequestRecorder({"code": 0, "data": CREATE_RFQ_DATA})
    monkeypatch.setattr(client, "_request", recorder)
    legs = [{"instrumentName": "BTCUSD-29OCT25-109000-C", "side": "BUY", "qty": "0.2"}]

    result = await client.create_rfq(legs)

    assert result.request_id == "1983130245586358272"
    assert recorder.calls == [
        (
            "POST",
            "/open/option/blocktrade/request/create/v1",
            {"legs": legs},
            False,
            False,
        )
    ]


@pytest.mark.asyncio
async def test_execute_quote_posts_expected_path_params_and_parses_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = CoincallRestClient("key", "secret")
    recorder = RequestRecorder({"code": 0, "data": EXECUTE_QUOTE_DATA})
    monkeypatch.setattr(client, "_request", recorder)

    result = await client.execute_quote("198313", "198314")

    assert result.block_trade_id == "BT123"
    assert recorder.calls == [
        (
            "POST",
            "/open/option/blocktrade/request/accept/v1",
            {"requestId": "198313", "quoteId": "198314"},
            False,
            True,
        )
    ]


@pytest.mark.asyncio
async def test_cancel_rfq_posts_expected_path_params_and_returns_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = CoincallRestClient("key", "secret")
    response = {"code": 0, "data": {"cancelled": True}}
    recorder = RequestRecorder(response)
    monkeypatch.setattr(client, "_request", recorder)

    result = await client.cancel_rfq("198313")

    assert result == response
    assert recorder.calls == [
        (
            "POST",
            "/open/option/blocktrade/request/cancel/v1",
            {"requestId": "198313"},
            False,
            True,
        )
    ]


@pytest.mark.asyncio
async def test_get_quotes_received_gets_expected_path_and_skips_malformed_items(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = CoincallRestClient("key", "secret")
    response = {
        "code": 0,
        "data": [
            *QUOTE_RECEIVED_DATA,
            {"requestId": "malformed", "state": "OPEN"},
            object(),
        ],
    }
    recorder = RequestRecorder(response)
    monkeypatch.setattr(client, "_request", recorder)

    with caplog.at_level(logging.WARNING):
        result = await client.get_quotes_received("r1")

    assert [quote.quote_id for quote in result] == ["q1"]
    assert recorder.calls == [
        (
            "GET",
            "/open/option/blocktrade/request/getQuotesReceived/v1",
            {"requestId": "r1"},
            True,
            False,
        )
    ]
    assert "Malformed quote REST item" in caplog.text


@pytest.mark.asyncio
async def test_get_option_instruments_gets_expected_path_and_skips_malformed_items(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = CoincallRestClient("key", "secret")
    response = {
        "code": 0,
        "data": [
            *OPTION_INSTRUMENT_DATA,
            {"symbolName": "BTCUSD-9JUL26-57000-C"},
            object(),
        ],
    }
    recorder = RequestRecorder(response)
    monkeypatch.setattr(client, "_request", recorder)

    with caplog.at_level(logging.WARNING):
        result = await client.get_option_instruments("BTC")

    assert [instrument.symbol_name for instrument in result] == ["BTCUSD-9JUL26-56000-C"]
    assert recorder.calls == [("GET", "/open/option/getInstruments/BTC", None, True, False)]
    assert "Malformed option instrument REST item" in caplog.text


@pytest.mark.asyncio
async def test_execute_quote_transport_error_is_not_retried_and_is_ambiguous() -> None:
    session = TransportErrorPostSession()
    client = CoincallRestClient("key", "secret")
    client._session = session  # type: ignore[assignment]

    with pytest.raises(CoincallAmbiguousError):
        await client.execute_quote("198313", "198314")

    assert session.post_attempts == 1
