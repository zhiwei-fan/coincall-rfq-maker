"""Transport-level body-encoding tests for CoincallRestClient._request.

Coincall's blocktrade cancel/accept (execute) endpoints require
`application/x-www-form-urlencoded` bodies (JSON -> code 10004 'Parameter
illegal'); the create endpoints take JSON. These tests pin the on-the-wire
serialization *and* prove the signed headers are untouched by the encoding
switch (the prehash is built from the params dict, encoding-agnostic).
"""

from types import TracebackType
from typing import Any

import pytest

from coincall_rfq_maker.adapters.rest import CoincallRestClient
from coincall_rfq_maker.adapters.signing import sign_rest_request

_FIXED_TS = 1_700_000_000_000
_SIGNED_HEADER_KEYS = ("sign", "ts", "X-CC-APIKEY", "X-REQ-TS-DIFF")


class _FakeResponse:
    def __init__(self, status: int, text: str) -> None:
        self.status = status
        self._text = text

    async def text(self) -> str:
        return self._text


class _ResponseContext:
    def __init__(self, response: _FakeResponse) -> None:
        self._response = response

    async def __aenter__(self) -> _FakeResponse:
        return self._response

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        return False


class CapturePostSession:
    """Records the kwargs passed to `.post` and returns a canned success."""

    def __init__(self, text: str = '{"code":0,"data":true}') -> None:
        self.calls: list[dict[str, Any]] = []
        self._text = text

    def post(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        json: Any = None,
        data: Any = None,
    ) -> _ResponseContext:
        self.calls.append({"url": url, "headers": headers, "json": json, "data": data})
        return _ResponseContext(_FakeResponse(200, self._text))


@pytest.fixture
def _fixed_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("coincall_rfq_maker.adapters.rest.get_timestamp_ms", lambda: _FIXED_TS)


def _expected_signed_headers(
    method: str, path: str, params: dict[str, Any] | None
) -> dict[str, str]:
    return sign_rest_request(method, path, "key", "secret", _FIXED_TS, 5000, params)


@pytest.mark.asyncio
async def test_cancel_rfq_sends_form_encoded_body_with_intact_signature(
    _fixed_clock: None,
) -> None:
    session = CapturePostSession()
    client = CoincallRestClient("key", "secret")
    client._session = session  # type: ignore[assignment]

    await client.cancel_rfq("198313")

    call = session.calls[0]
    assert call["headers"]["Content-Type"] == "application/x-www-form-urlencoded"
    assert call["json"] is None
    assert call["data"] == "requestId=198313"

    path = "/open/option/blocktrade/request/cancel/v1"
    expected = _expected_signed_headers("POST", path, {"requestId": "198313"})
    for key in _SIGNED_HEADER_KEYS:
        assert call["headers"][key] == expected[key]


@pytest.mark.asyncio
async def test_execute_quote_sends_form_encoded_body_with_intact_signature(
    _fixed_clock: None,
) -> None:
    session = CapturePostSession(
        text='{"code":0,"data":{"blockTradeId":"BT1","requestId":"198313",'
        '"quoteId":"198314","role":"TAKER","legs":[]}}'
    )
    client = CoincallRestClient("key", "secret")
    client._session = session  # type: ignore[assignment]

    await client.execute_quote("198313", "198314")

    call = session.calls[0]
    assert call["headers"]["Content-Type"] == "application/x-www-form-urlencoded"
    assert call["json"] is None
    assert call["data"] == "requestId=198313&quoteId=198314"

    path = "/open/option/blocktrade/request/accept/v1"
    params = {"requestId": "198313", "quoteId": "198314"}
    expected = _expected_signed_headers("POST", path, params)
    for key in _SIGNED_HEADER_KEYS:
        assert call["headers"][key] == expected[key]


@pytest.mark.asyncio
async def test_cancel_quote_sends_form_encoded_body(_fixed_clock: None) -> None:
    session = CapturePostSession()
    client = CoincallRestClient("key", "secret")
    client._session = session  # type: ignore[assignment]

    await client.cancel_quote("q-1")

    call = session.calls[0]
    assert call["headers"]["Content-Type"] == "application/x-www-form-urlencoded"
    assert call["json"] is None
    assert call["data"] == "quoteId=q-1"


@pytest.mark.asyncio
async def test_cancel_all_quotes_sends_form_encoded_empty_body(_fixed_clock: None) -> None:
    session = CapturePostSession()
    client = CoincallRestClient("key", "secret")
    client._session = session  # type: ignore[assignment]

    await client.cancel_all_quotes()

    call = session.calls[0]
    assert call["headers"]["Content-Type"] == "application/x-www-form-urlencoded"
    assert call["json"] is None
    assert call["data"] == ""


@pytest.mark.asyncio
async def test_create_rfq_sends_json_body(_fixed_clock: None) -> None:
    session = CapturePostSession(text='{"code":0,"data":{"requestId":"r1","state":"ACTIVE"}}')
    client = CoincallRestClient("key", "secret")
    client._session = session  # type: ignore[assignment]
    legs = [{"instrumentName": "BTCUSD-29OCT25-109000-C", "side": "BUY", "qty": "0.2"}]

    await client.create_rfq(legs)

    call = session.calls[0]
    assert call["headers"]["Content-Type"] == "application/json"
    assert call["data"] is None
    assert call["json"] == {"legs": legs}


@pytest.mark.asyncio
async def test_create_quote_sends_json_body(_fixed_clock: None) -> None:
    session = CapturePostSession(text='{"code":0,"data":{"quoteId":"q1"}}')
    client = CoincallRestClient("key", "secret")
    client._session = session  # type: ignore[assignment]
    legs = [{"instrumentName": "BTCUSD-29OCT25-109000-C", "side": "SELL", "price": "0.05"}]

    await client.create_quote("r1", legs)

    call = session.calls[0]
    assert call["headers"]["Content-Type"] == "application/json"
    assert call["data"] is None
    assert call["json"] == {"requestId": "r1", "legs": legs}
