import logging
from types import TracebackType
from typing import Any

import aiohttp
import pytest

from coincall_rfq_maker.core.adapters.rest import (
    _MAX_ATTEMPTS,
    _RETRY_BACKOFF_SECONDS,
    ApiFailureKind,
    CoincallAmbiguousError,
    CoincallApiError,
    CoincallConnectivityError,
    CoincallMalformedResponseError,
    CoincallRequestError,
    CoincallRestClient,
    _parse_quote_list,
    _parse_rfq_list,
    _wire_id,
    classify_api_failure,
    track_exchange_io,
)
from coincall_rfq_maker.domain.quote import QuoteStage
from coincall_rfq_maker.quoting.lifecycle import QuoteLifecycle
from coincall_rfq_maker.quoting.strategy import QuoteIntent, QuoteLegIntent


class TimeoutContext:
    async def __aenter__(self) -> object:
        raise TimeoutError("timed out after server receipt might have happened")

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        return False


class TimeoutPostSession:
    def __init__(self) -> None:
        self.post_attempts = 0

    def post(self, *args: Any, **kwargs: Any) -> TimeoutContext:
        self.post_attempts += 1
        return TimeoutContext()


class TransientGetContext:
    async def __aenter__(self) -> object:
        raise aiohttp.ClientConnectionError("temporary disconnect")

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        return False


class TransientGetSession:
    def __init__(self) -> None:
        self.get_attempts = 0

    def get(self, *args: Any, **kwargs: Any) -> TransientGetContext:
        self.get_attempts += 1
        return TransientGetContext()


class ResponseContext:
    def __init__(self, status: int, text: str) -> None:
        self._response = FakeResponse(status, text)

    async def __aenter__(self) -> "FakeResponse":
        return self._response

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        return False


class FakeResponse:
    def __init__(self, status: int, text: str) -> None:
        self.status = status
        self._text = text

    async def text(self) -> str:
        return self._text


class ApiErrorGetSession:
    def __init__(self, code: int = 400001, msg: str = "application rejected") -> None:
        self.get_attempts = 0
        self._code = code
        self._msg = msg

    def get(self, *args: Any, **kwargs: Any) -> ResponseContext:
        self.get_attempts += 1
        return ResponseContext(200, f'{{"code":{self._code},"msg":"{self._msg}"}}')


class SequencedGetSession:
    def __init__(self, responses: list[str]) -> None:
        self._responses = responses
        self.get_attempts = 0

    def get(self, *args: Any, **kwargs: Any) -> ResponseContext:
        response = self._responses[self.get_attempts]
        self.get_attempts += 1
        return ResponseContext(200, response)


class StaticPostSession:
    def __init__(self, text: str, status: int = 200) -> None:
        self._text = text
        self._status = status
        self.post_attempts = 0

    def post(self, *args: Any, **kwargs: Any) -> ResponseContext:
        self.post_attempts += 1
        return ResponseContext(self._status, self._text)


class StaticGetSession:
    def __init__(self, text: str) -> None:
        self._text = text
        self.get_attempts = 0

    def get(self, *args: Any, **kwargs: Any) -> ResponseContext:
        self.get_attempts += 1
        return ResponseContext(200, self._text)


class SequencedPostGetSession:
    def __init__(
        self,
        post_responses: list[tuple[int, str]],
        get_responses: list[tuple[int, str]],
    ) -> None:
        self._post_responses = post_responses
        self._get_responses = get_responses
        self.post_attempts = 0
        self.get_attempts = 0

    def post(self, *args: Any, **kwargs: Any) -> ResponseContext:
        status, response = self._post_responses[self.post_attempts]
        self.post_attempts += 1
        return ResponseContext(status, response)

    def get(self, *args: Any, **kwargs: Any) -> ResponseContext:
        status, response = self._get_responses[self.get_attempts]
        self.get_attempts += 1
        return ResponseContext(status, response)


def _quote_intent(price: float = 100.0) -> QuoteIntent:
    return QuoteIntent(
        request_id="rfq-1",
        legs=(
            QuoteLegIntent(
                instrument_name="BTCUSD-21AUG25-120000-C",
                price=price,
            ),
        ),
    )


def test_api_failure_classifier_contract() -> None:
    assert classify_api_failure(CoincallApiError(200, 10000, "Try again later")) is (
        ApiFailureKind.TRANSIENT
    )
    assert classify_api_failure(CoincallApiError(503, None, "unavailable")) is (
        ApiFailureKind.TRANSIENT
    )
    assert classify_api_failure(CoincallApiError(429, None, "rate limited")) is (
        ApiFailureKind.TRANSIENT
    )
    assert classify_api_failure(CoincallApiError(408, None, "request timeout")) is (
        ApiFailureKind.TRANSIENT
    )
    assert classify_api_failure(CoincallApiError(425, None, "too early")) is (
        ApiFailureKind.TRANSIENT
    )
    assert classify_api_failure(CoincallMalformedResponseError("bad json")) is (
        ApiFailureKind.TRANSIENT
    )
    assert classify_api_failure(CoincallConnectivityError("network down")) is (
        ApiFailureKind.TRANSIENT
    )
    assert classify_api_failure(CoincallAmbiguousError("unknown outcome")) is (
        ApiFailureKind.AMBIGUOUS
    )
    assert classify_api_failure(CoincallApiError(200, 10004, "Parameter illegal")) is (
        ApiFailureKind.PERSISTENT
    )
    assert classify_api_failure(CoincallApiError(200, 499999, "unknown")) is (
        ApiFailureKind.PERSISTENT
    )
    assert classify_api_failure(CoincallApiError(400, None, "bad request")) is (
        ApiFailureKind.PERSISTENT
    )
    assert classify_api_failure(CoincallRequestError("Session not started")) is (
        ApiFailureKind.PERSISTENT
    )


@pytest.mark.asyncio
async def test_state_changing_post_timeout_is_not_retried_and_is_ambiguous() -> None:
    session = TimeoutPostSession()
    client = CoincallRestClient("key", "secret")
    client._session = session  # type: ignore[assignment]

    with (
        track_exchange_io() as exchange_io,
        pytest.raises(CoincallAmbiguousError),
    ):
        await client.create_quote("rfq-1", [{"instrumentName": "BTCUSD-21AUG25-120000-C"}])

    assert session.post_attempts == 1
    assert exchange_io.attempted


@pytest.mark.asyncio
async def test_idempotent_get_transient_failure_retries_max_attempts_then_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = TransientGetSession()
    client = CoincallRestClient("key", "secret")
    client._session = session  # type: ignore[assignment]
    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr("coincall_rfq_maker.core.adapters.rest.asyncio.sleep", fake_sleep)

    with pytest.raises(CoincallConnectivityError, match="failed after 3 attempts") as exc_info:
        await client._request("GET", "/open/option/blocktrade/rfqList/v1")

    assert type(exc_info.value) is CoincallConnectivityError
    assert classify_api_failure(exc_info.value) is ApiFailureKind.TRANSIENT
    assert session.get_attempts == _MAX_ATTEMPTS
    assert sleeps == [_RETRY_BACKOFF_SECONDS * attempt for attempt in range(1, _MAX_ATTEMPTS)]


@pytest.mark.asyncio
async def test_idempotent_get_code_10000_retries_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = SequencedGetSession(
        [
            '{"code":10000,"msg":"Try again later"}',
            '{"code":10000,"msg":"Try again later"}',
            '{"code":0,"data":{"rfqList":[]}}',
        ]
    )
    client = CoincallRestClient("key", "secret")
    client._session = session  # type: ignore[assignment]
    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr("coincall_rfq_maker.core.adapters.rest.asyncio.sleep", fake_sleep)

    response = await client._request("GET", "/open/option/blocktrade/rfqList/v1")

    assert response == {"code": 0, "data": {"rfqList": []}}
    assert session.get_attempts == 3
    assert sleeps == [_RETRY_BACKOFF_SECONDS, _RETRY_BACKOFF_SECONDS * 2]


@pytest.mark.asyncio
async def test_non_idempotent_post_code_10000_is_not_retried() -> None:
    session = StaticPostSession('{"code":10000,"msg":"Try again later"}')
    client = CoincallRestClient("key", "secret")
    client._session = session  # type: ignore[assignment]

    with pytest.raises(CoincallApiError) as exc_info:
        await client.create_quote("rfq-1", [{"instrumentName": "BTCUSD-21AUG25-120000-C"}])

    assert exc_info.value.code == 10000
    assert session.post_attempts == 1


@pytest.mark.asyncio
async def test_non_idempotent_code_10000_is_never_wrapped_as_ambiguous() -> None:
    # "Try again later" means the write did not land. Wrapping it as ambiguous would send
    # the maker off verifying, and then re-creating, a quote that never existed.
    session = StaticPostSession('{"code":10000,"msg":"Try again later"}')
    client = CoincallRestClient("key", "secret")
    client._session = session  # type: ignore[assignment]

    with pytest.raises(CoincallApiError) as exc_info:
        await client.create_quote("rfq-1", [{"instrumentName": "BTCUSD-21AUG25-120000-C"}])

    assert not isinstance(exc_info.value, CoincallAmbiguousError)
    assert classify_api_failure(exc_info.value) is ApiFailureKind.TRANSIENT
    assert session.post_attempts == 1


@pytest.mark.asyncio
async def test_non_idempotent_5xx_create_is_ambiguous_and_not_retried() -> None:
    session = StaticPostSession("upstream unavailable", status=503)
    client = CoincallRestClient("key", "secret")
    client._session = session  # type: ignore[assignment]

    with pytest.raises(CoincallAmbiguousError, match="quote/create/v1") as exc_info:
        await client.create_quote("rfq-1", [{"instrumentName": "BTCUSD-21AUG25-120000-C"}])

    assert classify_api_failure(exc_info.value) is ApiFailureKind.AMBIGUOUS
    assert isinstance(exc_info.value.__cause__, CoincallApiError)
    assert exc_info.value.__cause__.status == 503
    assert exc_info.value.__cause__.message == "upstream unavailable"
    assert session.post_attempts == 1


@pytest.mark.parametrize("status", [408, 500, 504])
@pytest.mark.asyncio
async def test_non_idempotent_http_failure_cancel_is_ambiguous_and_not_retried(
    status: int,
) -> None:
    session = StaticPostSession("possibly accepted", status=status)
    client = CoincallRestClient("key", "secret")
    client._session = session  # type: ignore[assignment]

    with pytest.raises(CoincallAmbiguousError, match="quote/cancel/v1") as exc_info:
        await client.cancel_quote("quote-1")

    assert classify_api_failure(exc_info.value) is ApiFailureKind.AMBIGUOUS
    assert isinstance(exc_info.value.__cause__, CoincallApiError)
    assert exc_info.value.__cause__.status == status
    assert exc_info.value.__cause__.message == "possibly accepted"
    assert session.post_attempts == 1


@pytest.mark.asyncio
async def test_5xx_create_drives_lifecycle_ambiguous_verification() -> None:
    session = SequencedPostGetSession(
        post_responses=[(503, "upstream unavailable")],
        get_responses=[
            (
                200,
                '{"code":0,"data":[{"requestId":"rfq-1","quoteId":"exchange-q-1","state":"OPEN"}]}',
            )
        ],
    )
    client = CoincallRestClient("key", "secret")
    client._session = session  # type: ignore[assignment]
    lifecycle = QuoteLifecycle(client, dry_run=False)

    quote = await lifecycle.reconcile(_quote_intent())

    assert quote.stage is QuoteStage.OPEN
    assert quote.quote_id == "exchange-q-1"
    assert session.post_attempts == 1
    assert session.get_attempts == 1


@pytest.mark.asyncio
async def test_5xx_cancel_drives_lifecycle_ambiguous_verification() -> None:
    session = SequencedPostGetSession(
        post_responses=[
            (200, '{"code":0,"data":{"quoteId":"exchange-q-1"}}'),
            (503, "upstream unavailable"),
        ],
        get_responses=[
            (
                200,
                '{"code":0,"data":['
                '{"requestId":"rfq-1","quoteId":"exchange-q-1","state":"FILLED"}'
                "]}",
            )
        ],
    )
    client = CoincallRestClient("key", "secret")
    client._session = session  # type: ignore[assignment]
    lifecycle = QuoteLifecycle(client, dry_run=False)
    await lifecycle.reconcile(_quote_intent())

    await lifecycle.cancel_for_rfq("rfq-1")

    quote = lifecycle.get_for_rfq("rfq-1")
    assert quote is not None
    assert quote.stage is QuoteStage.FILLED
    assert session.post_attempts == 2
    assert session.get_attempts == 1


@pytest.mark.asyncio
async def test_application_error_response_is_not_retried() -> None:
    session = ApiErrorGetSession()
    client = CoincallRestClient("key", "secret")
    client._session = session  # type: ignore[assignment]

    with pytest.raises(CoincallApiError) as exc_info:
        await client._request("GET", "/open/option/blocktrade/rfqList/v1")

    assert session.get_attempts == 1
    assert exc_info.value.status == 200
    assert exc_info.value.code == 400001


@pytest.mark.asyncio
async def test_non_idempotent_execute_non_json_200_body_is_ambiguous() -> None:
    session = StaticPostSession("not json")
    client = CoincallRestClient("key", "secret")
    client._session = session  # type: ignore[assignment]

    with pytest.raises(CoincallAmbiguousError):
        await client.execute_quote("rfq-1", "quote-1")

    assert session.post_attempts == 1


@pytest.mark.asyncio
async def test_non_idempotent_execute_json_array_200_body_is_ambiguous() -> None:
    session = StaticPostSession('["not", "an", "object"]')
    client = CoincallRestClient("key", "secret")
    client._session = session  # type: ignore[assignment]

    with pytest.raises(CoincallAmbiguousError):
        await client.execute_quote("rfq-1", "quote-1")

    assert session.post_attempts == 1


@pytest.mark.asyncio
async def test_idempotent_get_non_json_200_body_retries_then_connectivity_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = StaticGetSession("not json")
    client = CoincallRestClient("key", "secret")
    client._session = session  # type: ignore[assignment]

    async def fake_sleep(delay: float) -> None:
        pass

    monkeypatch.setattr("coincall_rfq_maker.core.adapters.rest.asyncio.sleep", fake_sleep)

    with pytest.raises(CoincallConnectivityError, match="failed after 3 attempts") as exc_info:
        await client._request("GET", "/open/option/blocktrade/rfqList/v1")

    assert type(exc_info.value) is CoincallConnectivityError
    assert classify_api_failure(exc_info.value) is ApiFailureKind.TRANSIENT
    assert session.get_attempts == _MAX_ATTEMPTS


@pytest.mark.asyncio
async def test_get_quote_list_rejects_time_window_over_three_days() -> None:
    client = CoincallRestClient("key", "secret")

    with pytest.raises(CoincallRequestError, match="cannot exceed 3 days"):
        await client.get_quote_list(start_time=0, end_time=3 * 24 * 60 * 60 * 1000 + 1)


@pytest.mark.asyncio
async def test_get_rfq_list_returns_valid_payloads_and_skips_malformed_items(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = CoincallRestClient("key", "secret")

    async def fake_request(*args: Any, **kwargs: Any) -> dict[str, Any]:
        return {
            "code": 0,
            "data": {
                "rfqList": [
                    {"requestId": "rfq-1", "state": "ACTIVE", "legs": []},
                    {"requestId": "rfq-2", "state": "SOMETHING_NEW", "legs": []},
                    {"state": "ACTIVE", "legs": []},
                    "not-an-object",
                ]
            },
        }

    monkeypatch.setattr(client, "_request", fake_request)

    with caplog.at_level(logging.WARNING):
        rfqs = await client.get_rfq_list(state="OPEN")

    assert [rfq.request_id for rfq in rfqs.payloads] == ["rfq-1"]
    assert "Malformed RFQ REST item" in caplog.text
    assert "unknown state" in caplog.text


@pytest.mark.asyncio
async def test_get_quote_list_returns_valid_payloads_and_skips_malformed_items(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = CoincallRestClient("key", "secret")

    async def fake_request(*args: Any, **kwargs: Any) -> dict[str, Any]:
        return {
            "code": 0,
            "data": [
                {"quoteId": "q-1", "requestId": "rfq-1", "state": "OPEN"},
                {"requestId": "rfq-2", "state": "OPEN"},
                object(),
            ],
        }

    monkeypatch.setattr(client, "_request", fake_request)

    with caplog.at_level(logging.WARNING):
        quotes = await client.get_quote_list(state="OPEN")

    assert [quote.quote_id for quote in quotes.payloads] == ["q-1"]
    assert "Malformed quote REST item" in caplog.text


@pytest.mark.asyncio
async def test_create_quote_missing_quote_id_is_ambiguous(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = CoincallRestClient("key", "secret")

    async def fake_request(*args: Any, **kwargs: Any) -> dict[str, Any]:
        return {"code": 0, "data": {}}

    monkeypatch.setattr(client, "_request", fake_request)

    with pytest.raises(CoincallAmbiguousError, match="missing quoteId"):
        await client.create_quote("rfq-1", [])


def test_wire_id_salvages_integer_ids_but_not_bool_or_empty() -> None:
    assert _wire_id(123) == "123"
    assert _wire_id(True) is None
    assert _wire_id("") is None
    assert _wire_id("abc") == "abc"


def test_quote_list_salvages_integer_ids_from_malformed_items() -> None:
    snapshot = _parse_quote_list(
        {
            "code": 0,
            "data": [
                {
                    "requestId": 2075207481654804480,
                    "quoteId": 2075207494989787138,
                    "state": "OPEN",
                    "filledPrice": "not-a-number",
                }
            ],
        }
    )

    assert snapshot.payloads == ()
    assert snapshot.malformed_id_pairs == frozenset(
        {("2075207481654804480", "2075207494989787138")}
    )


@pytest.mark.parametrize(
    "response",
    [
        {"code": 0, "data": "not-an-object"},
        {"code": 0, "data": {"rfqList": "not-a-list"}},
    ],
)
def test_rfq_list_rejects_truthy_malformed_top_level_structure(
    response: dict[str, Any],
) -> None:
    with pytest.raises(CoincallMalformedResponseError):
        _parse_rfq_list(response)


def test_quote_list_rejects_truthy_malformed_top_level_data() -> None:
    with pytest.raises(CoincallMalformedResponseError):
        _parse_quote_list({"code": 0, "data": "not-a-list"})
