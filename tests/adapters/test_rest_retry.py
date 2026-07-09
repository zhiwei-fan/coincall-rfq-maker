import logging
from types import TracebackType
from typing import Any

import pytest

from coincall_rfq_maker.adapters.rest import (
    CoincallAmbiguousError,
    CoincallRequestError,
    CoincallRestClient,
)


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


@pytest.mark.asyncio
async def test_state_changing_post_timeout_is_not_retried_and_is_ambiguous() -> None:
    session = TimeoutPostSession()
    client = CoincallRestClient("key", "secret")
    client._session = session  # type: ignore[assignment]

    with pytest.raises(CoincallAmbiguousError):
        await client.create_quote("rfq-1", [{"instrumentName": "BTCUSD-21AUG25-120000-C"}])

    assert session.post_attempts == 1


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

    assert [rfq.request_id for rfq in rfqs] == ["rfq-1"]
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

    assert [quote.quote_id for quote in quotes] == ["q-1"]
    assert "Malformed quote REST item" in caplog.text


@pytest.mark.asyncio
async def test_create_quote_missing_quote_id_raises_request_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = CoincallRestClient("key", "secret")

    async def fake_request(*args: Any, **kwargs: Any) -> dict[str, Any]:
        return {"code": 0, "data": {}}

    monkeypatch.setattr(client, "_request", fake_request)

    with pytest.raises(CoincallRequestError, match="missing quoteId"):
        await client.create_quote("rfq-1", [])
