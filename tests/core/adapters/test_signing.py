"""Golden-vector tests for exchange signing.

Vectors were captured by running the OLD `api_client.py` / `websocket_client.py`
signing functions directly (fixed api_key="test-key", api_secret="test-secret",
timestamp=1750000000000, diff=5000) before those files were deleted. If these
ever need to change, re-derive against a live account — do not hand-edit them.
"""

from coincall_rfq_maker.core.adapters.schemas import CreateQuoteResult
from coincall_rfq_maker.core.adapters.signing import (
    build_rest_headers,
    build_rest_prehash,
    build_ws_signed_url,
    sign_message,
    sign_rest_request,
)
from coincall_rfq_maker.quoting.lifecycle import QuoteLifecycle
from coincall_rfq_maker.quoting.strategy import QuoteIntent, QuoteLegIntent
from coincall_rfq_maker.risk.gate import ApprovedQuotePlan

API_KEY = "test-key"
API_SECRET = "test-secret"
TS = 1750000000000
DIFF = 5000


class NoopApiReporter:
    def record_api_failure(self) -> None:
        pass

    def record_api_success(self) -> None:
        pass


def test_get_prehash_with_params() -> None:
    message = build_rest_prehash(
        TS,
        "GET",
        "/open/option/blocktrade/rfqList/v1",
        API_KEY,
        DIFF,
        {"state": "OPEN", "role": "MAKER"},
    )
    assert message == (
        "GET/open/option/blocktrade/rfqList/v1?role=MAKER&state=OPEN"
        "&uuid=test-key&ts=1750000000000&x-req-ts-diff=5000"
    )
    assert (
        sign_message(message, API_SECRET)
        == "1677ADD7A27535F275539A17891025E3F0E8BEED9E2F780375E04A770E29656C"
    )


def test_get_prehash_without_params() -> None:
    message = build_rest_prehash(
        TS, "GET", "/open/option/blocktrade/list-quote/v1", API_KEY, DIFF, None
    )
    assert message == (
        "GET/open/option/blocktrade/list-quote/v1?uuid=test-key&ts=1750000000000&x-req-ts-diff=5000"
    )
    assert (
        sign_message(message, API_SECRET)
        == "EDB261BC0E9367939F77E4434202F03AB9C72C56D540C90BEFEF08367D7699FC"
    )


def test_post_prehash_with_nested_list_body() -> None:
    body = {
        "requestId": "rfq-123",
        "legs": [{"instrumentName": "BTCUSD-29AUG25-125000-C", "price": "22"}],
    }
    message = build_rest_prehash(
        TS, "POST", "/open/option/blocktrade/quote/create/v1", API_KEY, DIFF, body
    )
    assert message == (
        "POST/open/option/blocktrade/quote/create/v1?"
        'legs=[{"instrumentName":"BTCUSD-29AUG25-125000-C","price":"22"}]'
        "&requestId=rfq-123&uuid=test-key&ts=1750000000000&x-req-ts-diff=5000"
    )
    assert (
        sign_message(message, API_SECRET)
        == "2950EEF8E210F5FD45E0E72323589739001DABDFB2C8EC6901583C525DAF15FC"
    )


async def test_create_quote_callsite_payload_prehash_golden() -> None:
    class CapturingRestClient:
        def __init__(self) -> None:
            self.payload: dict[str, object] | None = None

        async def create_quote(
            self, request_id: str, legs: list[dict[str, str]]
        ) -> CreateQuoteResult:
            self.payload = {"requestId": request_id, "legs": legs}
            return CreateQuoteResult(quote_id="q-1")

    rest = CapturingRestClient()
    lifecycle = QuoteLifecycle(rest, dry_run=False, api_reporter=NoopApiReporter())  # type: ignore[arg-type]

    await lifecycle.reconcile(
        ApprovedQuotePlan(
            intent=QuoteIntent(
                request_id="rfq-1",
                legs=(
                    QuoteLegIntent(
                        instrument_name="BTCUSD-21AUG25-120000-C",
                        price=100.0,
                    ),
                ),
            ),
            decided_at_ms=0,
        )
    )

    assert rest.payload is not None
    message = build_rest_prehash(
        TS,
        "POST",
        "/open/option/blocktrade/quote/create/v1",
        API_KEY,
        DIFF,
        rest.payload,
    )
    assert message == (
        "POST/open/option/blocktrade/quote/create/v1?"
        'legs=[{"instrumentName":"BTCUSD-21AUG25-120000-C","price":"100.0"}]'
        "&requestId=rfq-1&uuid=test-key&ts=1750000000000&x-req-ts-diff=5000"
    )
    assert (
        sign_message(message, API_SECRET)
        == "9F4C2CF5225F568BBA8019D8E4712318C9C04993A8B967041DF1AA3C62C62DDA"
    )


def test_post_prehash_no_body() -> None:
    message = build_rest_prehash(
        TS, "POST", "/open/option/blocktrade/quote/cancel-all/v1", API_KEY, DIFF, None
    )
    assert message == (
        "POST/open/option/blocktrade/quote/cancel-all/v1?uuid=test-key"
        "&ts=1750000000000&x-req-ts-diff=5000"
    )
    assert (
        sign_message(message, API_SECRET)
        == "F55694FBDCCC1CE64E4521FBE4A1EEADE39D45D67006F22052130EDF99A3EDBC"
    )


def test_post_prehash_single_field_body() -> None:
    message = build_rest_prehash(
        TS, "POST", "/open/option/blocktrade/quote/cancel/v1", API_KEY, DIFF, {"quoteId": "q-999"}
    )
    assert message == (
        "POST/open/option/blocktrade/quote/cancel/v1?quoteId=q-999"
        "&uuid=test-key&ts=1750000000000&x-req-ts-diff=5000"
    )
    assert (
        sign_message(message, API_SECRET)
        == "9ED0FB5E03EEBCDFA6200D78EA54D51DB5BA58CB659E5B5C723AC25D90D98C6E"
    )


def test_sign_rest_request_end_to_end_headers() -> None:
    headers = sign_rest_request(
        "POST",
        "/open/option/blocktrade/quote/cancel/v1",
        API_KEY,
        API_SECRET,
        TS,
        DIFF,
        {"quoteId": "q-999"},
    )
    assert headers == {
        "Content-Type": "application/json",
        "X-CC-APIKEY": "test-key",
        "sign": "9ED0FB5E03EEBCDFA6200D78EA54D51DB5BA58CB659E5B5C723AC25D90D98C6E",
        "ts": "1750000000000",
        "X-REQ-TS-DIFF": "5000",
    }


def test_build_rest_headers_shape() -> None:
    headers = build_rest_headers(API_KEY, "SIGNATURE", TS, DIFF)
    assert set(headers) == {"Content-Type", "X-CC-APIKEY", "sign", "ts", "X-REQ-TS-DIFF"}
    assert headers["ts"] == "1750000000000"
    assert headers["X-REQ-TS-DIFF"] == "5000"


def test_ws_signed_url_golden_vector() -> None:
    url = build_ws_signed_url(
        "wss://betaws.seizeyouralpha.com/options", API_KEY, API_SECRET, timestamp=TS
    )
    assert url == (
        "wss://betaws.seizeyouralpha.com/options?code=10&uuid=test-key&ts=1750000000000"
        "&sign=49FA01D7DFD3BCEA5DF28F8079848D90683E8C999539F122EB1DC4DAE47AD734&apiKey=test-key"
    )
