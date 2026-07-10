"""Coincall REST client.

Endpoints, params, and payload shapes are ported EXACTLY from the old
`api_client.py` (`RfqAPI` / `FuturesAPI`). Adds `asyncio.timeout` around every
call and a small retry for transient network errors (never for application
errors reported via persistent `code` values).
"""

import asyncio
import json
import logging
from enum import Enum, auto
from types import TracebackType
from typing import Any, Literal, Self
from urllib.parse import urlencode

import aiohttp
from pydantic import ValidationError

from coincall_rfq_maker.core.adapters.schemas import (
    CreateQuoteResult,
    ExecutedLegPayload,
    ExecuteQuoteResult,
    OptionInstrument,
    QuoteListSnapshot,
    QuotePayload,
    RfqCreateResult,
    RfqListSnapshot,
    RfqPayload,
    SymbolInfoPayload,
    rfq_status_from_wire,
)
from coincall_rfq_maker.core.adapters.signing import (
    encode_query_params,
    sign_rest_request,
)
from coincall_rfq_maker.core.clock import get_timestamp_ms

logger = logging.getLogger(__name__)

_REQUEST_TIMEOUT_SECONDS = 10.0
_MAX_ATTEMPTS = 3
_RETRY_BACKOFF_SECONDS = 0.5
_MAX_QUOTE_LIST_WINDOW_MS = 3 * 24 * 60 * 60 * 1000


class CoincallError(Exception):
    """Base class for Coincall REST client failures."""


class CoincallApiError(CoincallError):
    """Raised when the exchange responds with a non-200 status or non-zero `code`."""

    def __init__(self, status: int, code: int | None, message: str) -> None:
        super().__init__(f"API error (status={status}, code={code}): {message}")
        self.status = status
        self.code = code
        self.message = message


class CoincallRequestError(CoincallError):
    """Raised for client-side misuse (e.g. session not started)."""


class CoincallConnectivityError(CoincallRequestError):
    """Raised when idempotent transport retries are exhausted."""


class CoincallMalformedResponseError(CoincallRequestError):
    """Raised when an HTTP-200 response body is not the expected JSON object."""


class CoincallAmbiguousError(CoincallRequestError):
    """Raised when a state-changing request may have reached the exchange."""


class ApiFailureKind(Enum):
    """Failure classes used by retry and kill-switch accounting."""

    TRANSIENT = auto()
    PERSISTENT = auto()
    AMBIGUOUS = auto()


def classify_api_failure(exc: CoincallError) -> ApiFailureKind:
    """Classify Coincall REST failures for retry/accounting boundaries."""
    if isinstance(exc, CoincallAmbiguousError):
        return ApiFailureKind.AMBIGUOUS
    if isinstance(exc, (CoincallConnectivityError, CoincallMalformedResponseError)):
        return ApiFailureKind.TRANSIENT
    if isinstance(exc, CoincallApiError):
        if exc.code == 10000:
            return ApiFailureKind.TRANSIENT
        if exc.status == 429 or 500 <= exc.status <= 599:
            return ApiFailureKind.TRANSIENT
    return ApiFailureKind.PERSISTENT


class CoincallRestClient:
    """Async REST client for the RFQ/quote and futures market-data endpoints."""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        base_url: str = "https://betaapi.coincall.com",
        diff: int = 5000,
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._base_url = base_url.rstrip("/")
        self._diff = diff
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> Self:
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()

    async def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        idempotent: bool = True,
        form_encoded: bool = False,
    ) -> dict[str, Any]:
        if self._session is None:
            raise CoincallRequestError("Session not started; use 'async with CoincallRestClient'")

        max_attempts = _MAX_ATTEMPTS if idempotent else 1

        last_error: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            timestamp = get_timestamp_ms()
            headers = sign_rest_request(
                method, path, self._api_key, self._api_secret, timestamp, self._diff, params
            )
            url = self._base_url + path
            try:
                async with asyncio.timeout(_REQUEST_TIMEOUT_SECONDS):
                    if method.upper() == "GET":
                        url += encode_query_params(params or {})
                        async with self._session.get(url, headers=headers) as resp:
                            return await self._parse_response(resp)
                    elif method.upper() == "POST":
                        # Coincall blocktrade cancel/accept endpoints require form-urlencoded
                        # bodies (JSON -> code 10004 'Parameter illegal'); create endpoints take
                        # JSON. Signing is identical either way (prehash is from the params dict).
                        if form_encoded:
                            headers["Content-Type"] = "application/x-www-form-urlencoded"
                            body = urlencode(
                                {k: str(v) for k, v in (params or {}).items() if v is not None}
                            )
                            async with self._session.post(url, headers=headers, data=body) as resp:
                                return await self._parse_response(resp)
                        async with self._session.post(
                            url, headers=headers, json=params or {}
                        ) as resp:
                            return await self._parse_response(resp)
                    else:
                        raise CoincallRequestError(f"Unsupported method {method!r}")
            except CoincallApiError as exc:
                if (
                    classify_api_failure(exc) is ApiFailureKind.TRANSIENT
                    and idempotent
                    and attempt < max_attempts
                ):
                    logger.warning(
                        "Request %s %s failed (attempt %d/%d): %s",
                        method,
                        path,
                        attempt,
                        max_attempts,
                        exc,
                    )
                    await asyncio.sleep(_RETRY_BACKOFF_SECONDS * attempt)
                    continue
                if (
                    not idempotent
                    and exc.code != 10000
                    and (exc.status == 408 or 500 <= exc.status <= 599)
                ):
                    logger.warning(
                        "Request %s %s failed (attempt %d/%d): %s",
                        method,
                        path,
                        attempt,
                        max_attempts,
                        exc,
                    )
                    raise CoincallAmbiguousError(
                        f"{method} {path} outcome ambiguous after API error: {exc}"
                    ) from exc
                raise
            except (TimeoutError, aiohttp.ClientError, CoincallMalformedResponseError) as exc:
                last_error = exc
                logger.warning(
                    "Request %s %s failed (attempt %d/%d): %s",
                    method,
                    path,
                    attempt,
                    max_attempts,
                    exc,
                )
                if not idempotent:
                    raise CoincallAmbiguousError(
                        f"{method} {path} outcome ambiguous after transport error: {exc}"
                    ) from exc
                if attempt < max_attempts:
                    await asyncio.sleep(_RETRY_BACKOFF_SECONDS * attempt)

        assert last_error is not None
        raise CoincallConnectivityError(
            f"{method} {path} failed after {max_attempts} attempts: {last_error}"
        ) from last_error

    @staticmethod
    async def _parse_response(resp: aiohttp.ClientResponse) -> dict[str, Any]:
        text = await resp.text()
        if resp.status != 200:
            raise CoincallApiError(resp.status, None, text)
        try:
            data = json.loads(text)
        except (json.JSONDecodeError, ValueError) as exc:
            raise CoincallMalformedResponseError("Malformed HTTP-200 response body") from exc
        if not isinstance(data, dict):
            raise CoincallMalformedResponseError(
                f"Malformed HTTP-200 response body: expected object, got {type(data).__name__}"
            )
        if data.get("code") != 0:
            raise CoincallApiError(resp.status, data.get("code"), data.get("msg", "Unknown error"))
        return data

    # -- RFQ endpoints --------------------------------------------------

    async def get_rfq_list(
        self,
        request_id: str | None = None,
        state: Literal["OPEN", "CLOSED"] | None = None,
        role: Literal["TAKER", "MAKER"] | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> RfqListSnapshot:
        """GET /open/option/blocktrade/rfqList/v1"""
        params: dict[str, Any] = {}
        if request_id:
            params["requestId"] = request_id
        if state:
            params["state"] = state
        if role:
            params["role"] = role
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time
        response = await self._request("GET", "/open/option/blocktrade/rfqList/v1", params)
        return _parse_rfq_list(response)

    async def create_rfq(self, legs: list[dict[str, str]]) -> RfqCreateResult:
        """POST /open/option/blocktrade/request/create/v1"""
        response = await self._request(
            "POST",
            "/open/option/blocktrade/request/create/v1",
            {"legs": legs},
            idempotent=False,
        )
        try:
            return RfqCreateResult.model_validate(response.get("data") or {})
        except ValidationError as exc:
            raise CoincallRequestError("Malformed create-RFQ response") from exc

    async def cancel_rfq(self, request_id: str) -> dict[str, Any]:
        """POST /open/option/blocktrade/request/cancel/v1"""
        return await self._request(
            "POST",
            "/open/option/blocktrade/request/cancel/v1",
            {"requestId": request_id},
            idempotent=False,
            form_encoded=True,
        )

    async def execute_quote(self, request_id: str, quote_id: str) -> ExecuteQuoteResult:
        """Accept a quote and execute the resulting block trade.

        A returned ``_request`` response means HTTP 200 + ``code==0``: the accept
        SUCCEEDED and the block trade FILLED. That fill must NEVER be discarded,
        so a payload that fails validation does NOT raise (raising would lose a
        real fill); instead we log and return a best-effort result carrying
        whatever ``blockTradeId`` can be salvaged (else the ``"UNKNOWN"``
        sentinel), so the caller always has a fill to audit.
        """
        response = await self._request(
            "POST",
            "/open/option/blocktrade/request/accept/v1",
            {"requestId": request_id, "quoteId": quote_id},
            idempotent=False,
            form_encoded=True,
        )
        data = response.get("data")
        raw = data if isinstance(data, dict) else {}
        try:
            return ExecuteQuoteResult.model_validate(raw)
        except ValidationError as exc:
            block_trade_id = _wire_id(raw.get("blockTradeId")) or "UNKNOWN"
            legs = _salvage_executed_legs(raw.get("legs"))
            logger.error(
                "Accept for quote %s (RFQ %s) SUCCEEDED (code==0) but its response failed "
                "validation; recording a best-effort fill (blockTradeId=%s) rather than "
                "discarding it: %s",
                quote_id,
                request_id,
                block_trade_id,
                exc,
            )
            return ExecuteQuoteResult.model_construct(
                block_trade_id=block_trade_id,
                request_id=request_id,
                quote_id=quote_id,
                legs=legs,
            )

    async def get_quotes_received(self, request_id: str | None = None) -> tuple[QuotePayload, ...]:
        """GET /open/option/blocktrade/request/getQuotesReceived/v1"""
        params = {"requestId": request_id} if request_id is not None else None
        response = await self._request(
            "GET", "/open/option/blocktrade/request/getQuotesReceived/v1", params
        )
        return _parse_quotes_received(response)

    # -- Quote endpoints --------------------------------------------------

    async def create_quote(self, request_id: str, legs: list[dict[str, str]]) -> CreateQuoteResult:
        """POST /open/option/blocktrade/quote/create/v1"""
        payload = {"requestId": request_id, "legs": legs}
        response = await self._request(
            "POST", "/open/option/blocktrade/quote/create/v1", payload, idempotent=False
        )
        data = response.get("data") or {}
        try:
            result = CreateQuoteResult.model_validate(data)
        except ValidationError as exc:
            logger.error(
                "Create-quote response missing quoteId for RFQ %s: %s",
                request_id,
                response,
            )
            raise CoincallAmbiguousError(
                f"Create-quote response missing quoteId for RFQ {request_id}"
            ) from exc
        if not result.quote_id:
            logger.error(
                "Create-quote response missing quoteId for RFQ %s: %s",
                request_id,
                response,
            )
            raise CoincallAmbiguousError(
                f"Create-quote response missing quoteId for RFQ {request_id}"
            )
        return result

    async def cancel_quote(self, quote_id: str) -> dict[str, Any]:
        """POST /open/option/blocktrade/quote/cancel/v1"""
        return await self._request(
            "POST",
            "/open/option/blocktrade/quote/cancel/v1",
            {"quoteId": quote_id},
            idempotent=False,
            form_encoded=True,
        )

    async def cancel_all_quotes(self) -> dict[str, Any]:
        """POST /open/option/blocktrade/quote/cancel-all/v1"""
        return await self._request(
            "POST",
            "/open/option/blocktrade/quote/cancel-all/v1",
            None,
            idempotent=False,
            form_encoded=True,
        )

    async def get_quote_list(
        self,
        quote_id: str | None = None,
        request_id: str | None = None,
        state: Literal["OPEN", "CLOSED"] | None = None,
        symbol: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> QuoteListSnapshot:
        """GET /open/option/blocktrade/list-quote/v1"""
        params: dict[str, Any] = {}
        if quote_id:
            params["quoteId"] = quote_id
        if request_id:
            params["requestId"] = request_id
        if state:
            params["state"] = state
        if symbol:
            params["symbol"] = symbol
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time
        if (
            start_time is not None
            and end_time is not None
            and end_time - start_time > _MAX_QUOTE_LIST_WINDOW_MS
        ):
            raise CoincallRequestError("Quote list time range cannot exceed 3 days")
        response = await self._request("GET", "/open/option/blocktrade/list-quote/v1", params)
        return _parse_quote_list(response)

    # -- Futures market data --------------------------------------------------

    async def get_symbol_info(self, symbol: str | None = None) -> SymbolInfoPayload:
        """GET /open/futures/market/symbol/v1"""
        params: dict[str, Any] = {}
        if symbol is not None:
            params["symbol"] = symbol
        response = await self._request("GET", "/open/futures/market/symbol/v1", params)
        try:
            return SymbolInfoPayload.model_validate(response.get("data") or {})
        except ValidationError as exc:
            raise CoincallRequestError(f"Malformed symbol info response for {symbol}") from exc

    async def get_option_instruments(self, base_currency: str) -> tuple[OptionInstrument, ...]:
        """GET /open/option/getInstruments/{base_currency}"""
        response = await self._request("GET", f"/open/option/getInstruments/{base_currency}", None)
        return _parse_option_instruments(response)


def _parse_rfq_list(response: dict[str, Any]) -> RfqListSnapshot:
    data = response.get("data") or {}
    if not isinstance(data, dict):
        raise CoincallMalformedResponseError("Malformed RFQ REST response data: expected object")
    raw_items = data.get("rfqList") or []
    if not isinstance(raw_items, list):
        raise CoincallMalformedResponseError("Malformed RFQ REST response rfqList: expected list")
    payloads, malformed_request_ids = _validated_rfq_items(raw_items)
    valid_payloads: list[RfqPayload] = []
    for payload in payloads:
        if rfq_status_from_wire(payload.state) is None:
            logger.warning(
                "Malformed RFQ REST item: unknown state %r for %s",
                payload.state,
                payload.request_id,
            )
            if payload.request_id:
                malformed_request_ids.add(payload.request_id)
            continue
        valid_payloads.append(payload)
    return RfqListSnapshot(tuple(valid_payloads), frozenset(malformed_request_ids))


def _parse_quote_list(response: dict[str, Any]) -> QuoteListSnapshot:
    raw_items = response.get("data") or []
    if not isinstance(raw_items, list):
        raise CoincallMalformedResponseError("Malformed quote REST response data: expected list")
    payloads, malformed_id_pairs = _validated_quote_items(raw_items)
    return QuoteListSnapshot(tuple(payloads), frozenset(malformed_id_pairs))


def _parse_quotes_received(response: dict[str, Any]) -> tuple[QuotePayload, ...]:
    raw_items = response.get("data") or []
    if not isinstance(raw_items, list):
        logger.warning("Malformed quotes-received REST response data: expected list")
        return ()
    payloads, _malformed_id_pairs = _validated_quote_items(raw_items)
    return tuple(payloads)


def _parse_option_instruments(response: dict[str, Any]) -> tuple[OptionInstrument, ...]:
    raw_items = response.get("data") or []
    if not isinstance(raw_items, list):
        logger.warning("Malformed option instruments REST response data: expected list")
        return ()
    items: list[OptionInstrument] = []
    for item in raw_items:
        if not isinstance(item, dict):
            logger.warning("Malformed option instrument REST item: expected object")
            continue
        try:
            items.append(OptionInstrument.model_validate(item))
        except ValidationError as exc:
            logger.warning("Malformed option instrument REST item: %s", exc)
    return tuple(items)


def _validated_rfq_items(raw_items: list[Any]) -> tuple[list[RfqPayload], set[str]]:
    items: list[RfqPayload] = []
    malformed_request_ids: set[str] = set()
    for item in raw_items:
        if not isinstance(item, dict):
            logger.warning("Malformed RFQ REST item: expected object")
            continue
        try:
            items.append(RfqPayload.model_validate(item))
        except ValidationError as exc:
            request_id = _wire_id(item.get("requestId"))
            if request_id is not None:
                malformed_request_ids.add(request_id)
            logger.warning("Malformed RFQ REST item: %s", exc)
    return items, malformed_request_ids


def _validated_quote_items(raw_items: list[Any]) -> tuple[list[QuotePayload], set[tuple[str, str]]]:
    items: list[QuotePayload] = []
    malformed_id_pairs: set[tuple[str, str]] = set()
    for item in raw_items:
        if not isinstance(item, dict):
            logger.warning("Malformed quote REST item: expected object")
            continue
        try:
            items.append(QuotePayload.model_validate(item))
        except ValidationError as exc:
            request_id = _wire_id(item.get("requestId"))
            quote_id = _wire_id(item.get("quoteId"))
            if request_id is not None and quote_id is not None:
                malformed_id_pairs.add((request_id, quote_id))
            logger.warning("Malformed quote REST item: %s", exc)
    return items, malformed_id_pairs


def _salvage_executed_legs(raw_legs: object) -> list[ExecutedLegPayload]:
    if not isinstance(raw_legs, list):
        return []
    legs: list[ExecutedLegPayload] = []
    for raw_leg in raw_legs:
        if not isinstance(raw_leg, dict):
            logger.warning("Malformed executed leg in successful accept response: expected object")
            continue
        try:
            legs.append(ExecutedLegPayload.model_validate(raw_leg))
        except ValidationError as exc:
            logger.warning("Malformed executed leg in successful accept response: %s", exc)
    return legs


def _wire_id(value: object) -> str | None:
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, str) and value:
        return value
    return None
