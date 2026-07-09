"""Coincall REST client.

Endpoints, params, and payload shapes are ported EXACTLY from the old
`api_client.py` (`RfqAPI` / `FuturesAPI`). Adds `asyncio.timeout` around every
call and a small retry for transient network errors (never for application
errors reported via the `code` field).
"""

import asyncio
import json
import logging
from types import TracebackType
from typing import Any, Literal, Self

import aiohttp

from coincall_rfq_maker.adapters.signing import (
    encode_query_params,
    get_timestamp_ms,
    sign_rest_request,
)

logger = logging.getLogger(__name__)

_REQUEST_TIMEOUT_SECONDS = 10.0
_MAX_ATTEMPTS = 3
_RETRY_BACKOFF_SECONDS = 0.5


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


class CoincallAmbiguousError(CoincallRequestError):
    """Raised when a state-changing request may have reached the exchange."""


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
                        async with self._session.post(
                            url, headers=headers, json=params or {}
                        ) as resp:
                            return await self._parse_response(resp)
                    else:
                        raise CoincallRequestError(f"Unsupported method {method!r}")
            except CoincallApiError:
                raise
            except (TimeoutError, aiohttp.ClientError) as exc:
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
        raise CoincallRequestError(
            f"{method} {path} failed after {max_attempts} attempts: {last_error}"
        ) from last_error

    @staticmethod
    async def _parse_response(resp: aiohttp.ClientResponse) -> dict[str, Any]:
        text = await resp.text()
        if resp.status != 200:
            raise CoincallApiError(resp.status, None, text)
        data: dict[str, Any] = json.loads(text)
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
    ) -> dict[str, Any]:
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
        return await self._request("GET", "/open/option/blocktrade/rfqList/v1", params)

    # -- Quote endpoints --------------------------------------------------

    async def create_quote(self, request_id: str, legs: list[dict[str, str]]) -> dict[str, Any]:
        """POST /open/option/blocktrade/quote/create/v1"""
        payload = {"requestId": request_id, "legs": legs}
        return await self._request(
            "POST", "/open/option/blocktrade/quote/create/v1", payload, idempotent=False
        )

    async def cancel_quote(self, quote_id: str) -> dict[str, Any]:
        """POST /open/option/blocktrade/quote/cancel/v1"""
        return await self._request(
            "POST",
            "/open/option/blocktrade/quote/cancel/v1",
            {"quoteId": quote_id},
            idempotent=False,
        )

    async def cancel_all_quotes(self) -> dict[str, Any]:
        """POST /open/option/blocktrade/quote/cancel-all/v1"""
        return await self._request(
            "POST", "/open/option/blocktrade/quote/cancel-all/v1", None, idempotent=False
        )

    async def get_quote_list(
        self,
        quote_id: str | None = None,
        request_id: str | None = None,
        state: Literal["OPEN", "CLOSED"] | None = None,
        symbol: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> dict[str, Any]:
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
        return await self._request("GET", "/open/option/blocktrade/list-quote/v1", params)

    # -- Futures market data --------------------------------------------------

    async def get_symbol_info(self, symbol: str | None = None) -> dict[str, Any]:
        """GET /open/futures/market/symbol/v1"""
        params: dict[str, Any] = {}
        if symbol is not None:
            params["symbol"] = symbol
        return await self._request("GET", "/open/futures/market/symbol/v1", params)
