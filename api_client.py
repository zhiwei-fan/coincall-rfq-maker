# Package: coincall
import aiohttp
import asyncio
import hmac
import hashlib
import json
import time
from typing import Any, Dict
from urllib.parse import quote
from dotenv import load_dotenv

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

@dataclass(frozen=True)
class CoincallCredential:
    api_key: str
    secret_key: str


class CcAPIException(Exception):
    """Raised when an API response has a non-200 status."""
    def __init__(self, status: int, code: int | None, message: str):
        super().__init__(f"API Request Error(status={status}, code={code}): {message}")
        self.status = status
        self.code = code
        self.message = message

class CcRequestException(Exception):
    """Raised for client-side request errors."""
    pass

class CcParamsException(Exception):
    """Raised when parameters validation fails."""
    pass


def get_timestamp() -> int:
    """Return current UNIX timestamp in milliseconds."""
    return int(time.time() * 1000)

def pre_hash(
    timestamp: int,
    method: str,
    path: str,
    api_key: str,
    diff: int,
    body: Dict[str, Any] | None = None,
) -> str:
    """
    Construct the prehash string for signing:
      METHOD + path + '?' + sorted_body + '&' + auth_params
    """
    sorted_pairs = []
    if body:
        for k in sorted(body):
            v = body[k]
            if v is not None:
                sorted_pairs.append(f"{k}={v}")
    body_str = "&".join(sorted_pairs)
    connector = f"?{body_str}&" if body_str else "?"
    base = (
        f"{method.upper()}{path}{connector}"
        f"uuid={api_key}&ts={timestamp}&x-req-ts-diff={diff}"
    )
    return base

def sign_message(message: str, secret_key: str) -> str:
    """Compute HMAC-SHA256 and return uppercase hex signature."""
    digest = hmac.new(
        secret_key.encode(), message.encode(), hashlib.sha256
    ).hexdigest().upper()
    return digest

def get_header(
    api_key: str,
    signature: str,
    timestamp: int,
    diff: int,
) -> Dict[str, str]:
    return {
        "Content-Type": "application/json",
        "X-CC-APIKEY": api_key,
        "sign": signature,
        "ts": str(timestamp),
        "X-REQ-TS-DIFF": str(diff),
    }

def parse_params_to_string(params: Dict[str, Any]) -> str:
    """URL-encode GET parameters into a query string."""
    if not params:
        return ""
    parts = []
    for k, v in params.items():
        if v is not None:
            parts.append(f"{k}={quote(str(v))}")
    return "?" + "&".join(parts)

class AsyncCoincallClient:
    """Asynchronous HTTP client for Coincall REST API."""
    def __init__(
        self,
        credential: CoincallCredential,
        diff: int = 5000,
        use_server_time: bool = False,
        base_api: str = "https://betaapi.coincall.com",
    ):
        self._credential = credential
        self._diff = diff
        self._use_server_time = use_server_time
        self._base_url = base_api.rstrip("/")
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> "AsyncCoincallClient":
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _get_server_time(self) -> int:
        url = f"{self._base_url}/open/public/time/v1"
        async with self._session.get(url) as resp:
            data = await resp.json()
            if resp.status == 200:
                return data["data"]["serverTime"]
            raise CcAPIException(resp.status, data.get("code"), data.get("msg", resp.text))

    async def _request(
        self, method: str, path: str, params: dict | None = None
    ) -> dict:
        if not self._session:
            raise CcRequestException("Session not initialized. Use 'async with'.")
        ts = (
            await self._get_server_time() if self._use_server_time else get_timestamp()
        )
        message = pre_hash(
            ts, method, path, self._credential.api_key, self._diff, params
        )
        signature = sign_message(message, self._credential.secret_key)
        headers = get_header(
            self._credential.api_key, signature, ts, self._diff
        )
        url = self._base_url + path
        if method.upper() == "GET":
            url += parse_params_to_string(params or {})
            async with self._session.get(url, headers=headers) as resp:
                text = await resp.text()
                if resp.status != 200:
                    raise CcAPIException(resp.status, None, text)
                return json.loads(text)
        elif method.upper() == "POST":
            async with self._session.post(
                url, headers=headers, json=params or {}
            ) as resp:
                text = await resp.text()
                if resp.status != 200:
                    raise CcAPIException(resp.status, None, text)
                return json.loads(text)
        else:
            raise CcRequestException(f"Unsupported method '{method}'")


class RfqAPI(AsyncCoincallClient):
    """Endpoints for RFQ (block trade) operations."""
    async def get_board_list(
        self, current_page: int = 1, page_size: int = 10
    ) -> dict:
        """GET /open/option/blockTrade/seek/list/v1"""
        path = "/open/option/blockTrade/seek/list/v1"
        params = {"currentPage": current_page, "pageSize": page_size}
        return await self._request("GET", path, params)

    async def create_quote(
        self, request_id: int, order_details: list[dict]
    ) -> dict:
        """POST /open/option/blockTrade/quote/create/v1"""
        path = "/open/option/blockTrade/quote/create/v1"
        payload = {
            "requestId": request_id,
            "orderOpenApiDetailReqs": json.dumps(order_details, separators=(",", ":")),
        }
        return await self._request("POST", path, payload)

    async def cancel_quote(self, quote_id: str) -> dict:
        """GET /open/option/blockTrade/quote/cancel/v1/{quote_id}"""
        path = f"/open/option/blockTrade/quote/cancel/v1/{quote_id}"
        return await self._request("GET", path, None)



async def main():
    cred = CoincallCredential(api_key="oVboOYYXWXKMAQDA94ycWbwd4eRL7AWUOpYw/Zm9URs=", secret_key="y5CnMMZm92cUqFBtXvKl+ZdnNdZ/f70Y7rxyv8drPdw=")
    async with RfqAPI(credential=cred) as api:
        board = await api.get_board_list(current_page=1, page_size=5)
        print("Board list:", json.dumps(board, indent=2))

        # Example: create a quote
        order_details = [
            {"price": "4000", "symbol": "BTCUSD-2MAY25-96000-C", "type": "1"}
        ]
        quote = await api.create_quote(
            request_id=1915804766831775744, order_details=order_details
        )
        print("Create quote response:", quote)

        # # Example: cancel the quote
        # cancel_resp = await api.cancel_quote(str(quote.get("data", quote)))
        # print("Cancel quote response:", cancel_resp)


if __name__ == "__main__":
    import platform
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())