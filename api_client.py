# Package: coincall
import aiohttp
import asyncio
import hmac
import hashlib
import json
import os
import time
from typing import Any, Optional, Literal
from urllib.parse import quote
from dotenv import load_dotenv
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@dataclass(frozen=True)
class CoincallCredential:
    api_key: str
    secret_key: str


@dataclass
class RFQLeg:
    """Represents a leg in an RFQ or Quote"""
    instrumentName: str
    side: Literal["BUY", "SELL"]
    quantity: str
    price: Optional[str] = None  # Price is optional for RFQ, required for Quote


@dataclass
class QuoteRequest:
    """Request structure for creating a quote"""
    requestId: str
    legs: list[dict[str, str]]  # List of {instrumentName, price}


class CcAPIException(Exception):
    """Raised when an API response has a non-200 status."""
    def __init__(self, status: int, code: Optional[int], message: str):
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
    body: Optional[dict[str, Any]] = None,
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
                # For lists and dicts, convert to JSON string without spaces
                if isinstance(v, (list, dict)):
                    v_str = json.dumps(v, separators=(',', ':'), ensure_ascii=False)
                else:
                    v_str = str(v)
                sorted_pairs.append(f"{k}={v_str}")
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
) -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "X-CC-APIKEY": api_key,
        "sign": signature,
        "ts": str(timestamp),
        "X-REQ-TS-DIFF": str(diff),
    }


def parse_params_to_string(params: dict[str, Any]) -> str:
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
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> "AsyncCoincallClient":
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _get_server_time(self) -> int:
        """Get server timestamp for synchronization"""
        url = f"{self._base_url}/open/public/time/v1"
        if not self._session:
            raise CcRequestException("Session not initialized")
        async with self._session.get(url) as resp:
            data = await resp.json()
            if resp.status == 200:
                return data["data"]["serverTime"]
            raise CcAPIException(resp.status, data.get("code"), data.get("msg", resp.text))

    async def _request(
        self, method: str, path: str, params: Optional[dict] = None
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
                data = json.loads(text)
                if data.get("code") != 0:
                    raise CcAPIException(resp.status, data.get("code"), data.get("msg", "Unknown error"))
                return data
        elif method.upper() == "POST":
            async with self._session.post(
                url, headers=headers, json=params or {}
            ) as resp:
                text = await resp.text()
                if resp.status != 200:
                    raise CcAPIException(resp.status, None, text)
                data = json.loads(text)
                if data.get("code") != 0:
                    raise CcAPIException(resp.status, data.get("code"), data.get("msg", "Unknown error"))
                return data
        else:
            raise CcRequestException(f"Unsupported method '{method}'")


class RfqAPI(AsyncCoincallClient):
    """RFQ Maker API endpoints for block trade operations."""
    
    # RFQ Endpoints
    async def get_rfq_list(
        self,
        requestId: Optional[str] = None,
        state: Optional[Literal["OPEN", "CLOSED"]] = None,
        role: Optional[Literal["TAKER", "MAKER"]] = None,
        startTime: Optional[int] = None,
        endTime: Optional[int] = None
    ) -> dict:
        """
        Get list of RFQs
        GET /open/option/blocktrade/rfqList/v1
        
        Returns RFQs with states: ACTIVE | CANCELLED | EXPIRED | FILLED | TRADED_AWAY
        Default time range: 3 days from current time
        """
        path = "/open/option/blocktrade/rfqList/v1"
        params = {}
        
        if requestId:
            params["requestId"] = requestId
        if state:
            params["state"] = state
        if role:
            params["role"] = role
        if startTime:
            params["startTime"] = startTime
        if endTime:
            params["endTime"] = endTime
            
        return await self._request("GET", path, params)
    
    # Quote Endpoints
    async def create_quote(
        self,
        requestId: str,
        legs: list[dict[str, str]]
    ) -> dict:
        """
        Create a quote for an RFQ
        POST /open/option/blocktrade/quote/create/v1
        
        Args:
            requestId: The RFQ ID to quote for
            legs: List of dicts with instrumentName and price
                  e.g., [{"instrumentName": "BTCUSD-29AUG25-125000-C", "price": "22"}]
        
        Returns:
            Quote details including quoteId
        """
        path = "/open/option/blocktrade/quote/create/v1"
        payload = {
            "requestId": requestId,
            "legs": legs
        }
        return await self._request("POST", path, payload)
    
    async def cancel_quote(self, quoteId: str) -> dict:
        """
        Cancel a specific quote by ID
        POST /open/option/blocktrade/quote/cancel/v1
        
        Args:
            quoteId: The quote ID to cancel
        """
        path = "/open/option/blocktrade/quote/cancel/v1"
        params = {"quoteId": quoteId}
        return await self._request("POST", path, params)
    
    async def cancel_all_quotes(self) -> dict:
        """
        Cancel all active quotes
        POST /open/option/blocktrade/quote/cancel-all/v1
        """
        path = "/open/option/blocktrade/quote/cancel-all/v1"
        return await self._request("POST", path, None)
    
    async def get_quote_list(
        self,
        quoteId: Optional[str] = None,
        requestId: Optional[str] = None,
        state: Optional[Literal["OPEN", "CLOSED"]] = None,
        symbol: Optional[str] = None,
        startTime: Optional[int] = None,
        endTime: Optional[int] = None
    ) -> dict:
        """
        Get list of quotes
        GET /open/option/blocktrade/list-quote/v1
        
        Returns quotes with states: OPEN | CANCELLED | FILLED
        Default: Returns up to 3 days of quotes from current timestamp
        """
        path = "/open/option/blocktrade/list-quote/v1"
        params = {}
        
        if quoteId:
            params["quoteId"] = quoteId
        if requestId:
            params["requestId"] = requestId
        if state:
            params["state"] = state
        if symbol:
            params["symbol"] = symbol
        if startTime:
            params["startTime"] = startTime
        if endTime:
            params["endTime"] = endTime
            
        # Validate time range (max 3 days)
        if startTime and endTime:
            time_diff = endTime - startTime
            max_diff = 3 * 24 * 60 * 60 * 1000  # 3 days in milliseconds
            if time_diff > max_diff:
                raise CcParamsException("Time range cannot exceed 3 days")
                
        return await self._request("GET", path, params)
    
    # Private Trade Endpoints
    async def get_private_trades(
        self,
        blocktradeId: Optional[str] = None,
        fromId: Optional[str] = None,
        direction: Optional[Literal["NEXT", "PREV"]] = None,
        pageSize: int = 20,
        currency: Optional[str] = None
    ) -> dict:
        """
        Get list of private trades
        GET /open/option/blocktrade/orderList/v1
        
        Args:
            blocktradeId: Filter by specific trade ID
            fromId: Pagination cursor
            direction: Pagination direction (NEXT or PREV)
            pageSize: Number of results (max 500, default 20)
            currency: Filter by currency
        """
        path = "/open/option/blocktrade/orderList/v1"
        
        if pageSize > 500:
            raise CcParamsException("pageSize cannot exceed 500")
            
        params = {"pageSize": pageSize}
        
        if blocktradeId:
            params["blocktradeId"] = blocktradeId
        if fromId:
            params["fromId"] = fromId
        if direction:
            params["direction"] = direction
        if currency:
            params["currency"] = currency
            
        return await self._request("GET", path, params)
    
    # Helper methods for common workflows
    async def get_active_rfqs(self) -> dict:
        """Get all active RFQs where user is a maker"""
        return await self.get_rfq_list(state="OPEN")
    
    async def get_my_open_quotes(self) -> dict:
        """Get all open quotes created by the user"""
        return await self.get_quote_list(state="OPEN")
    
    async def quote_rfq_with_spreads(
        self,
        requestId: str,
        rfq_legs: list[dict],
        spread_bps: int = 100
    ) -> dict:
        """
        Helper to quote an RFQ with a spread from mark prices
        
        Args:
            requestId: RFQ ID
            rfq_legs: List of RFQ legs with instrumentName, side, quantity
            spread_bps: Spread in basis points (100 = 1%)
        """
        quote_legs = []
        spread_mult = 1 + (spread_bps / 10000)
        
        for leg in rfq_legs:
            # In production, fetch actual mark prices
            # This is a simplified example
            base_price = 1000  # Would fetch from market data
            
            if leg["side"] == "BUY":
                # Maker sells at higher price
                price = str(base_price * spread_mult)
            else:
                # Maker buys at lower price
                price = str(base_price / spread_mult)
                
            quote_legs.append({
                "instrumentName": leg["instrumentName"],
                "price": price
            })
            
        return await self.create_quote(requestId, quote_legs)


async def example_usage():
    """Example usage of the RFQ API"""
    load_dotenv()
    API_KEY = os.environ.get("API_KEY")
    API_SECRET = os.environ.get("API_SECRET")
    
    if not API_KEY or not API_SECRET:
        raise ValueError("API_KEY and API_SECRET must be set in environment")
    
    cred = CoincallCredential(api_key=API_KEY, secret_key=API_SECRET)
    
    async with RfqAPI(credential=cred) as api:
        try:
            # 1. Get active RFQs as a maker
            print("\n=== Active RFQs ===")
            rfqs = await api.get_active_rfqs()
            print(f"Found {len(rfqs.get('data', {}).get('rfqList', []))} active RFQs")
            
            # 2. Create a quote for an RFQ (if any exist)
            if rfqs.get('data', {}).get('rfqList'):
                first_rfq = rfqs['data']['rfqList'][0]
                request_id = first_rfq['requestId']
                legs = first_rfq['legs']
                
                print(f"\n=== Creating quote for RFQ {request_id} ===")
                
                # Build quote legs with prices
                quote_legs = []
                for leg in legs:
                    quote_legs.append({
                        "instrumentName": leg["instrumentName"],
                        "price": "100"  # Example price
                    })
                
                quote_resp = await api.create_quote(request_id, quote_legs)
                quote_id = quote_resp.get('data', {}).get('quoteId')
                print(f"Created quote: {quote_id}")
                
                # 3. Get quote details
                print("\n=== My Open Quotes ===")
                quotes = await api.get_my_open_quotes()
                print(f"Found {len(quotes.get('data', []))} open quotes")
                
                # 4. Cancel the quote
                if quote_id:
                    print(f"\n=== Canceling quote {quote_id} ===")
                    cancel_resp = await api.cancel_quote(quote_id)
                    print(f"CANCELLED: {cancel_resp.get('msg')}")
            
            # 5. Get recent private trades
            print("\n=== Recent Private Trades ===")
            trades = await api.get_private_trades(pageSize=1)
            trade_list = trades.get('data', {}).get('list', [])
            print(f"Found {len(trade_list)} recent trades")
            
        except CcAPIException as e:
            print(f"API Error: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")
            raise


if __name__ == "__main__":
    # Python 3.13+ compatible event loop setup
    import sys
    if sys.version_info >= (3, 10):
        asyncio.run(example_usage())
    else:
        import platform
        if platform.system() == "Windows":
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(example_usage())