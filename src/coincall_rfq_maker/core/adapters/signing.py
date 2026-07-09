"""Coincall request signing — ported EXACTLY from the old `api_client.py` /
`websocket_client.py`. Pure functions only, golden-vector tested against the
original implementation's output (see tests/adapters/test_signing.py).

Do not change the string construction here without re-deriving golden
vectors from a live account — any drift breaks authentication silently.
"""

import hashlib
import hmac
import json
from typing import Any
from urllib.parse import quote

from coincall_rfq_maker.core.clock import get_timestamp_ms as _get_timestamp_ms


def get_timestamp_ms() -> int:
    """Current UNIX timestamp in milliseconds."""
    return _get_timestamp_ms()


def build_rest_prehash(
    timestamp: int,
    method: str,
    path: str,
    api_key: str,
    diff: int,
    body: dict[str, Any] | None = None,
) -> str:
    """Construct the REST prehash string: METHOD + path + '?' + sorted_body + '&' + auth_params."""
    sorted_pairs = []
    if body:
        for key in sorted(body):
            value = body[key]
            if value is None:
                continue
            if isinstance(value, list | dict):
                value_str = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
            else:
                value_str = str(value)
            sorted_pairs.append(f"{key}={value_str}")
    body_str = "&".join(sorted_pairs)
    connector = f"?{body_str}&" if body_str else "?"
    return f"{method.upper()}{path}{connector}uuid={api_key}&ts={timestamp}&x-req-ts-diff={diff}"


def sign_message(message: str, secret_key: str) -> str:
    """HMAC-SHA256 of `message` with `secret_key`, uppercase hex."""
    return hmac.new(secret_key.encode(), message.encode(), hashlib.sha256).hexdigest().upper()


def build_rest_headers(api_key: str, signature: str, timestamp: int, diff: int) -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "X-CC-APIKEY": api_key,
        "sign": signature,
        "ts": str(timestamp),
        "X-REQ-TS-DIFF": str(diff),
    }


def sign_rest_request(
    method: str,
    path: str,
    api_key: str,
    secret_key: str,
    timestamp: int,
    diff: int = 5000,
    body: dict[str, Any] | None = None,
) -> dict[str, str]:
    """Compute the signed headers for one REST request."""
    message = build_rest_prehash(timestamp, method, path, api_key, diff, body)
    signature = sign_message(message, secret_key)
    return build_rest_headers(api_key, signature, timestamp, diff)


def encode_query_params(params: dict[str, Any]) -> str:
    """URL-encode GET parameters into a leading-`?` query string (or "" if empty)."""
    if not params:
        return ""
    parts = [f"{key}={quote(str(value))}" for key, value in params.items() if value is not None]
    return "?" + "&".join(parts)


def build_ws_signed_url(
    base_url: str,
    api_key: str,
    api_secret: str,
    timestamp: int | None = None,
) -> str:
    """Build the signed WS connection URL (verb/uri/params ported exactly)."""
    ts = timestamp if timestamp is not None else get_timestamp_ms()
    verb = "GET"
    uri = "/users/self/verify"
    auth = f"{verb}{uri}?uuid={api_key}&ts={ts}"
    signature = sign_message(auth, api_secret)
    params = f"?code=10&uuid={api_key}&ts={ts}&sign={signature}&apiKey={api_key}"
    return base_url + params
