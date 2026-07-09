"""Coincall WebSocket client.

Signed URL, subscriptions, dt-code routing, and reconnect/backoff are ported
EXACTLY from the old `websocket_client.py` (channels `rfqMaker`, `rfqQuote`,
`blockTradeDetail`, `blockTradePublic`; dt 28/20/22/23).

Actor-model boundary: this module only PARSES and ENQUEUES typed events. It
never touches RFQ/quote/store state directly.
"""

import asyncio
import json
import logging
from enum import IntEnum
from typing import Protocol

import websockets

from coincall_rfq_maker.adapters.schemas import (
    BlockTradePayload,
    QuotePayload,
    RfqPayload,
    WsEnvelope,
)
from coincall_rfq_maker.adapters.signing import build_ws_signed_url
from coincall_rfq_maker.domain.quote import QuoteStage
from coincall_rfq_maker.domain.rfq import Rfq, RfqLeg, RfqStatus
from coincall_rfq_maker.events import QuoteUpdated, RfqReceived, RfqTerminated, TradeExecuted

logger = logging.getLogger(__name__)

WsEvent = RfqReceived | RfqTerminated | QuoteUpdated | TradeExecuted

SUBSCRIPTIONS: tuple[str, ...] = ("rfqMaker", "rfqQuote", "blockTradeDetail", "blockTradePublic")

_CONNECT_TIMEOUT_SECONDS = 30.0
_RECV_TIMEOUT_SECONDS = 60.0
_INITIAL_RECONNECT_DELAY_SECONDS = 1.0
_MAX_RECONNECT_DELAY_SECONDS = 60.0
_DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 5.0
_HEARTBEAT_MESSAGE = json.dumps({"action": "heartbeat"})

_WIRE_QUOTE_STATE_TO_STAGE = {
    "OPEN": QuoteStage.OPEN,
    "CANCELLED": QuoteStage.CANCELLED,
    "FILLED": QuoteStage.FILLED,
    "EXPIRED": QuoteStage.EXPIRED,
}

_TERMINAL_RFQ_STATES = {
    RfqStatus.CANCELLED,
    RfqStatus.EXPIRED,
    RfqStatus.FILLED,
    RfqStatus.TRADED_AWAY,
}


class _WsConnection(Protocol):
    async def send(self, message: str) -> None: ...

    async def recv(self) -> str | bytes: ...


class DtCode(IntEnum):
    """WS `dt` (data type) routing codes, ported exactly."""

    RFQ_MAKER = 28
    RFQ_QUOTE = 20
    BLOCK_TRADE_DETAIL = 22
    BLOCK_TRADE_PUBLIC = 23


def parse_ws_message(raw: str) -> WsEvent | None:
    """Parse one raw WS text frame into a typed event, or None if it's a
    control message / heartbeat / unroutable payload (which is logged)."""
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("Received non-JSON WS message: %.100s", raw)
        return None

    envelope = WsEnvelope.model_validate(obj)

    if envelope.dt is None:
        if envelope.action and envelope.result:
            logger.debug("WS control message: %s -> %s", envelope.action, envelope.result)
        elif envelope.c == 11 and envelope.rc == 1:
            logger.debug("WS heartbeat received")
        else:
            logger.debug("WS message with no dt/control fields: %s", obj)
        return None

    data = envelope.d or {}
    if envelope.dt == DtCode.RFQ_MAKER:
        return _parse_rfq_event(data)
    if envelope.dt == DtCode.RFQ_QUOTE:
        return _parse_quote_event(data)
    if envelope.dt == DtCode.BLOCK_TRADE_DETAIL:
        return _parse_trade_event(data)
    if envelope.dt == DtCode.BLOCK_TRADE_PUBLIC:
        logger.debug("Ignoring public block trade tape message")
        return None

    logger.warning("Unknown WS dt code %s, ignoring", envelope.dt)
    return None


def _parse_rfq_event(data: dict[str, object]) -> RfqReceived | RfqTerminated | None:
    try:
        payload = RfqPayload.model_validate(data)
        status = RfqStatus(payload.state)
    except ValueError as exc:
        logger.warning("Malformed RFQ WS payload: %s", exc)
        return None

    if status is RfqStatus.ACTIVE:
        rfq = Rfq(
            request_id=payload.request_id,
            status=status,
            legs=tuple(
                RfqLeg(instrument_name=leg.instrument_name, side=leg.side, quantity=leg.quantity)
                for leg in payload.legs
            ),
            create_time_ms=payload.create_time or 0,
            expiry_time_ms=payload.expiry_time or 0,
            taker_name=payload.taker_name,
            counterparty=payload.counterparty,
            last_update_time_ms=payload.update_time,
        )
        return RfqReceived(rfq=rfq)
    if status in _TERMINAL_RFQ_STATES:
        return RfqTerminated(request_id=payload.request_id, status=status)
    logger.warning("Unknown RFQ state %r for %s", payload.state, payload.request_id)
    return None


def _parse_quote_event(data: dict[str, object]) -> QuoteUpdated | None:
    payload = QuotePayload.model_validate(data)
    stage = _WIRE_QUOTE_STATE_TO_STAGE.get(payload.state)
    if stage is None:
        logger.warning("Unknown quote state %r for %s", payload.state, payload.quote_id)
        return None
    return QuoteUpdated(
        quote_id=payload.quote_id,
        request_id=payload.request_id or "",
        stage=stage,
        filled_price=payload.filled_price,
        filled_quantity=payload.filled_quantity,
        fill_time_ms=payload.fill_time,
        block_trade_id=payload.block_trade_id,
    )


def _parse_trade_event(data: dict[str, object]) -> TradeExecuted:
    payload = BlockTradePayload.model_validate(data)
    return TradeExecuted(
        block_trade_id=payload.block_trade_id,
        quote_id=payload.quote_id or "",
        request_id=payload.request_id,
    )


class CoincallWsClient:
    """Owns the WS connection lifecycle; enqueues typed events for consumers."""

    def __init__(
        self,
        ws_url: str,
        api_key: str,
        api_secret: str,
        event_queue: "asyncio.Queue[object]",
        heartbeat_interval_seconds: float = _DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    ) -> None:
        self._ws_url = ws_url
        self._api_key = api_key
        self._api_secret = api_secret
        self._queue = event_queue
        self._heartbeat_interval_seconds = heartbeat_interval_seconds

    async def run(self, shutdown: asyncio.Event) -> None:
        """Connect, subscribe, and read until `shutdown` is set, reconnecting with backoff."""
        delay = _INITIAL_RECONNECT_DELAY_SECONDS
        while not shutdown.is_set():
            try:
                await self._connect_and_read(shutdown)
                delay = _INITIAL_RECONNECT_DELAY_SECONDS
            except Exception:
                logger.exception("WS connection error")
            if shutdown.is_set():
                return
            logger.info("Reconnecting WS in %.1fs", delay)
            try:
                async with asyncio.timeout(delay + 1.0):
                    await shutdown.wait()
                return
            except TimeoutError:
                pass
            delay = min(delay * 2, _MAX_RECONNECT_DELAY_SECONDS)

    async def _connect_and_read(self, shutdown: asyncio.Event) -> None:
        url = build_ws_signed_url(self._ws_url, self._api_key, self._api_secret)
        async with asyncio.timeout(_CONNECT_TIMEOUT_SECONDS):
            connection = await websockets.connect(
                url, ping_interval=20, ping_timeout=10, close_timeout=10
            )
        logger.info("WS connected")
        async with connection:
            for data_type in SUBSCRIPTIONS:
                await connection.send(json.dumps({"action": "subscribe", "dataType": data_type}))
            logger.info("Subscribed to %s", SUBSCRIPTIONS)
            await self._supervise_connection(connection, shutdown)

    async def _supervise_connection(
        self, connection: _WsConnection, shutdown: asyncio.Event
    ) -> None:
        read_task = asyncio.create_task(self._read_loop(connection, shutdown), name="ws-read")
        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(connection, shutdown), name="ws-heartbeat"
        )
        try:
            done, _pending = await asyncio.wait(
                {read_task, heartbeat_task}, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                task.result()
        finally:
            for task in (read_task, heartbeat_task):
                task.cancel()
            await asyncio.gather(read_task, heartbeat_task, return_exceptions=True)

    async def _read_loop(self, connection: _WsConnection, shutdown: asyncio.Event) -> None:
        while not shutdown.is_set():
            try:
                async with asyncio.timeout(_RECV_TIMEOUT_SECONDS):
                    raw = await connection.recv()
            except TimeoutError:
                continue
            text = raw.decode() if isinstance(raw, bytes) else raw
            event = parse_ws_message(text)
            if event is not None:
                try:
                    await self._queue.put(event)
                except asyncio.QueueShutDown:
                    logger.debug("WS event queue shut down; exiting read loop")
                    return

    async def _heartbeat_loop(self, connection: _WsConnection, shutdown: asyncio.Event) -> None:
        while not shutdown.is_set():
            await connection.send(_HEARTBEAT_MESSAGE)
            try:
                async with asyncio.timeout(self._heartbeat_interval_seconds):
                    await shutdown.wait()
                return
            except TimeoutError:
                pass
