"""In-process Coincall-like exchange for maker-vs-taker harness tests."""

import asyncio
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal

from coincall_rfq_maker.core.adapters.rest import CoincallApiError, CoincallRequestError
from coincall_rfq_maker.core.adapters.schemas import (
    CreateQuoteResult,
    OptionInstrument,
    QuoteListSnapshot,
    QuotePayload,
    RfqListSnapshot,
    RfqPayload,
    SymbolInfoPayload,
)
from coincall_rfq_maker.domain.instruments import InstrumentParseError, parse_instrument
from coincall_rfq_maker.domain.quote import QuoteStage
from coincall_rfq_maker.domain.rfq import Rfq, RfqLeg, RfqStatus
from coincall_rfq_maker.events import QuoteUpdated, RfqReceived, RfqTerminated, TradeExecuted


@dataclass(slots=True)
class _QuoteRecord:
    quote_id: str
    request_id: str
    legs: list[dict[str, str]]
    state: Literal["OPEN", "CANCELLED", "FILLED", "EXPIRED"] = "OPEN"
    create_time_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    update_time_ms: int | None = None
    filled_price: float | None = None
    filled_quantity: float | None = None
    fill_time_ms: int | None = None
    block_trade_id: str | None = None


class FakeExchange:
    """A deterministic fake that duck-types the maker REST client.

    Maker methods match `CoincallRestClient`; taker methods create/cancel RFQs
    and execute quotes while emitting the typed events the WS parser emits.
    """

    def __init__(
        self,
        event_queue: "asyncio.Queue[object]",
        underlying_prices: dict[str, float] | None = None,
    ) -> None:
        self._events = event_queue
        self._underlying_prices = dict(underlying_prices or {"BTCUSD": 100_000.0})
        self._rfqs: dict[str, Rfq] = {}
        self._quotes: dict[str, _QuoteRecord] = {}
        self._next_rfq_id = 1
        self._next_quote_id = 1
        self._next_block_trade_id = 1

    def set_underlying_price(self, underlying: str, price: float) -> None:
        self._underlying_prices[underlying] = price

    async def get_rfq_list(
        self,
        request_id: str | None = None,
        state: Literal["OPEN", "CLOSED"] | None = None,
        role: Literal["TAKER", "MAKER"] | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> RfqListSnapshot:
        del role, start_time, end_time
        rfqs = list(self._rfqs.values())
        if request_id is not None:
            rfqs = [rfq for rfq in rfqs if rfq.request_id == request_id]
        if state == "OPEN":
            rfqs = [rfq for rfq in rfqs if not rfq.is_terminal_status]
        elif state == "CLOSED":
            rfqs = [rfq for rfq in rfqs if rfq.is_terminal_status]
        return RfqListSnapshot(
            tuple(RfqPayload.model_validate(self._rfq_payload(rfq)) for rfq in rfqs)
        )

    async def create_quote(self, request_id: str, legs: list[dict[str, str]]) -> CreateQuoteResult:
        rfq = self._rfqs.get(request_id)
        if rfq is None or rfq.is_terminal_status:
            raise CoincallRequestError(f"Cannot quote unknown or terminal RFQ {request_id}")

        quote_id = f"quote-{self._next_quote_id}"
        self._next_quote_id += 1
        self._quotes[quote_id] = _QuoteRecord(
            quote_id=quote_id,
            request_id=request_id,
            legs=[dict(leg) for leg in legs],
        )
        await self._events.put(QuoteUpdated(quote_id, request_id, QuoteStage.OPEN))
        return CreateQuoteResult.model_validate({"quoteId": quote_id})

    async def cancel_quote(self, quote_id: str) -> dict[str, Any]:
        quote = self._quote_or_error(quote_id)
        if quote.state == "OPEN":
            quote.state = "CANCELLED"
            quote.update_time_ms = self._now_ms()
            await self._events.put(QuoteUpdated(quote_id, quote.request_id, QuoteStage.CANCELLED))
        return {"code": 0, "data": {}}

    async def cancel_all_quotes(self) -> dict[str, Any]:
        for quote_id in list(self._quotes):
            await self.cancel_quote(quote_id)
        return {"code": 0, "data": {}}

    async def get_quote_list(
        self,
        quote_id: str | None = None,
        request_id: str | None = None,
        state: Literal["OPEN", "CLOSED"] | None = None,
        symbol: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> QuoteListSnapshot:
        del start_time, end_time
        quotes = list(self._quotes.values())
        if quote_id is not None:
            quotes = [quote for quote in quotes if quote.quote_id == quote_id]
        if request_id is not None:
            quotes = [quote for quote in quotes if quote.request_id == request_id]
        if state == "OPEN":
            quotes = [quote for quote in quotes if quote.state == "OPEN"]
        elif state == "CLOSED":
            quotes = [quote for quote in quotes if quote.state != "OPEN"]
        if symbol is not None:
            quotes = [
                quote
                for quote in quotes
                if any(leg.get("instrumentName") == symbol for leg in quote.legs)
            ]
        return QuoteListSnapshot(
            tuple(QuotePayload.model_validate(self._quote_payload(quote)) for quote in quotes)
        )

    async def get_symbol_info(self, symbol: str | None = None) -> SymbolInfoPayload:
        underlying = symbol or next(iter(self._underlying_prices))
        price = self._underlying_prices.get(underlying)
        if price is None:
            raise _fake_api_error(f"No fake price configured for {underlying}")
        return SymbolInfoPayload.model_validate(
            {"symbol": underlying, "indexPrice": price, "markPrice": price}
        )

    async def get_option_instruments(self, base_currency: str) -> tuple[OptionInstrument, ...]:
        symbols = {leg.instrument_name for rfq in self._rfqs.values() for leg in rfq.legs}
        instruments = []
        for symbol in symbols:
            try:
                parsed = parse_instrument(symbol)
            except InstrumentParseError:
                continue
            if parsed.underlying != f"{base_currency}USD":
                continue
            expiry = datetime.combine(parsed.expiry_date, datetime.min.time(), tzinfo=UTC)
            instruments.append(
                OptionInstrument.model_validate(
                    {
                        "symbolName": symbol,
                        "baseCurrency": base_currency,
                        "strike": parsed.strike,
                        "expirationTimestamp": int(expiry.timestamp() * 1000),
                        "isActive": True,
                        "minQty": 0.01,
                        "tickSize": 0.01,
                    }
                )
            )
        return tuple(instruments)

    async def create_rfq(self, legs: tuple[RfqLeg, ...]) -> str:
        request_id = f"rfq-{self._next_rfq_id}"
        self._next_rfq_id += 1
        now_ms = self._now_ms()
        rfq = Rfq(
            request_id=request_id,
            status=RfqStatus.ACTIVE,
            legs=legs,
            create_time_ms=now_ms,
            expiry_time_ms=4_000_000_000_000,
            taker_name="fake-taker",
            counterparty="maker",
            last_update_time_ms=now_ms,
        )
        self._rfqs[request_id] = rfq
        await self._events.put(RfqReceived(rfq))
        return request_id

    async def execute_quote(self, quote_id: str) -> str:
        quote = self._quote_or_error(quote_id)
        if quote.state != "OPEN":
            raise _fake_api_error(f"Quote {quote_id} is not open")

        rfq = self._rfq_or_error(quote.request_id)
        if rfq.is_terminal_status:
            raise _fake_api_error(f"RFQ {quote.request_id} is terminal")

        now_ms = self._now_ms()
        block_trade_id = f"block-{self._next_block_trade_id}"
        self._next_block_trade_id += 1

        quote.state = "FILLED"
        quote.update_time_ms = now_ms
        quote.fill_time_ms = now_ms
        quote.block_trade_id = block_trade_id
        quote.filled_price = float(quote.legs[0]["price"])
        quote.filled_quantity = sum(float(leg.quantity) for leg in rfq.legs)
        self._rfqs[rfq.request_id] = rfq.with_status(RfqStatus.FILLED, now_ms)

        await self._events.put(
            QuoteUpdated(
                quote_id=quote_id,
                request_id=quote.request_id,
                stage=QuoteStage.FILLED,
                filled_price=quote.filled_price,
                filled_quantity=quote.filled_quantity,
                fill_time_ms=quote.fill_time_ms,
                block_trade_id=block_trade_id,
            )
        )
        await self._events.put(
            TradeExecuted(
                block_trade_id=block_trade_id,
                quote_id=quote_id,
                request_id=quote.request_id,
            )
        )
        await self._events.put(RfqTerminated(quote.request_id, RfqStatus.FILLED))
        return block_trade_id

    async def cancel_rfq(self, request_id: str) -> None:
        rfq = self._rfq_or_error(request_id)
        if not rfq.is_terminal_status:
            now_ms = self._now_ms()
            self._rfqs[request_id] = rfq.with_status(RfqStatus.CANCELLED, now_ms)
            for quote in self._quotes.values():
                if quote.request_id == request_id and quote.state == "OPEN":
                    quote.state = "CANCELLED"
                    quote.update_time_ms = now_ms
                    await self._events.put(
                        QuoteUpdated(quote.quote_id, request_id, QuoteStage.CANCELLED)
                    )
            await self._events.put(RfqTerminated(request_id, RfqStatus.CANCELLED))

    def _rfq_or_error(self, request_id: str) -> Rfq:
        rfq = self._rfqs.get(request_id)
        if rfq is None:
            raise CoincallRequestError(f"Unknown RFQ {request_id}")
        return rfq

    def _quote_or_error(self, quote_id: str) -> _QuoteRecord:
        quote = self._quotes.get(quote_id)
        if quote is None:
            raise CoincallRequestError(f"Unknown quote {quote_id}")
        return quote

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    @staticmethod
    def _rfq_payload(rfq: Rfq) -> dict[str, Any]:
        return {
            "requestId": rfq.request_id,
            "state": rfq.status.value,
            "legs": [
                {
                    "instrumentName": leg.instrument_name,
                    "side": leg.side.value,
                    "quantity": leg.quantity,
                }
                for leg in rfq.legs
            ],
            "createTime": rfq.create_time_ms,
            "expiryTime": rfq.expiry_time_ms,
            "takerName": rfq.taker_name,
            "counterparty": rfq.counterparty,
            "updateTime": rfq.last_update_time_ms,
        }

    @staticmethod
    def _quote_payload(quote: _QuoteRecord) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "quoteId": quote.quote_id,
            "requestId": quote.request_id,
            "state": quote.state,
            "legs": quote.legs,
            "createTime": quote.create_time_ms,
            "updateTime": quote.update_time_ms,
        }
        if quote.filled_price is not None:
            payload["filledPrice"] = quote.filled_price
        if quote.filled_quantity is not None:
            payload["filledQuantity"] = quote.filled_quantity
        if quote.fill_time_ms is not None:
            payload["fillTime"] = quote.fill_time_ms
        if quote.block_trade_id is not None:
            payload["blockTradeId"] = quote.block_trade_id
        return payload


def _fake_api_error(message: str) -> CoincallApiError:
    return CoincallApiError(status=200, code=400001, message=message)
