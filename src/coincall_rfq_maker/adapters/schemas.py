"""Pydantic models for Coincall REST/WS payloads — the adapter boundary.

Field names mirror the wire's camelCase exactly (via aliases) so we can
`model_validate` raw exchange JSON directly.
"""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from coincall_rfq_maker.domain.rfq import Rfq, RfqLeg, RfqStatus, Side


class ApiEnvelope(BaseModel):
    """Generic `{code, data, msg}` REST response envelope."""

    model_config = ConfigDict(extra="ignore")

    code: int
    msg: str | None = None
    data: Any = None


class RfqLegPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    instrument_name: str = Field(alias="instrumentName")
    side: Side
    quantity: str


class RfqPayload(BaseModel):
    """An RFQ as reported by REST (rfqList) or WS (dt=28)."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    request_id: str = Field(alias="requestId")
    state: str
    legs: list[RfqLegPayload] = Field(default_factory=list)
    create_time: int | None = Field(default=None, alias="createTime")
    expiry_time: int | None = Field(default=None, alias="expiryTime")
    taker_name: str | None = Field(default=None, alias="takerName")
    counterparty: str | None = Field(default=None, alias="counterparty")
    update_time: int | None = Field(default=None, alias="updateTime")


def rfq_from_payload(payload: RfqPayload) -> Rfq:
    """Convert a validated REST/WS RFQ payload into the domain model."""
    status = RfqStatus(payload.state)
    return Rfq(
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


class QuoteLegPayload(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    instrument_name: str = Field(alias="instrumentName")
    price: str
    side: Side | None = None
    quantity: str | None = None


class QuotePayload(BaseModel):
    """A quote as reported by REST (list-quote) or WS (dt=20)."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    quote_id: str = Field(alias="quoteId")
    request_id: str | None = Field(default=None, alias="requestId")
    state: str = "OPEN"
    legs: list[QuoteLegPayload] = Field(default_factory=list)
    create_time: int | None = Field(default=None, alias="createTime")
    update_time: int | None = Field(default=None, alias="updateTime")
    expiry_time: int | None = Field(default=None, alias="expiryTime")
    filled_price: float | None = Field(default=None, alias="filledPrice")
    filled_quantity: float | None = Field(default=None, alias="filledQuantity")
    fill_time: int | None = Field(default=None, alias="fillTime")
    block_trade_id: str | None = Field(default=None, alias="blockTradeId")


class BlockTradePayload(BaseModel):
    """A block trade fill notification (WS dt=22)."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    block_trade_id: str = Field(alias="blockTradeId")
    quote_id: str | None = Field(default=None, alias="quoteId")
    request_id: str | None = Field(default=None, alias="requestId")


class SymbolInfoPayload(BaseModel):
    """Futures symbol info (index/mark price) used as the pricing underlying."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    symbol: str | None = None
    index_price: float | None = Field(default=None, alias="indexPrice")
    mark_price: float | None = Field(default=None, alias="markPrice")

    @property
    def underlying_price(self) -> float | None:
        """Prefer indexPrice, fall back to markPrice — ported from old market_data.py."""
        if self.index_price is not None and self.index_price > 0:
            return self.index_price
        if self.mark_price is not None and self.mark_price > 0:
            return self.mark_price
        return None


class WsEnvelope(BaseModel):
    """Top-level WS message shape used to route by `dt` (data type) code."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    dt: int | None = None
    d: dict[str, Any] | None = None
    action: str | None = None
    result: str | None = None
    c: int | None = None
    rc: int | None = None
