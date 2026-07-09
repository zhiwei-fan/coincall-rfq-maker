"""Thin taker-side client over the Phase-1 REST endpoints.

`TakerClient` wraps a `CoincallRestClient` (structurally, via `TakerRestPort`)
and exposes the RFQ surface the taker CLI needs: list instruments, create an
RFQ, view quotes received, cancel an RFQ, re-validate a received quote, and
execute (accept) a quote. There is no taker list-RFQs endpoint (rfqList 404s
for the taker account) — the audit log is the orphan-recovery mechanism.
"""

from collections.abc import Sequence
from typing import Any, Protocol

from coincall_rfq_maker.core.adapters.schemas import (
    ExecuteQuoteResult,
    OptionInstrument,
    QuotePayload,
    RfqCreateResult,
)


class TakerInputError(Exception):
    """Raised when taker CLI input (e.g. a `--leg` string) is malformed or invalid."""


class TakerRestPort(Protocol):
    """The subset of `CoincallRestClient` the taker client depends on."""

    async def get_option_instruments(self, base_currency: str) -> tuple[OptionInstrument, ...]: ...

    async def create_rfq(self, legs: list[dict[str, str]]) -> RfqCreateResult: ...

    async def get_quotes_received(
        self, request_id: str | None = None
    ) -> tuple[QuotePayload, ...]: ...

    async def cancel_rfq(self, request_id: str) -> dict[str, Any]: ...

    async def execute_quote(self, request_id: str, quote_id: str) -> ExecuteQuoteResult: ...


class TakerClient:
    """Safe taker operations over the Coincall REST endpoints."""

    def __init__(self, rest: TakerRestPort) -> None:
        self._rest = rest

    async def list_instruments(
        self, base_currency: str, active_only: bool = False
    ) -> tuple[OptionInstrument, ...]:
        instruments = await self._rest.get_option_instruments(base_currency)
        if active_only:
            return tuple(inst for inst in instruments if inst.is_active)
        return instruments

    def preflight_legs(
        self, raw_legs: list[str], instruments: Sequence[OptionInstrument]
    ) -> list[dict[str, str]]:
        """Validate raw ``SYMBOL:SIDE:QTY`` legs against ``instruments``.

        Returns leg dicts ``{"instrumentName", "side", "qty"}`` ready for
        ``create_rfq``. Raises ``TakerInputError`` (naming the offending leg)
        on any malformed field, unknown/inactive instrument, or sub-min qty.
        """
        by_symbol = {inst.symbol_name: inst for inst in instruments}
        legs: list[dict[str, str]] = []
        for raw in raw_legs:
            parts = raw.split(":")
            if len(parts) != 3:
                raise TakerInputError(
                    f"leg {raw!r}: expected format SYMBOL:SIDE:QTY (got {len(parts)} field(s))"
                )
            symbol, side_raw, qty_raw = (part.strip() for part in parts)

            side = side_raw.upper()
            if side not in ("BUY", "SELL"):
                raise TakerInputError(f"leg {raw!r}: side must be BUY or SELL, got {side_raw!r}")

            instrument = by_symbol.get(symbol)
            if instrument is None:
                raise TakerInputError(f"leg {raw!r}: unknown instrument {symbol!r}")
            if not instrument.is_active:
                raise TakerInputError(f"leg {raw!r}: instrument {symbol!r} is not active")

            try:
                qty = float(qty_raw)
            except ValueError:
                raise TakerInputError(
                    f"leg {raw!r}: quantity {qty_raw!r} is not a number"
                ) from None
            if qty <= 0:
                raise TakerInputError(f"leg {raw!r}: quantity must be positive, got {qty_raw!r}")
            if qty < instrument.min_qty:
                raise TakerInputError(
                    f"leg {raw!r}: quantity {qty_raw} is below min_qty "
                    f"{instrument.min_qty:g} for {symbol!r}"
                )

            legs.append({"instrumentName": symbol, "side": side, "qty": qty_raw})
        return legs

    async def create_rfq(self, legs: list[dict[str, str]]) -> RfqCreateResult:
        return await self._rest.create_rfq(legs)

    async def get_quotes(self, request_id: str) -> tuple[QuotePayload, ...]:
        return await self._rest.get_quotes_received(request_id)

    async def cancel_rfq(self, request_id: str) -> None:
        await self._rest.cancel_rfq(request_id)

    async def execute_quote(self, request_id: str, quote_id: str) -> ExecuteQuoteResult:
        """Accept ``quote_id`` for ``request_id`` — places a REAL block trade.

        ``execute_quote`` is non-idempotent and MUST NOT be retried; a transport
        failure surfaces as ``CoincallAmbiguousError`` for the caller to handle.
        """
        return await self._rest.execute_quote(request_id, quote_id)

    async def find_received_quote(self, request_id: str, quote_id: str) -> QuotePayload | None:
        """Return the still-live received quote matching both ids, else ``None``.

        Used for pre-execute re-validation: the quote must still exist and belong
        to this RFQ. Returns ``None`` when it has expired off the book or the ids
        do not line up.
        """
        quotes = await self._rest.get_quotes_received(request_id)
        for quote in quotes:
            # Tolerate a quote that omits requestId (the live getQuotesReceived
            # response may not echo it per quote) — else every execute would
            # refuse as not_found. A PRESENT-but-mismatched requestId is still
            # rejected.
            if quote.quote_id == quote_id and (
                quote.request_id is None or quote.request_id == request_id
            ):
                return quote
        return None
