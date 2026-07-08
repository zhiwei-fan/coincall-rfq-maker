"""Quoting strategy: turn a priced RFQ into a `QuoteIntent`.

Leg-side pricing is ported exactly from the old
`QuoteOrchestrator.create_quote_for_rfq`: the customer's BUY is quoted at our
ask, the customer's SELL is quoted at our bid.
"""

from collections.abc import Mapping
from dataclasses import dataclass

from coincall_rfq_maker.domain.rfq import Rfq, Side
from coincall_rfq_maker.pricing.engine import LegPrice


@dataclass(frozen=True, slots=True)
class QuoteLegIntent:
    instrument_name: str
    price: float


@dataclass(frozen=True, slots=True)
class QuoteIntent:
    request_id: str
    legs: tuple[QuoteLegIntent, ...]


def build_quote_intent(rfq: Rfq, leg_prices: Mapping[str, LegPrice]) -> QuoteIntent | None:
    """Build the quote we'd submit for `rfq`, or None if any leg is unpriced."""
    legs = []
    for leg in rfq.legs:
        leg_price = leg_prices.get(leg.instrument_name)
        if leg_price is None:
            return None
        price = leg_price.ask if leg.side is Side.BUY else leg_price.bid
        legs.append(QuoteLegIntent(instrument_name=leg.instrument_name, price=price))
    return QuoteIntent(request_id=rfq.request_id, legs=tuple(legs))
