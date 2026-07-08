"""Pricing model Protocol and the Black-Scholes stub implementation.

`BlackScholesModel` is ported from the old `pricing_engine.BlackScholesPricer`:
fixed bid/ask vols straddling the true vol (explicitly a stub — no vol
surface, no skew). It is deliberately swappable via the `PricingModel`
Protocol so a real model can replace it later without touching callers.
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

import numpy as np
from scipy.stats import norm

from coincall_rfq_maker.domain.instruments import Instrument, OptionType
from coincall_rfq_maker.pricing.rounding import round_option_price


@dataclass(frozen=True, slots=True)
class LegPrice:
    bid: float
    ask: float


class PricingModel(Protocol):
    def price(
        self, instrument: Instrument, underlying_price: float, now: datetime | None = None
    ) -> LegPrice: ...


@dataclass(frozen=True, slots=True)
class BlackScholesModel:
    """Explicit stub model: fixed bid/ask vols, not a real pricing surface."""

    bid_vol: float = 0.20
    ask_vol: float = 2.00
    risk_free_rate: float = 0.05

    def price(
        self, instrument: Instrument, underlying_price: float, now: datetime | None = None
    ) -> LegPrice:
        reference = now if now is not None else datetime.now(UTC)
        time_to_expiry = instrument.time_to_expiry_years(reference)
        if time_to_expiry <= 0:
            return LegPrice(bid=round_option_price(0.0), ask=round_option_price(0.0))

        bid = _black_scholes(
            underlying_price,
            instrument.strike,
            time_to_expiry,
            self.risk_free_rate,
            self.bid_vol,
            instrument.option_type,
        )
        ask = _black_scholes(
            underlying_price,
            instrument.strike,
            time_to_expiry,
            self.risk_free_rate,
            self.ask_vol,
            instrument.option_type,
        )
        return LegPrice(bid=round_option_price(bid), ask=round_option_price(ask))


def _black_scholes(
    spot: float,
    strike: float,
    time_to_expiry: float,
    risk_free_rate: float,
    sigma: float,
    option_type: OptionType,
) -> float:
    d1 = (np.log(spot / strike) + (risk_free_rate + 0.5 * sigma**2) * time_to_expiry) / (
        sigma * np.sqrt(time_to_expiry)
    )
    d2 = d1 - sigma * np.sqrt(time_to_expiry)

    if option_type is OptionType.CALL:
        price = spot * norm.cdf(d1) - strike * np.exp(-risk_free_rate * time_to_expiry) * norm.cdf(
            d2
        )
    else:
        price = strike * np.exp(-risk_free_rate * time_to_expiry) * norm.cdf(-d2) - spot * norm.cdf(
            -d1
        )

    return max(float(price), 0.0)
