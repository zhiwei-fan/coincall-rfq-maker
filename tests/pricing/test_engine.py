from datetime import UTC, datetime

import pytest

from coincall_rfq_maker.domain.instruments import Instrument, OptionType
from coincall_rfq_maker.pricing.engine import BlackScholesModel
from coincall_rfq_maker.pricing.rounding import round_option_price

NOW = datetime(2025, 1, 1, tzinfo=UTC)


def make_instrument(strike: float, option_type: OptionType, expiry_year: int = 2026) -> Instrument:
    return Instrument(
        symbol=f"BTCUSD-01JAN{expiry_year - 2000}-{int(strike)}-{option_type.value}",
        underlying="BTCUSD",
        expiry=datetime(expiry_year, 1, 1, tzinfo=UTC),
        strike=strike,
        option_type=option_type,
    )


def test_call_price_increases_as_strike_decreases() -> None:
    model = BlackScholesModel()
    spot = 50_000.0
    low_strike = model.price(make_instrument(40_000.0, OptionType.CALL), spot, NOW)
    high_strike = model.price(make_instrument(60_000.0, OptionType.CALL), spot, NOW)
    assert low_strike is not None
    assert high_strike is not None
    assert low_strike.bid > high_strike.bid
    assert low_strike.ask > high_strike.ask


def test_put_price_increases_as_strike_increases() -> None:
    model = BlackScholesModel()
    spot = 50_000.0
    low_strike = model.price(make_instrument(40_000.0, OptionType.PUT), spot, NOW)
    high_strike = model.price(make_instrument(60_000.0, OptionType.PUT), spot, NOW)
    assert low_strike is not None
    assert high_strike is not None
    assert high_strike.bid > low_strike.bid
    assert high_strike.ask > low_strike.ask


def test_ask_vol_greater_than_bid_vol_widens_spread() -> None:
    model = BlackScholesModel(bid_vol=0.20, ask_vol=2.00, risk_free_rate=0.05)
    priced = model.price(make_instrument(50_000.0, OptionType.CALL), 50_000.0, NOW)
    assert priced is not None
    assert priced.ask >= priced.bid


def test_expired_deep_itm_instrument_is_unpriceable() -> None:
    model = BlackScholesModel()
    expired = Instrument(
        symbol="BTCUSD-09JUL26-56000-C",
        underlying="BTCUSD",
        expiry=datetime(2026, 7, 9, tzinfo=UTC),
        strike=56_000.0,
        option_type=OptionType.CALL,
    )
    priced = model.price(expired, 60_000.0, datetime(2026, 7, 9, 2, 0, tzinfo=UTC))
    assert priced is None


def test_floor_applies_to_small_finite_valuations() -> None:
    assert round_option_price(0.004) == 0.01

    model = BlackScholesModel(bid_vol=0.01, ask_vol=0.01)
    deep_otm_call = make_instrument(1_000_000.0, OptionType.CALL)
    priced = model.price(deep_otm_call, 50_000.0, NOW)
    assert priced is not None
    assert priced.bid == 0.01
    assert priced.ask == 0.01


@pytest.mark.parametrize("underlying_price", [float("nan"), float("inf")])
def test_non_finite_valuation_is_unpriceable(underlying_price: float) -> None:
    priced = BlackScholesModel().price(
        make_instrument(50_000.0, OptionType.CALL), underlying_price, NOW
    )
    assert priced is None
