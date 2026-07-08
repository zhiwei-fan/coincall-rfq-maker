from datetime import UTC, datetime

from coincall_rfq_maker.domain.instruments import Instrument, OptionType
from coincall_rfq_maker.pricing.engine import BlackScholesModel
from coincall_rfq_maker.pricing.rounding import MIN_OPTION_PRICE

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
    assert low_strike.bid > high_strike.bid
    assert low_strike.ask > high_strike.ask


def test_put_price_increases_as_strike_increases() -> None:
    model = BlackScholesModel()
    spot = 50_000.0
    low_strike = model.price(make_instrument(40_000.0, OptionType.PUT), spot, NOW)
    high_strike = model.price(make_instrument(60_000.0, OptionType.PUT), spot, NOW)
    assert high_strike.bid > low_strike.bid
    assert high_strike.ask > low_strike.ask


def test_ask_vol_greater_than_bid_vol_widens_spread() -> None:
    model = BlackScholesModel(bid_vol=0.20, ask_vol=2.00, risk_free_rate=0.05)
    priced = model.price(make_instrument(50_000.0, OptionType.CALL), 50_000.0, NOW)
    assert priced.ask >= priced.bid


def test_expired_instrument_prices_at_floor() -> None:
    model = BlackScholesModel()
    expired = make_instrument(50_000.0, OptionType.CALL, expiry_year=2020)
    priced = model.price(expired, 50_000.0, NOW)
    assert priced.bid == MIN_OPTION_PRICE
    assert priced.ask == MIN_OPTION_PRICE


def test_prices_never_below_floor() -> None:
    model = BlackScholesModel(bid_vol=0.01, ask_vol=0.01)
    deep_otm_call = make_instrument(1_000_000.0, OptionType.CALL)
    priced = model.price(deep_otm_call, 50_000.0, NOW)
    assert priced.bid >= MIN_OPTION_PRICE
    assert priced.ask >= MIN_OPTION_PRICE
