from coincall_rfq_maker.pricing.rounding import (
    MIN_OPTION_PRICE,
    apply_price_floor,
    round_option_price,
    round_to_tick,
)


def test_round_to_tick_rounds_to_nearest_cent() -> None:
    assert round_to_tick(1.004) == 1.0
    assert round_to_tick(1.006) == 1.01


def test_round_to_tick_custom_size() -> None:
    assert round_to_tick(1.03, tick_size=0.05) == 1.05


def test_apply_price_floor_enforces_minimum() -> None:
    assert apply_price_floor(0.001) == MIN_OPTION_PRICE
    assert apply_price_floor(5.0) == 5.0


def test_round_option_price_combines_both() -> None:
    assert round_option_price(0.0) == MIN_OPTION_PRICE
    assert round_option_price(0.004) == MIN_OPTION_PRICE
    assert round_option_price(12.346) == 12.35
