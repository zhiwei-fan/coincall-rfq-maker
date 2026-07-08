"""Tick rounding and the exchange's minimum option price.

The $0.01 floor is salvaged from the old `main.py` design-note docstring
("Min option price = $0.01"); the old pricer never actually applied it.
"""

MIN_OPTION_PRICE = 0.01
DEFAULT_TICK_SIZE = 0.01


def round_to_tick(price: float, tick_size: float = DEFAULT_TICK_SIZE) -> float:
    """Round `price` to the nearest multiple of `tick_size`."""
    if tick_size <= 0:
        raise ValueError("tick_size must be positive")
    ticks = round(price / tick_size)
    return round(ticks * tick_size, 10)


def apply_price_floor(price: float, floor: float = MIN_OPTION_PRICE) -> float:
    """Never quote below the exchange's minimum option price."""
    return max(price, floor)


def round_option_price(
    price: float, tick_size: float = DEFAULT_TICK_SIZE, floor: float = MIN_OPTION_PRICE
) -> float:
    """Round to tick, then apply the price floor."""
    return apply_price_floor(round_to_tick(price, tick_size), floor)
