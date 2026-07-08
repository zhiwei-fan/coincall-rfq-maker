"""Instrument value object and symbol parser.

Symbol format ported exactly from the old ``pricing_engine.InstrumentParser``:
``{UNDERLYING}-{DDMONYY}-{STRIKE}-{C|P}``, e.g. ``BTCUSD-21AUG25-120000-C``.
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum

_MONTHS = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}


class OptionType(StrEnum):
    CALL = "C"
    PUT = "P"


class InstrumentParseError(ValueError):
    """Raised when an instrument symbol does not match the expected format."""


@dataclass(frozen=True, slots=True)
class Instrument:
    """Immutable option instrument definition."""

    symbol: str
    underlying: str
    expiry: datetime
    strike: float
    option_type: OptionType

    def time_to_expiry_years(self, now: datetime | None = None) -> float:
        """Time to expiry in (act/365) years, floored at 0."""
        reference = now if now is not None else datetime.now(UTC)
        expiry = self.expiry if self.expiry.tzinfo else self.expiry.replace(tzinfo=UTC)
        if reference.tzinfo is None:
            reference = reference.replace(tzinfo=UTC)
        seconds = (expiry - reference).total_seconds()
        return max(seconds / (60 * 60 * 24 * 365), 0.0)


def parse_instrument(symbol: str) -> Instrument:
    """Parse an instrument symbol string into an `Instrument`.

    Raises `InstrumentParseError` on any malformed input.
    """
    parts = symbol.split("-")
    if len(parts) != 4:
        raise InstrumentParseError(f"Invalid instrument format: {symbol!r}")

    underlying, expiry_str, strike_str, option_type_str = parts

    month_start = 0
    for i, char in enumerate(expiry_str):
        if char.isalpha():
            month_start = i
            break
    else:
        raise InstrumentParseError(f"Invalid expiry (no month letters): {symbol!r}")

    try:
        day = int(expiry_str[:month_start])
    except ValueError as exc:
        raise InstrumentParseError(f"Invalid expiry day in {symbol!r}") from exc

    month_str = expiry_str[month_start : month_start + 3].upper()
    month = _MONTHS.get(month_str)
    if month is None:
        raise InstrumentParseError(f"Invalid month {month_str!r} in {symbol!r}")

    try:
        year = 2000 + int(expiry_str[month_start + 3 :])
    except ValueError as exc:
        raise InstrumentParseError(f"Invalid expiry year in {symbol!r}") from exc

    try:
        strike = float(strike_str)
    except ValueError as exc:
        raise InstrumentParseError(f"Invalid strike in {symbol!r}") from exc

    option_type_upper = option_type_str.upper()
    if option_type_upper not in (OptionType.CALL, OptionType.PUT):
        raise InstrumentParseError(f"Invalid option type {option_type_str!r} in {symbol!r}")

    try:
        expiry = datetime(year, month, day, tzinfo=UTC)
    except ValueError as exc:
        raise InstrumentParseError(f"Invalid expiry date in {symbol!r}") from exc

    return Instrument(
        symbol=symbol,
        underlying=underlying,
        expiry=expiry,
        strike=strike,
        option_type=OptionType(option_type_upper),
    )
