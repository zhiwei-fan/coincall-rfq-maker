import pytest

from coincall_rfq_maker.domain.instruments import InstrumentParseError, OptionType, parse_instrument


def test_parses_valid_call_symbol() -> None:
    instrument = parse_instrument("BTCUSD-21AUG25-120000-C")
    assert instrument.underlying == "BTCUSD"
    assert instrument.strike == 120000.0
    assert instrument.option_type is OptionType.CALL
    assert instrument.expiry.year == 2025
    assert instrument.expiry.month == 8
    assert instrument.expiry.day == 21


def test_parses_valid_put_symbol_lowercase_type() -> None:
    instrument = parse_instrument("ETHUSD-01JAN26-3000-p")
    assert instrument.option_type is OptionType.PUT
    assert instrument.expiry.year == 2026
    assert instrument.expiry.month == 1
    assert instrument.expiry.day == 1


@pytest.mark.parametrize(
    "symbol",
    [
        "BTCUSD-21AUG25-120000",  # too few parts
        "BTCUSD-21AUG25-120000-C-extra",  # too many parts
        "BTCUSD-21XXX25-120000-C",  # bad month
        "BTCUSD-AAAUG25-120000-C",  # bad day
        "BTCUSD-21AUGYY-120000-C",  # bad year
        "BTCUSD-21AUG25-notanumber-C",  # bad strike
        "BTCUSD-21AUG25-120000-X",  # bad option type
        "BTCUSD-32AUG25-120000-C",  # invalid calendar date
    ],
)
def test_rejects_malformed_symbols(symbol: str) -> None:
    with pytest.raises(InstrumentParseError):
        parse_instrument(symbol)


def test_time_to_expiry_floors_at_zero_in_the_past() -> None:
    from datetime import UTC, datetime

    instrument = parse_instrument("BTCUSD-21AUG20-120000-C")  # long expired
    assert instrument.time_to_expiry_years(datetime.now(UTC)) == 0.0


def test_time_to_expiry_positive_in_the_future() -> None:
    from datetime import UTC, datetime

    instrument = parse_instrument("BTCUSD-21AUG40-120000-C")
    assert instrument.time_to_expiry_years(datetime.now(UTC)) > 0.0
