from datetime import UTC, datetime

import pytest

from coincall_rfq_maker.domain.instruments import (
    ExpiryMismatchError,
    Instrument,
    InstrumentParseError,
    OptionType,
    parse_instrument,
    resolve_instrument,
)


def test_parses_valid_call_symbol() -> None:
    instrument = parse_instrument("BTCUSD-21AUG25-120000-C")
    assert instrument.underlying == "BTCUSD"
    assert instrument.strike == 120000.0
    assert instrument.option_type is OptionType.CALL
    assert instrument.expiry_date.year == 2025
    assert instrument.expiry_date.month == 8
    assert instrument.expiry_date.day == 21


def test_parses_valid_put_symbol_lowercase_type() -> None:
    instrument = parse_instrument("ETHUSD-01JAN26-3000-p")
    assert instrument.option_type is OptionType.PUT
    assert instrument.expiry_date.year == 2026
    assert instrument.expiry_date.month == 1
    assert instrument.expiry_date.day == 1


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


def test_resolved_instrument_time_to_expiry_floors_at_zero_in_the_past() -> None:
    parsed = parse_instrument("BTCUSD-21AUG20-120000-C")  # long expired
    instrument = resolve_instrument(parsed, 1_598_000_000_000)
    assert instrument.time_to_expiry_years(datetime.now(UTC)) == 0.0


def test_resolved_instrument_time_to_expiry_positive_in_the_future() -> None:
    parsed = parse_instrument("BTCUSD-21AUG40-120000-C")
    expiration_ms = int(datetime(2040, 8, 21, 8, tzinfo=UTC).timestamp() * 1000)
    instrument = resolve_instrument(parsed, expiration_ms)
    assert instrument.time_to_expiry_years(datetime.now(UTC)) > 0.0


def test_resolve_instrument_uses_exchange_expiry_and_cross_checks_symbol_date() -> None:
    parsed = parse_instrument("BTCUSD-09JUL26-56000-C")
    resolved = resolve_instrument(parsed, 1_783_584_000_000)

    assert resolved.expiry == datetime(2026, 7, 9, 8, tzinfo=UTC)

    with pytest.raises(ExpiryMismatchError):
        resolve_instrument(parsed, 1_783_670_400_000)
    with pytest.raises(ExpiryMismatchError):
        resolve_instrument(parsed, 1_783_584_000)


def test_parse_result_cannot_represent_a_priceable_expiry() -> None:
    parsed = parse_instrument("BTCUSD-09JUL26-56000-C")

    assert not isinstance(parsed, Instrument)
    assert not hasattr(parsed, "time_to_expiry_years")
