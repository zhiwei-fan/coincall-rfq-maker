from datetime import UTC, datetime

import pytest

from coincall_rfq_maker.core.adapters.schemas import OptionInstrument
from coincall_rfq_maker.marketdata.instruments import InstrumentCatalog

SYMBOL = "BTCUSD-09JUL26-56000-C"
EXPIRATION_MS = int(datetime(2026, 7, 9, 8, tzinfo=UTC).timestamp() * 1000)


def option_instrument(
    symbol: str = SYMBOL, expiration_timestamp: int = EXPIRATION_MS
) -> OptionInstrument:
    return OptionInstrument.model_validate(
        {
            "symbolName": symbol,
            "baseCurrency": "BTC",
            "strike": 56_000,
            "expirationTimestamp": expiration_timestamp,
            "isActive": True,
            "minQty": 0.01,
            "tickSize": 0.01,
        }
    )


class CountingRest:
    def __init__(self, instruments: tuple[OptionInstrument, ...] = ()) -> None:
        self.instruments = instruments
        self.calls = 0
        self.error: Exception | None = None

    async def get_option_instruments(self, base: str) -> tuple[OptionInstrument, ...]:
        assert base == "BTC"
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.instruments


@pytest.mark.asyncio
async def test_positive_expiry_cache_never_refetches() -> None:
    rest = CountingRest((option_instrument(),))
    catalog = InstrumentCatalog(rest, now_ms=lambda: 1_000_000)  # type: ignore[arg-type]

    assert await catalog.expiration_ms(SYMBOL) == EXPIRATION_MS
    assert await catalog.expiration_ms(SYMBOL) == EXPIRATION_MS
    assert rest.calls == 1


@pytest.mark.asyncio
async def test_failed_fetch_uses_the_hardcoded_cooldown_boundaries() -> None:
    now = 0.0
    rest = CountingRest()
    rest.error = RuntimeError("exchange unavailable")
    catalog = InstrumentCatalog(rest, monotonic=lambda: now)  # type: ignore[arg-type]

    assert await catalog.expiration_ms(SYMBOL) is None
    assert rest.calls == 1
    now = 29.9
    assert await catalog.expiration_ms(SYMBOL) is None
    assert rest.calls == 1
    now = 30.1
    assert await catalog.expiration_ms(SYMBOL) is None
    assert rest.calls == 2


@pytest.mark.asyncio
async def test_unknown_symbol_after_success_is_negative_cached() -> None:
    now = 0.0
    rest = CountingRest((option_instrument("BTCUSD-10JUL26-56000-C"),))
    catalog = InstrumentCatalog(rest, monotonic=lambda: now, now_ms=lambda: 1_000_000)  # type: ignore[arg-type]

    assert await catalog.expiration_ms(SYMBOL) is None
    assert rest.calls == 1
    assert await catalog.expiration_ms(SYMBOL) is None
    assert rest.calls == 1


@pytest.mark.asyncio
async def test_successful_fetch_evicts_past_expiry_entries() -> None:
    future_symbol = "BTCUSD-10JUL26-56000-C"
    other_future_symbol = "BTCUSD-11JUL26-56000-C"
    rest = CountingRest(
        (
            option_instrument(SYMBOL, 999_999),
            option_instrument(future_symbol, 1_000_001),
            option_instrument(other_future_symbol, 1_000_002),
        )
    )
    catalog = InstrumentCatalog(rest, now_ms=lambda: 1_000_000)  # type: ignore[arg-type]

    assert await catalog.expiration_ms(future_symbol) == 1_000_001
    assert catalog._expirations == {
        future_symbol: 1_000_001,
        other_future_symbol: 1_000_002,
    }


@pytest.mark.asyncio
async def test_expired_request_returns_value_and_missing_is_negative_cached() -> None:
    missing_symbol = "BTCUSD-12JUL26-56000-C"
    rest = CountingRest((option_instrument(SYMBOL, 999_999),))
    catalog = InstrumentCatalog(rest, monotonic=lambda: 0.0, now_ms=lambda: 1_000_000)  # type: ignore[arg-type]

    assert await catalog.expiration_ms(SYMBOL) == 999_999
    assert catalog._expirations == {}
    assert await catalog.expiration_ms(missing_symbol) is None
    assert rest.calls == 2
    assert await catalog.expiration_ms(missing_symbol) is None
    assert rest.calls == 2


@pytest.mark.asyncio
async def test_fetching_only_expired_entries_does_not_accumulate_them() -> None:
    rest = CountingRest(
        (
            option_instrument(SYMBOL, 999_998),
            option_instrument("BTCUSD-10JUL26-56000-C", 999_999),
        )
    )
    catalog = InstrumentCatalog(rest, now_ms=lambda: 1_000_000)  # type: ignore[arg-type]

    assert await catalog.expiration_ms(SYMBOL) == 999_998
    assert catalog._expirations == {}


@pytest.mark.asyncio
async def test_pruning_keeps_only_expiries_strictly_after_now() -> None:
    equal_symbol = "BTCUSD-10JUL26-56000-C"
    future_symbol = "BTCUSD-11JUL26-56000-C"
    rest = CountingRest()
    catalog = InstrumentCatalog(rest, now_ms=lambda: 1_000_000)  # type: ignore[arg-type]
    catalog._expirations = {
        equal_symbol: 1_000_000,
        future_symbol: 1_000_001,
    }

    assert await catalog.expiration_ms("BTCUSD-12JUL26-56000-C") is None
    assert catalog._expirations == {future_symbol: 1_000_001}
