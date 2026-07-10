from datetime import UTC, datetime

import pytest

from coincall_rfq_maker.core.adapters.schemas import OptionInstrument
from coincall_rfq_maker.marketdata.instruments import InstrumentCatalog

SYMBOL = "BTCUSD-09JUL26-56000-C"
EXPIRATION_MS = int(datetime(2026, 7, 9, 8, tzinfo=UTC).timestamp() * 1000)


def option_instrument(symbol: str = SYMBOL) -> OptionInstrument:
    return OptionInstrument.model_validate(
        {
            "symbolName": symbol,
            "baseCurrency": "BTC",
            "strike": 56_000,
            "expirationTimestamp": EXPIRATION_MS,
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
    catalog = InstrumentCatalog(rest)  # type: ignore[arg-type]

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
    catalog = InstrumentCatalog(rest, monotonic=lambda: now)  # type: ignore[arg-type]

    assert await catalog.expiration_ms(SYMBOL) is None
    assert rest.calls == 1
    assert await catalog.expiration_ms(SYMBOL) is None
    assert rest.calls == 1
