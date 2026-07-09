import pytest

from coincall_rfq_maker.core.adapters.schemas import OptionInstrument
from coincall_rfq_maker.taker.client import TakerClient, TakerInputError


def _instrument(symbol: str, *, is_active: bool = True, min_qty: float = 0.01) -> OptionInstrument:
    return OptionInstrument.model_validate(
        {
            "symbolName": symbol,
            "baseCurrency": "BTC",
            "strike": 56000.0,
            "expirationTimestamp": 1783584000000,
            "isActive": is_active,
            "minQty": min_qty,
            "tickSize": 1,
        }
    )


ACTIVE = _instrument("BTCUSD-9JUL26-56000-C")
INACTIVE = _instrument("BTCUSD-9JUL26-57000-C", is_active=False)
INSTRUMENTS = (ACTIVE, INACTIVE)


def _client() -> TakerClient:
    # preflight_legs does not touch the REST port, so None is fine here.
    return TakerClient(rest=None)  # type: ignore[arg-type]


def test_preflight_legs_parses_valid_legs() -> None:
    legs = _client().preflight_legs(
        ["BTCUSD-9JUL26-56000-C:buy:0.2", "BTCUSD-9JUL26-56000-C:SELL:1"], INSTRUMENTS
    )

    assert legs == [
        {"instrumentName": "BTCUSD-9JUL26-56000-C", "side": "BUY", "qty": "0.2"},
        {"instrumentName": "BTCUSD-9JUL26-56000-C", "side": "SELL", "qty": "1"},
    ]


def test_preflight_legs_rejects_unknown_symbol() -> None:
    with pytest.raises(TakerInputError, match="NOPE-1JAN27-1-C"):
        _client().preflight_legs(["NOPE-1JAN27-1-C:BUY:1"], INSTRUMENTS)


def test_preflight_legs_rejects_inactive_symbol() -> None:
    with pytest.raises(TakerInputError, match="not active"):
        _client().preflight_legs(["BTCUSD-9JUL26-57000-C:BUY:1"], INSTRUMENTS)


def test_preflight_legs_rejects_sub_min_qty() -> None:
    with pytest.raises(TakerInputError, match="below min_qty"):
        _client().preflight_legs(["BTCUSD-9JUL26-56000-C:BUY:0.001"], INSTRUMENTS)


def test_preflight_legs_rejects_non_positive_qty() -> None:
    with pytest.raises(TakerInputError, match="must be positive"):
        _client().preflight_legs(["BTCUSD-9JUL26-56000-C:BUY:0"], INSTRUMENTS)


@pytest.mark.parametrize(
    "leg",
    [
        "BTCUSD-9JUL26-56000-C:BUY",  # too few fields
        "BTCUSD-9JUL26-56000-C:BUY:0.2:extra",  # too many fields
        "BTCUSD-9JUL26-56000-C:HOLD:0.2",  # bad side
        "BTCUSD-9JUL26-56000-C:BUY:abc",  # non-numeric qty
    ],
)
def test_preflight_legs_rejects_malformed_leg_naming_it(leg: str) -> None:
    with pytest.raises(TakerInputError) as exc_info:
        _client().preflight_legs([leg], INSTRUMENTS)
    assert leg in str(exc_info.value)
