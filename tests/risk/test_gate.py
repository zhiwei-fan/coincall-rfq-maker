import math

import pytest

from coincall_rfq_maker.domain.rfq import Rfq, RfqLeg, RfqStatus, Side
from coincall_rfq_maker.quoting.strategy import QuoteIntent, QuoteLegIntent
from coincall_rfq_maker.risk.gate import ExposureSnapshot, NullExposureProvider, RiskGate

INSTRUMENT = "BTCUSD-21AUG25-120000-C"
NOW_MS = 1_000_000


class UnusableExposureProvider:
    def current_exposure(self) -> ExposureSnapshot:
        return ExposureSnapshot(usable=False, reason="stale snapshot")


def make_gate(**overrides: float | int) -> RiskGate:
    defaults: dict[str, float | int] = {
        "max_quote_notional_usd": 1_000_000.0,
        "max_leg_qty": 100.0,
        "min_time_to_expiry_hours": 1.0,
        "stale_market_data_seconds": 30.0,
    }
    defaults.update(overrides)
    return RiskGate(**defaults)  # type: ignore[arg-type]


def make_rfq(quantity: str = "1", expiry_time_ms: int = NOW_MS + 10 * 3600 * 1000) -> Rfq:
    return Rfq(
        request_id="rfq-1",
        status=RfqStatus.ACTIVE,
        legs=(RfqLeg(instrument_name=INSTRUMENT, side=Side.BUY, quantity=quantity),),
        create_time_ms=0,
        expiry_time_ms=expiry_time_ms,
    )


def make_intent(price: float = 100.0) -> QuoteIntent:
    return QuoteIntent(
        request_id="rfq-1", legs=(QuoteLegIntent(instrument_name=INSTRUMENT, price=price),)
    )


def test_approves_within_all_limits() -> None:
    gate = make_gate()
    decision = gate.evaluate(make_rfq(), make_intent(), {INSTRUMENT: 1.0}, NOW_MS)
    assert decision.approved
    assert decision.reason is None


def test_null_exposure_provider_preserves_approval_path() -> None:
    gate = RiskGate(
        max_quote_notional_usd=1_000_000.0,
        max_leg_qty=100.0,
        min_time_to_expiry_hours=1.0,
        stale_market_data_seconds=30.0,
        exposure_provider=NullExposureProvider(),
    )

    decision = gate.evaluate(make_rfq(), make_intent(), {INSTRUMENT: 1.0}, NOW_MS)

    assert decision.approved
    assert decision.reason is None


def test_unusable_exposure_provider_fails_closed_with_distinct_reason() -> None:
    gate = RiskGate(
        max_quote_notional_usd=1_000_000.0,
        max_leg_qty=100.0,
        min_time_to_expiry_hours=1.0,
        stale_market_data_seconds=30.0,
        exposure_provider=UnusableExposureProvider(),
    )

    decision = gate.evaluate(make_rfq(), make_intent(), {INSTRUMENT: 1.0}, NOW_MS)

    assert not decision.approved
    assert decision.reason == "exposure data unavailable: stale snapshot"


def test_rejects_leg_qty_over_max() -> None:
    gate = make_gate(max_leg_qty=0.5)
    decision = gate.evaluate(make_rfq(quantity="1"), make_intent(), {INSTRUMENT: 1.0}, NOW_MS)
    assert not decision.approved
    assert "quantity" in decision.reason


@pytest.mark.parametrize("quantity", ["nan", "inf", "-1", "0"])
def test_rejects_non_finite_or_non_positive_leg_quantity(quantity: str) -> None:
    decision = make_gate().evaluate(make_rfq(quantity), make_intent(), {INSTRUMENT: 1.0}, NOW_MS)

    assert not decision.approved
    assert decision.reason is not None and "quantity" in decision.reason


def test_approves_finite_positive_leg_quantity() -> None:
    decision = make_gate().evaluate(make_rfq("0.01"), make_intent(), {INSTRUMENT: 1.0}, NOW_MS)

    assert decision.approved


def test_rejects_notional_cancellation_by_negative_quantity() -> None:
    rfq = Rfq(
        request_id="rfq-1",
        status=RfqStatus.ACTIVE,
        legs=(
            RfqLeg(instrument_name=INSTRUMENT, side=Side.BUY, quantity="100"),
            RfqLeg(instrument_name=INSTRUMENT, side=Side.SELL, quantity="-99"),
        ),
        create_time_ms=0,
        expiry_time_ms=NOW_MS + 10 * 3600 * 1000,
    )

    decision = make_gate(max_quote_notional_usd=500.0).evaluate(
        rfq, make_intent(price=100.0), {INSTRUMENT: 1.0}, NOW_MS
    )

    assert not decision.approved
    assert decision.reason is not None and "quantity" in decision.reason


@pytest.mark.parametrize(
    ("parameter", "value"),
    [
        ("max_quote_notional_usd", float("nan")),
        ("max_quote_notional_usd", math.inf),
        ("max_quote_notional_usd", 0.0),
        ("max_quote_notional_usd", -1.0),
        ("max_leg_qty", float("nan")),
        ("max_leg_qty", math.inf),
        ("max_leg_qty", 0.0),
        ("max_leg_qty", -1.0),
        ("min_time_to_expiry_hours", math.inf),
        ("stale_market_data_seconds", -1.0),
    ],
)
def test_rejects_invalid_numeric_config(parameter: str, value: float) -> None:
    with pytest.raises(ValueError, match=parameter):
        make_gate(**{parameter: value})


@pytest.mark.parametrize("value", [0, -1, 1.0, True])
def test_rejects_invalid_kill_switch_threshold(value: object) -> None:
    with pytest.raises(ValueError, match="kill_switch_threshold"):
        make_gate(kill_switch_threshold=value)  # type: ignore[arg-type]


@pytest.mark.parametrize("price", [float("nan"), math.inf, -1.0, 0.0])
def test_rejects_non_finite_or_non_positive_intent_price(price: float) -> None:
    decision = make_gate().evaluate(make_rfq(), make_intent(price), {INSTRUMENT: 1.0}, NOW_MS)

    assert not decision.approved
    assert decision.reason is not None and "price" in decision.reason


def test_rejects_notional_over_max() -> None:
    gate = make_gate(max_quote_notional_usd=10.0)
    decision = gate.evaluate(
        make_rfq(quantity="1"), make_intent(price=100.0), {INSTRUMENT: 1.0}, NOW_MS
    )
    assert not decision.approved
    assert "notional" in decision.reason


def test_rejects_stale_market_data() -> None:
    gate = make_gate(stale_market_data_seconds=5.0)
    decision = gate.evaluate(make_rfq(), make_intent(), {INSTRUMENT: 999.0}, NOW_MS)
    assert not decision.approved
    assert "stale" in decision.reason


def test_rejects_missing_market_data_age_for_intent_leg() -> None:
    gate = make_gate()
    decision = gate.evaluate(make_rfq(), make_intent(), {}, NOW_MS)
    assert not decision.approved
    assert decision.reason == f"missing market data age for {INSTRUMENT}"


def test_rejects_below_min_time_to_expiry() -> None:
    gate = make_gate(min_time_to_expiry_hours=2.0)
    rfq = make_rfq(expiry_time_ms=NOW_MS + 60 * 1000)  # 1 minute out
    decision = gate.evaluate(rfq, make_intent(), {INSTRUMENT: 1.0}, NOW_MS)
    assert not decision.approved
    assert "expiry" in decision.reason


def test_kill_switch_trips_after_repeated_failures_and_rejects_everything() -> None:
    gate = make_gate(kill_switch_threshold=3)
    for _ in range(3):
        gate.record_api_failure()
    assert gate.kill_switch_tripped
    assert not hasattr(gate, "reset_kill_switch")
    decision = gate.evaluate(make_rfq(), make_intent(), {INSTRUMENT: 1.0}, NOW_MS)
    assert not decision.approved
    assert "kill switch" in decision.reason


def test_api_success_resets_failure_count_before_trip() -> None:
    gate = make_gate(kill_switch_threshold=3)
    gate.record_api_failure()
    gate.record_api_failure()
    gate.record_api_success()
    gate.record_api_failure()
    assert not gate.kill_switch_tripped


def test_failure_total_is_monotonic_across_success() -> None:
    gate = make_gate()

    gate.record_api_failure()
    gate.record_api_failure()
    gate.record_api_success()

    assert gate.failures_total == 2
    assert gate.consecutive_failures == 0


def test_explicit_kill_switch_trip_is_idempotent_and_preserves_streak() -> None:
    trips = 0

    def on_trip() -> None:
        nonlocal trips
        trips += 1

    gate = RiskGate(
        max_quote_notional_usd=1_000_000.0,
        max_leg_qty=100.0,
        min_time_to_expiry_hours=1.0,
        stale_market_data_seconds=30.0,
        on_trip=on_trip,
    )
    gate.record_api_failure()
    gate.record_api_failure()

    gate.trip_kill_switch("orphan cannot be cancelled")
    gate.trip_kill_switch("ignored duplicate")

    assert gate.kill_switch_tripped
    assert gate.consecutive_failures == 2
    assert trips == 1


def test_fail_closed_on_missing_leg_price() -> None:
    gate = make_gate()
    empty_intent = QuoteIntent(request_id="rfq-1", legs=())
    decision = gate.evaluate(make_rfq(), empty_intent, {INSTRUMENT: 1.0}, NOW_MS)
    assert not decision.approved


class FalseyUnusableExposureProvider(UnusableExposureProvider):
    """A provider that is falsey (e.g. caches positions, currently empty)."""

    def __len__(self) -> int:
        return 0


def test_falsey_exposure_provider_is_not_replaced_by_null_provider() -> None:
    gate = RiskGate(
        max_quote_notional_usd=1_000_000.0,
        max_leg_qty=100.0,
        min_time_to_expiry_hours=1.0,
        stale_market_data_seconds=30.0,
        exposure_provider=FalseyUnusableExposureProvider(),
    )
    decision = gate.evaluate(make_rfq(), make_intent(), {INSTRUMENT: 1.0}, NOW_MS)
    assert not decision.approved
    assert decision.reason is not None and "exposure" in decision.reason
