from coincall_rfq_maker.domain.rfq import Rfq, RfqLeg, RfqStatus, Side
from coincall_rfq_maker.quoting.strategy import QuoteIntent, QuoteLegIntent
from coincall_rfq_maker.risk.gate import RiskGate

INSTRUMENT = "BTCUSD-21AUG25-120000-C"
NOW_MS = 1_000_000


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


def test_rejects_leg_qty_over_max() -> None:
    gate = make_gate(max_leg_qty=0.5)
    decision = gate.evaluate(make_rfq(quantity="1"), make_intent(), {INSTRUMENT: 1.0}, NOW_MS)
    assert not decision.approved
    assert "quantity" in decision.reason


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


def test_fail_closed_on_missing_leg_price() -> None:
    gate = make_gate()
    empty_intent = QuoteIntent(request_id="rfq-1", legs=())
    decision = gate.evaluate(make_rfq(), empty_intent, {INSTRUMENT: 1.0}, NOW_MS)
    assert not decision.approved
