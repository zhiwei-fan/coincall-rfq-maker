import pytest

from coincall_rfq_maker.domain.rfq import (
    IllegalRfqTransition,
    Rfq,
    RfqLeg,
    RfqStage,
    RfqStatus,
    Side,
)


def make_rfq(stage: RfqStage = RfqStage.RECEIVED) -> Rfq:
    return Rfq(
        request_id="rfq-1",
        status=RfqStatus.ACTIVE,
        legs=(RfqLeg(instrument_name="BTCUSD-21AUG25-120000-C", side=Side.BUY, quantity="1"),),
        create_time_ms=1_000,
        expiry_time_ms=2_000,
        stage=stage,
    )


def test_legal_transition_received_to_priced() -> None:
    rfq = make_rfq(RfqStage.RECEIVED)
    updated = rfq.with_stage(RfqStage.PRICED)
    assert updated.stage is RfqStage.PRICED
    assert rfq.stage is RfqStage.RECEIVED  # original untouched (frozen)


def test_legal_transition_priced_to_quoted() -> None:
    rfq = make_rfq(RfqStage.PRICED)
    assert rfq.with_stage(RfqStage.QUOTED).stage is RfqStage.QUOTED


def test_legal_transition_quoted_back_to_priced_on_reprice() -> None:
    rfq = make_rfq(RfqStage.QUOTED)
    assert rfq.with_stage(RfqStage.PRICED).stage is RfqStage.PRICED


@pytest.mark.parametrize("stage", [RfqStage.RECEIVED, RfqStage.PRICED, RfqStage.QUOTED])
def test_any_stage_can_go_terminal(stage: RfqStage) -> None:
    rfq = make_rfq(stage)
    assert rfq.with_stage(RfqStage.TERMINAL).stage is RfqStage.TERMINAL


def test_illegal_transition_received_to_quoted_skips_priced() -> None:
    rfq = make_rfq(RfqStage.RECEIVED)
    with pytest.raises(IllegalRfqTransition):
        rfq.with_stage(RfqStage.QUOTED)


def test_illegal_transition_out_of_terminal() -> None:
    rfq = make_rfq(RfqStage.TERMINAL)
    with pytest.raises(IllegalRfqTransition):
        rfq.with_stage(RfqStage.PRICED)


def test_with_status_terminal_forces_terminal_stage() -> None:
    rfq = make_rfq(RfqStage.QUOTED)
    updated = rfq.with_status(RfqStatus.FILLED, last_update_time_ms=5_000)
    assert updated.status is RfqStatus.FILLED
    assert updated.stage is RfqStage.TERMINAL
    assert updated.is_terminal_status


def test_with_status_non_terminal_keeps_stage() -> None:
    rfq = make_rfq(RfqStage.QUOTED)
    updated = rfq.with_status(RfqStatus.ACTIVE, last_update_time_ms=5_000)
    assert updated.stage is RfqStage.QUOTED
    assert not updated.is_terminal_status


def test_instrument_names() -> None:
    rfq = make_rfq()
    assert rfq.instrument_names() == ("BTCUSD-21AUG25-120000-C",)
