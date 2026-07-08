import pytest

from coincall_rfq_maker.domain.quote import IllegalQuoteTransition, Quote, QuoteLeg, QuoteStage


def make_quote(stage: QuoteStage = QuoteStage.PENDING_CREATE) -> Quote:
    return Quote(
        request_id="rfq-1",
        stage=stage,
        legs=(QuoteLeg(instrument_name="BTCUSD-21AUG25-120000-C", price=100.0),),
        create_time_ms=1_000,
    )


def test_pending_create_to_open() -> None:
    quote = make_quote(QuoteStage.PENDING_CREATE)
    updated = quote.with_stage(QuoteStage.OPEN)
    assert updated.stage is QuoteStage.OPEN
    assert updated.is_open


def test_pending_create_to_cancelled_on_failed_submit() -> None:
    quote = make_quote(QuoteStage.PENDING_CREATE)
    updated = quote.with_stage(QuoteStage.CANCELLED)
    assert updated.is_terminal


def test_open_to_pending_cancel_to_cancelled() -> None:
    quote = make_quote(QuoteStage.OPEN)
    pending_cancel = quote.with_stage(QuoteStage.PENDING_CANCEL)
    assert pending_cancel.stage is QuoteStage.PENDING_CANCEL
    cancelled = pending_cancel.with_stage(QuoteStage.CANCELLED)
    assert cancelled.is_terminal


def test_open_to_filled() -> None:
    quote = make_quote(QuoteStage.OPEN)
    filled = quote.with_stage(QuoteStage.FILLED)
    assert filled.is_terminal
    assert not filled.is_open


def test_pending_cancel_can_revert_to_open_if_cancel_fails() -> None:
    quote = make_quote(QuoteStage.PENDING_CANCEL)
    reverted = quote.with_stage(QuoteStage.OPEN)
    assert reverted.is_open


@pytest.mark.parametrize("stage", [QuoteStage.CANCELLED, QuoteStage.FILLED, QuoteStage.EXPIRED])
def test_terminal_states_are_final(stage: QuoteStage) -> None:
    quote = make_quote(stage)
    with pytest.raises(IllegalQuoteTransition):
        quote.with_stage(QuoteStage.OPEN)


def test_illegal_skip_pending_create_direct_pending_cancel() -> None:
    quote = make_quote(QuoteStage.PENDING_CREATE)
    with pytest.raises(IllegalQuoteTransition):
        quote.with_stage(QuoteStage.PENDING_CANCEL)
