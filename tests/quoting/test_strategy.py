from coincall_rfq_maker.domain.quote import Quote, QuoteLeg, QuoteStage
from coincall_rfq_maker.domain.rfq import Rfq, RfqLeg, RfqStatus, Side
from coincall_rfq_maker.pricing.engine import LegPrice
from coincall_rfq_maker.quoting.strategy import (
    QuoteIntent,
    QuoteLegIntent,
    build_quote_intent,
    matches,
)

INSTRUMENT = "BTCUSD-21AUG25-120000-C"
OTHER_INSTRUMENT = "BTCUSD-21AUG25-130000-P"


def make_rfq(side: Side) -> Rfq:
    return Rfq(
        request_id="rfq-1",
        status=RfqStatus.ACTIVE,
        legs=(RfqLeg(instrument_name=INSTRUMENT, side=side, quantity="1"),),
        create_time_ms=0,
        expiry_time_ms=1_000_000,
    )


def make_quote(price: float = 100.0, instruments: tuple[str, ...] = (INSTRUMENT,)) -> Quote:
    return Quote(
        request_id="rfq-1",
        stage=QuoteStage.OPEN,
        legs=tuple(QuoteLeg(instrument_name=instrument, price=price) for instrument in instruments),
        create_time_ms=0,
    )


def make_intent(price: float = 100.0, instruments: tuple[str, ...] = (INSTRUMENT,)) -> QuoteIntent:
    return QuoteIntent(
        request_id="rfq-1",
        legs=tuple(
            QuoteLegIntent(instrument_name=instrument, price=price) for instrument in instruments
        ),
    )


def test_customer_buy_is_quoted_at_our_ask() -> None:
    rfq = make_rfq(Side.BUY)
    intent = build_quote_intent(rfq, {INSTRUMENT: LegPrice(bid=10.0, ask=15.0)})
    assert intent is not None
    assert intent.legs[0].price == 15.0


def test_customer_sell_is_quoted_at_our_bid() -> None:
    rfq = make_rfq(Side.SELL)
    intent = build_quote_intent(rfq, {INSTRUMENT: LegPrice(bid=10.0, ask=15.0)})
    assert intent is not None
    assert intent.legs[0].price == 10.0


def test_missing_leg_price_returns_none() -> None:
    rfq = make_rfq(Side.BUY)
    assert build_quote_intent(rfq, {}) is None


def test_multi_leg_mixed_sides() -> None:
    rfq = Rfq(
        request_id="rfq-2",
        status=RfqStatus.ACTIVE,
        legs=(
            RfqLeg(instrument_name=INSTRUMENT, side=Side.BUY, quantity="1"),
            RfqLeg(instrument_name=OTHER_INSTRUMENT, side=Side.SELL, quantity="2"),
        ),
        create_time_ms=0,
        expiry_time_ms=1_000_000,
    )
    prices = {
        INSTRUMENT: LegPrice(bid=10.0, ask=15.0),
        OTHER_INSTRUMENT: LegPrice(bid=5.0, ask=8.0),
    }
    intent = build_quote_intent(rfq, prices)
    assert intent is not None
    by_name = {leg.instrument_name: leg.price for leg in intent.legs}
    assert by_name[INSTRUMENT] == 15.0
    assert by_name[OTHER_INSTRUMENT] == 5.0


def test_matches_when_price_is_within_tolerance() -> None:
    assert matches(make_quote(), make_intent(price=100.0 + 1e-10))


def test_does_not_match_when_price_exceeds_tolerance() -> None:
    assert not matches(make_quote(), make_intent(price=100.0 + 2e-9))


def test_does_not_match_when_leg_counts_differ() -> None:
    quote = make_quote(instruments=(INSTRUMENT, OTHER_INSTRUMENT))
    assert not matches(quote, make_intent())
