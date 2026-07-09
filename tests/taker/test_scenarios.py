import asyncio
from datetime import UTC, datetime
from pathlib import Path

import pytest

from coincall_rfq_maker.adapters.rest import CoincallApiError
from coincall_rfq_maker.domain.quote import QuoteStage
from coincall_rfq_maker.domain.rfq import RfqLeg, Side
from coincall_rfq_maker.events import QuoteUpdated
from coincall_rfq_maker.taker import scenarios
from coincall_rfq_maker.taker.scenarios import (
    SCENARIO_NAMES,
    ScenarioFailure,
    _check,
    _fixture_instrument,
    fake_harness,
    run_scenario,
)
from coincall_rfq_maker.testing.fake_exchange import FakeExchange


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", SCENARIO_NAMES)
async def test_taker_scenario(scenario: str, tmp_path) -> None:  # type: ignore[no-untyped-def]
    async with fake_harness(str(tmp_path / f"{scenario}.db")) as harness:
        await run_scenario(scenario, harness.exchange, harness.context)


@pytest.mark.asyncio
async def test_fake_cancel_rfq_closes_quotes_and_rejects_execution() -> None:
    events: asyncio.Queue[object] = asyncio.Queue()
    exchange = FakeExchange(events)
    request_id = await exchange.create_rfq(
        (RfqLeg(instrument_name="BTCUSD-09JUL27-100000-C", side=Side.BUY, quantity="1"),)
    )
    response = await exchange.create_quote(
        request_id,
        [{"instrumentName": "BTCUSD-09JUL27-100000-C", "price": "100.0"}],
    )
    quote_id = response["data"]["quoteId"]

    await exchange.cancel_rfq(request_id)

    open_quotes = await exchange.get_quote_list(request_id=request_id, state="OPEN")
    closed_quote = await exchange.get_quote_list(quote_id=quote_id)
    assert open_quotes["data"] == []
    assert closed_quote["data"][0]["state"] == "CANCELLED"

    queued_events = []
    while not events.empty():
        queued_events.append(events.get_nowait())
    assert any(
        isinstance(event, QuoteUpdated)
        and event.quote_id == quote_id
        and event.stage is QuoteStage.CANCELLED
        for event in queued_events
    )

    with pytest.raises(CoincallApiError) as exc_info:
        await exchange.execute_quote(quote_id)
    assert exc_info.value.code is not None
    assert exc_info.value.code != 0


def test_scenario_check_raises_failure() -> None:
    with pytest.raises(ScenarioFailure, match="broken maker"):
        _check(False, "broken maker")


def test_scenarios_do_not_use_bare_asserts() -> None:
    source = Path(scenarios.__file__).read_text()
    assert "assert " not in source


def test_fixture_instrument_uses_computed_future_expiry() -> None:
    symbol = _fixture_instrument(datetime(2026, 7, 9, tzinfo=UTC))
    assert symbol == "BTCUSD-09JUL27-100000-C"
    assert "21AUG" not in Path(scenarios.__file__).read_text()
