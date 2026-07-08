from typing import Any

import pytest

from coincall_rfq_maker.domain.rfq import Rfq, RfqLeg, RfqStatus, Side
from coincall_rfq_maker.orchestration import Orchestrator

INSTRUMENT = "BTCUSD-21AUG25-120000-C"


class FakeRest:
    def __init__(self, open_request_ids: list[str]) -> None:
        self._open_request_ids = open_request_ids

    async def get_rfq_list(self, **kwargs: Any) -> dict[str, Any]:
        return {
            "code": 0,
            "data": {"rfqList": [{"requestId": rid} for rid in self._open_request_ids]},
        }


class FakeMarketData:
    def __init__(self) -> None:
        self.tracked: set[str] = set()

    def track(self, underlying: str) -> None:
        self.tracked.add(underlying)

    def untrack(self, underlying: str) -> None:
        self.tracked.discard(underlying)

    def get_price(self, underlying: str) -> float | None:
        return None

    def age_seconds(self, underlying: str) -> float:
        return 0.0


class FakeQuoteLifecycle:
    def __init__(self) -> None:
        self.cancelled_for: list[str] = []

    async def cancel_for_rfq(self, request_id: str) -> None:
        self.cancelled_for.append(request_id)


def make_rfq(request_id: str) -> Rfq:
    return Rfq(
        request_id=request_id,
        status=RfqStatus.ACTIVE,
        legs=(RfqLeg(instrument_name=INSTRUMENT, side=Side.BUY, quantity="1"),),
        create_time_ms=0,
        expiry_time_ms=1_000_000_000_000,
    )


@pytest.mark.asyncio
async def test_reconciler_marks_locally_active_rfq_terminal_when_exchange_disagrees() -> None:
    rest = FakeRest(open_request_ids=["rfq-1"])  # exchange only knows about rfq-1
    market_data = FakeMarketData()
    quotes = FakeQuoteLifecycle()
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=None,  # type: ignore[arg-type]
        risk_gate=None,  # type: ignore[arg-type]
        quote_lifecycle=quotes,  # type: ignore[arg-type]
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))
    orchestrator.rfq_store.upsert(make_rfq("rfq-2"))  # exchange has already dropped this one

    await orchestrator.reconcile_with_exchange()

    assert orchestrator.rfq_store.get("rfq-1").is_terminal_status is False  # type: ignore[union-attr]
    rfq_2 = orchestrator.rfq_store.get("rfq-2")
    assert rfq_2 is not None
    assert rfq_2.is_terminal_status
    assert rfq_2.status is RfqStatus.EXPIRED
    assert quotes.cancelled_for == ["rfq-2"]


@pytest.mark.asyncio
async def test_reconciler_converges_when_exchange_matches_local_state() -> None:
    rest = FakeRest(open_request_ids=["rfq-1"])
    market_data = FakeMarketData()
    quotes = FakeQuoteLifecycle()
    orchestrator = Orchestrator(
        rest_client=rest,  # type: ignore[arg-type]
        market_data=market_data,  # type: ignore[arg-type]
        pricing_model=None,  # type: ignore[arg-type]
        risk_gate=None,  # type: ignore[arg-type]
        quote_lifecycle=quotes,  # type: ignore[arg-type]
    )
    orchestrator.rfq_store.upsert(make_rfq("rfq-1"))

    await orchestrator.reconcile_with_exchange()

    assert quotes.cancelled_for == []
    assert orchestrator.rfq_store.get("rfq-1").is_terminal_status is False  # type: ignore[union-attr]
