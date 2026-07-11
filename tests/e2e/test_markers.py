import asyncio
import json
import logging
from datetime import UTC, datetime
from typing import Any

import pytest

import coincall_rfq_maker.ws as ws
from coincall_rfq_maker.core.adapters.schemas import CreateQuoteResult
from coincall_rfq_maker.domain.rfq import Rfq, RfqLeg, RfqStatus, Side
from coincall_rfq_maker.e2e.constants import (
    QUOTE_CREATED_MARKER,
    RFQ_NOT_QUOTED_MARKER,
    WS_SUBSCRIBED_MARKER,
)
from coincall_rfq_maker.orchestration import Orchestrator
from coincall_rfq_maker.pricing.engine import LegPrice
from coincall_rfq_maker.quoting.lifecycle import QuoteLifecycle
from coincall_rfq_maker.quoting.strategy import QuoteIntent, QuoteLegIntent
from coincall_rfq_maker.risk.gate import RiskDecision
from coincall_rfq_maker.ws import CoincallWsClient


class Reporter:
    def record_api_failure(self) -> None:
        pass

    def record_api_success(self) -> None:
        pass


class CreateRest:
    async def create_quote(self, request_id: str, legs: list[dict[str, str]]) -> CreateQuoteResult:
        return CreateQuoteResult(quote_id="q-e2e")


class MarkerInstrumentCatalog:
    async def expiration_ms(self, symbol: str) -> int:
        assert symbol == "BTCUSD-21AUG49-120000-C"
        return int(datetime(2049, 8, 21, 8, tzinfo=UTC).timestamp() * 1000)


class MarkerMarketData:
    def get_price(self, underlying: str) -> float:
        return 50_000.0

    def age_seconds(self, underlying: str) -> float:
        return 0.0


class MarkerPricing:
    def price(self, *args: object, **kwargs: object) -> LegPrice:
        return LegPrice(bid=99.0, ask=100.0)


class MarkerRiskGate:
    def evaluate(self, *args: object, **kwargs: object) -> RiskDecision:
        return RiskDecision(approved=False, reason="marker-proof rejection")


class MarkerQuoteLifecycle:
    def get_for_rfq(self, request_id: str) -> None:
        return None

    async def withdraw_for_rfq(self, request_id: str) -> None:
        assert request_id == "r-e2e"


class SubscriptionConnection:
    def __init__(self, shutdown: asyncio.Event) -> None:
        self.shutdown = shutdown
        self.sent: list[str] = []

    async def __aenter__(self) -> "SubscriptionConnection":
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    async def send(self, message: str) -> None:
        self.sent.append(message)

    async def recv(self) -> str:
        self.shutdown.set()
        return json.dumps({"dt": 999, "data": {}})


@pytest.mark.asyncio
async def test_created_quote_marker_matches_actual_lifecycle_emit(
    caplog: pytest.LogCaptureFixture,
) -> None:
    lifecycle = QuoteLifecycle(CreateRest(), dry_run=False, api_reporter=Reporter())  # type: ignore[arg-type]
    plan = QuoteIntent("r-e2e", (QuoteLegIntent("BTCUSD-1JAN30-50000-C", 1.0),))

    with caplog.at_level(logging.INFO):
        await lifecycle._create(plan)

    assert f"{QUOTE_CREATED_MARKER}q-e2e for RFQ r-e2e" in caplog.text


@pytest.mark.asyncio
async def test_rfq_not_quoted_marker_matches_actual_orchestration_emit(
    caplog: pytest.LogCaptureFixture,
) -> None:
    rfq = Rfq(
        request_id="r-e2e",
        status=RfqStatus.ACTIVE,
        legs=(RfqLeg("BTCUSD-21AUG49-120000-C", Side.BUY, "1"),),
        create_time_ms=0,
        expiry_time_ms=4_000_000_000_000,
    )
    orchestrator = Orchestrator(
        rest_client=object(),  # type: ignore[arg-type]
        market_data=MarkerMarketData(),  # type: ignore[arg-type]
        pricing_model=MarkerPricing(),
        risk_gate=MarkerRiskGate(),  # type: ignore[arg-type]
        quote_lifecycle=MarkerQuoteLifecycle(),  # type: ignore[arg-type]
        instrument_catalog=MarkerInstrumentCatalog(),  # type: ignore[arg-type]
    )
    orchestrator.rfq_store.upsert(rfq)

    with caplog.at_level(logging.WARNING):
        await orchestrator.reprice_all_active()

    assert f"{RFQ_NOT_QUOTED_MARKER}r-e2e: marker-proof rejection" in caplog.text


@pytest.mark.asyncio
async def test_ws_subscribed_marker_matches_actual_ws_emit(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    shutdown = asyncio.Event()
    connection = SubscriptionConnection(shutdown)

    async def fake_connect(*args: Any, **kwargs: Any) -> SubscriptionConnection:
        return connection

    monkeypatch.setattr(ws.websockets, "connect", fake_connect)
    client = CoincallWsClient("wss://example.test", "key", "secret", asyncio.Queue())

    with caplog.at_level(logging.INFO):
        await client._connect_and_read(shutdown)

    assert WS_SUBSCRIBED_MARKER in caplog.text
