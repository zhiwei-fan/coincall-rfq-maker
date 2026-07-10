from types import TracebackType
from typing import Any

import pytest

from coincall_rfq_maker.core.adapters.rest import (
    CoincallAmbiguousError,
    CoincallApiError,
    CoincallRestClient,
)
from coincall_rfq_maker.quoting.api_accounting import ApiOutcomeBoundary
from coincall_rfq_maker.risk.gate import RiskGate


class RecordingReporter:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def record_api_failure(self) -> None:
        self.calls.append("failure")

    def record_api_success(self) -> None:
        self.calls.append("success")


class SuccessfulResponse:
    status = 200

    async def text(self) -> str:
        return '{"code":0,"data":{"rfqList":[]}}'


class SuccessfulRequest:
    async def __aenter__(self) -> SuccessfulResponse:
        return SuccessfulResponse()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        return False


class SuccessfulSession:
    def get(self, *args: Any, **kwargs: Any) -> SuccessfulRequest:
        return SuccessfulRequest()


def successful_client() -> CoincallRestClient:
    client = CoincallRestClient("key", "secret")
    client._session = SuccessfulSession()  # type: ignore[assignment]
    return client


def make_gate(**overrides: float | int) -> RiskGate:
    defaults: dict[str, float | int] = {
        "max_quote_notional_usd": 1_000_000.0,
        "max_leg_qty": 100.0,
        "min_time_to_expiry_hours": 0.000001,
        "stale_market_data_seconds": 30.0,
        "kill_switch_threshold": 5,
    }
    defaults.update(overrides)
    return RiskGate(**defaults)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_accounting_boundary_ignores_transient_failures_and_reraises() -> None:
    reporter = RecordingReporter()
    boundary = ApiOutcomeBoundary(reporter)

    async def operation() -> None:
        raise CoincallApiError(200, 10000, "Try again later")

    with pytest.raises(CoincallApiError):
        await boundary.run(operation)

    assert reporter.calls == []


@pytest.mark.asyncio
async def test_accounting_boundary_counts_persistent_failure_once_and_reraises() -> None:
    reporter = RecordingReporter()
    boundary = ApiOutcomeBoundary(reporter)

    async def operation() -> None:
        raise CoincallApiError(200, 10004, "Parameter illegal")

    with pytest.raises(CoincallApiError):
        await boundary.run(operation)

    assert reporter.calls == ["failure"]


@pytest.mark.asyncio
async def test_accounting_boundary_ignores_ambiguous_failures_and_reraises() -> None:
    reporter = RecordingReporter()
    boundary = ApiOutcomeBoundary(reporter)

    async def operation() -> None:
        raise CoincallAmbiguousError("unknown outcome")

    with pytest.raises(CoincallAmbiguousError):
        await boundary.run(operation)

    assert reporter.calls == []


@pytest.mark.asyncio
async def test_accounting_boundary_no_io_records_neither_outcome() -> None:
    reporter = RecordingReporter()
    boundary = ApiOutcomeBoundary(reporter)

    async def operation() -> str:
        return "ok"

    assert await boundary.run(operation) == "ok"
    assert reporter.calls == []


@pytest.mark.asyncio
async def test_accounting_boundary_records_success_after_real_exchange_io() -> None:
    reporter = RecordingReporter()
    boundary = ApiOutcomeBoundary(reporter)
    client = successful_client()

    async def operation() -> None:
        await client.get_rfq_list(state="OPEN")

    await boundary.run(operation)

    assert reporter.calls == ["success"]


@pytest.mark.asyncio
async def test_transient_failures_do_not_trip_kill_switch_via_boundary() -> None:
    gate = make_gate(kill_switch_threshold=5)
    boundary = ApiOutcomeBoundary(gate)

    async def operation() -> None:
        raise CoincallApiError(200, 10000, "Try again later")

    for _ in range(12):
        with pytest.raises(CoincallApiError):
            await boundary.run(operation)

    assert not gate.kill_switch_tripped


@pytest.mark.asyncio
async def test_persistent_failures_trip_kill_switch_via_boundary() -> None:
    gate = make_gate(kill_switch_threshold=5)
    boundary = ApiOutcomeBoundary(gate)

    async def operation() -> None:
        raise CoincallApiError(200, 10004, "Parameter illegal")

    for _ in range(5):
        with pytest.raises(CoincallApiError):
            await boundary.run(operation)

    assert gate.kill_switch_tripped


@pytest.mark.asyncio
async def test_success_clears_pretrip_streak_but_does_not_untrip() -> None:
    gate = make_gate(kill_switch_threshold=3)
    boundary = ApiOutcomeBoundary(gate)

    async def persistent() -> None:
        raise CoincallApiError(200, 10004, "Parameter illegal")

    client = successful_client()

    async def success() -> None:
        await client.get_rfq_list(state="OPEN")

    for _ in range(2):
        with pytest.raises(CoincallApiError):
            await boundary.run(persistent)
    await boundary.run(success)
    with pytest.raises(CoincallApiError):
        await boundary.run(persistent)
    assert not gate.kill_switch_tripped

    for _ in range(2):
        with pytest.raises(CoincallApiError):
            await boundary.run(persistent)
    assert gate.kill_switch_tripped

    await boundary.run(success)
    assert gate.kill_switch_tripped
