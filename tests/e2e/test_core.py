import math
from pathlib import Path

import pytest

from coincall_rfq_maker.core.adapters.schemas import OptionInstrument, QuotePayload, RfqCreateResult
from coincall_rfq_maker.e2e import constants
from coincall_rfq_maker.e2e.core import (
    E2eFailure,
    maker_env,
    run_e2e,
    select_instrument,
    validate_beta_urls,
)


def instrument(
    symbol: str = "BTCUSD-1JAN30-50000-C",
    *,
    expiry: int = 100_000_000,
    strike: float = 50_000.0,
    active: bool = True,
    min_qty: float = 0.01,
) -> OptionInstrument:
    return OptionInstrument.model_validate(
        {
            "symbolName": symbol,
            "baseCurrency": "BTC",
            "strike": strike,
            "expirationTimestamp": expiry,
            "isActive": active,
            "minQty": min_qty,
            "tickSize": 1,
        }
    )


def quote(request_id: str = "r1", state: str | None = "OPEN", quote_id: str = "q1") -> QuotePayload:
    payload: dict[str, object] = {"quoteId": quote_id, "requestId": request_id, "legs": []}
    if state is not None:
        payload["state"] = state
    return QuotePayload.model_validate(payload)


class FakeClock:
    def __init__(self, *, monotonic: float = 0.0, epoch_ms: int = 0) -> None:
        self.value = monotonic
        self.wall_ms = epoch_ms
        self.sleeps: list[float] = []

    def now(self) -> float:
        return self.value

    def epoch_ms(self) -> int:
        return self.wall_ms

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.value += seconds


class FakeMaker:
    def __init__(self, *, exit_code: int | None = 0, exit_after_kill: int | None = -9) -> None:
        self.exit_code = exit_code
        self.exit_after_kill = exit_after_kill
        self.started_env: dict[str, str] | None = None
        self.interrupts = 0
        self.kills = 0
        self.waits: list[float] = []

    async def start(self, env: dict[str, str]) -> None:
        self.started_env = dict(env)

    async def interrupt(self) -> None:
        self.interrupts += 1

    async def kill(self) -> None:
        self.kills += 1

    async def wait(self, timeout_seconds: float) -> int | None:
        self.waits.append(timeout_seconds)
        if self.exit_code is None and self.kills == 0:
            return None
        return self.exit_after_kill if self.exit_code is None else self.exit_code


class FakeTaker:
    def __init__(self, quote_reads: list[tuple[QuotePayload, ...]]) -> None:
        self.instruments = (instrument(),)
        self.quote_reads = quote_reads
        self.created: list[list[dict[str, str]]] = []
        self.cancelled: list[str] = []

    async def list_instruments(self, base_currency: str) -> tuple[OptionInstrument, ...]:
        assert base_currency == "BTC"
        return self.instruments

    def preflight_legs(
        self, raw_legs: list[str], instruments: tuple[OptionInstrument, ...]
    ) -> list[dict[str, str]]:
        assert instruments == self.instruments
        symbol, side, quantity = raw_legs[0].split(":")
        return [{"instrumentName": symbol, "side": side, "qty": quantity}]

    async def create_rfq(self, legs: list[dict[str, str]]) -> RfqCreateResult:
        self.created.append(legs)
        return RfqCreateResult.model_validate({"requestId": "r1"})

    async def get_quotes(self, request_id: str) -> tuple[QuotePayload, ...]:
        assert request_id == "r1"
        return self.quote_reads.pop(0) if self.quote_reads else ()

    async def cancel_rfq(self, request_id: str) -> None:
        self.cancelled.append(request_id)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (constants.READINESS_TIMEOUT_SECONDS, 30.0),
        (constants.QUOTE_WAIT_TIMEOUT_SECONDS, 120.0),
        (constants.SHUTDOWN_TIMEOUT_SECONDS, 30.0),
        (constants.OVERALL_TIMEOUT_SECONDS, 300.0),
        (constants.POLL_INTERVAL_SECONDS, 3.0),
        (constants.MIN_INSTRUMENT_TTE_HOURS, 24.0),
    ],
)
def test_frozen_e2e_bounds(value: float, expected: float) -> None:
    assert value == expected


@pytest.mark.parametrize(
    "url",
    [
        "https://api.coincall.com",
        "https://betaapi.coincall.com.evil.com",
        "https://evil.betaapi.coincall.com",
        "https://betaapi.coincall.com@evil.com",
        "https://evil.com@betaapi.coincall.com",
        "https://betaapi.coincall.com:invalid",
        "http://betaapi.coincall.com",
    ],
)
def test_beta_gate_rejects_hostile_literals(url: str) -> None:
    with pytest.raises(E2eFailure):
        validate_beta_urls(
            maker_rest_url=url,
            maker_ws_url="wss://betaws.seizeyouralpha.com/options",
            taker_rest_url="https://betaapi.coincall.com",
        )


def test_beta_gate_requires_all_three_exact_hosts() -> None:
    validate_beta_urls(
        maker_rest_url="https://BETAAPI.COINCALL.COM/path",
        maker_ws_url="wss://betaws.seizeyouralpha.com/options",
        taker_rest_url="https://betaapi.coincall.com",
    )


def test_beta_gate_rejects_plaintext_ws_scheme() -> None:
    with pytest.raises(E2eFailure, match="must be wss"):
        validate_beta_urls(
            maker_rest_url="https://betaapi.coincall.com",
            maker_ws_url="ws://betaws.seizeyouralpha.com/options",
            taker_rest_url="https://betaapi.coincall.com",
        )


def test_beta_gate_rejects_plaintext_taker_rest_scheme() -> None:
    with pytest.raises(E2eFailure, match="taker REST URL must be https"):
        validate_beta_urls(
            maker_rest_url="https://betaapi.coincall.com",
            maker_ws_url="wss://betaws.seizeyouralpha.com/options",
            taker_rest_url="http://betaapi.coincall.com",
        )


def test_select_instrument_uses_soonest_expiry_then_nearest_median_and_wire_quantity() -> None:
    picked = select_instrument(
        (
            instrument("BTCUSD-late", expiry=200_000_000, strike=1),
            instrument("BTCUSD-z", expiry=100_000_000, strike=100, min_qty=0.001),
            instrument("BTCUSD-a", expiry=100_000_000, strike=200),
            instrument("BTCUSD-c", expiry=100_000_000, strike=300),
            instrument("BTCUSD-inactive", expiry=90_000_000, strike=1, active=False),
        ),
        now_ms=0,
    )

    assert picked.instrument.symbol_name == "BTCUSD-a"
    assert picked.quantity == "0.01"


def test_select_instrument_requires_active_finite_24_hour_option() -> None:
    with pytest.raises(E2eFailure, match="no active BTC"):
        select_instrument(
            (
                instrument(expiry=86_399_999),
                instrument(symbol="bad", expiry=100_000_000, strike=math.nan),
            ),
            now_ms=0,
        )


def test_maker_env_isolated_and_leaves_risk_knobs_unset(tmp_path: Path) -> None:
    env = maker_env({"MAX_QUOTE_NOTIONAL_USD": "9", "API_KEY": "maker"}, tmp_path)

    assert env["DRY_RUN"] == "false"
    assert env["ALLOW_NO_EXPOSURE_LIMITS"] == "true"
    assert env["CANCEL_ALL_ON_START"] == "true"
    assert env["CANCEL_ALL_ON_STOP"] == "true"
    assert env["LOG_LEVEL"] == "DEBUG"
    assert env["LOG_FILE"] == str(tmp_path / "maker.log")
    assert env["DB_PATH"] == str(tmp_path / "maker.db")
    assert env["MAX_QUOTE_NOTIONAL_USD"] == "9"


@pytest.mark.asyncio
async def test_run_uses_epoch_time_for_24_hour_instrument_floor(tmp_path: Path) -> None:
    wall_now = 1_800_000_000_000
    taker = FakeTaker([(quote(),), ()])
    taker.instruments = (
        instrument("BTCUSD-near", expiry=wall_now + 60 * 60 * 1000),
        instrument("BTCUSD-far", expiry=wall_now + 30 * 60 * 60 * 1000),
    )

    result = await run_e2e(
        taker=taker,
        maker=FakeMaker(),
        clock=FakeClock(monotonic=5.0, epoch_ms=wall_now),
        read_log=lambda: "Subscribed to x\nCreated quote q1 for RFQ r1\n",
        maker_environment={},
        run_dir=tmp_path,
    )

    assert result.passed
    assert taker.created == [[{"instrumentName": "BTCUSD-far", "side": "BUY", "qty": "0.01"}]]


@pytest.mark.asyncio
async def test_happy_path_proves_evidence_then_cleans_up(tmp_path: Path) -> None:
    taker = FakeTaker([(), (quote(),), ()])
    maker = FakeMaker()
    log = (
        f"{constants.WS_SUBSCRIBED_MARKER}('rfq',)\n{constants.QUOTE_CREATED_MARKER}q1 for RFQ r1\n"
    )

    result = await run_e2e(
        taker=taker,
        maker=maker,
        clock=FakeClock(),
        read_log=lambda: log,
        maker_environment={"LOG_FILE": str(tmp_path / "maker.log")},
        run_dir=tmp_path,
    )

    assert result.passed
    assert taker.created == [
        [{"instrumentName": "BTCUSD-1JAN30-50000-C", "side": "BUY", "qty": "0.01"}]
    ]
    assert taker.cancelled == ["r1"]
    assert maker.interrupts == 1
    assert maker.kills == 0
    assert maker.waits == [30.0]


@pytest.mark.asyncio
async def test_stateless_quote_payload_is_not_open_evidence(tmp_path: Path) -> None:
    stateless = quote(state=None)
    assert "state" not in stateless.model_fields_set

    result = await run_e2e(
        taker=FakeTaker([(stateless,)]),
        maker=FakeMaker(),
        clock=FakeClock(),
        read_log=lambda: "Subscribed to x\nCreated quote q1 for RFQ r1\n",
        maker_environment={},
        run_dir=tmp_path,
    )

    assert not result.passed
    assert "no maker-attributed OPEN quote" in result.reason


@pytest.mark.asyncio
async def test_foreign_open_quote_is_ignored_until_maker_quote_appears(tmp_path: Path) -> None:
    clock = FakeClock()
    result = await run_e2e(
        taker=FakeTaker([(quote(quote_id="q-foreign"),), (quote(quote_id="q1"),), ()]),
        maker=FakeMaker(),
        clock=clock,
        read_log=lambda: "Subscribed to x\nCreated quote q1 for RFQ r1\n",
        maker_environment={},
        run_dir=tmp_path,
    )

    assert result.passed
    assert clock.sleeps == [3.0]
    assert result.quote_id == "q1"


@pytest.mark.asyncio
async def test_quote_visible_before_created_log_marker_still_passes(tmp_path: Path) -> None:
    clock = FakeClock()

    def delayed_log() -> str:
        created = "Created quote q1 for RFQ r1\n" if clock.now() >= 3.0 else ""
        return f"Subscribed to x\n{created}"

    result = await run_e2e(
        taker=FakeTaker([(quote(),), (quote(),), ()]),
        maker=FakeMaker(),
        clock=clock,
        read_log=delayed_log,
        maker_environment={},
        run_dir=tmp_path,
    )

    assert result.passed
    assert clock.sleeps == [3.0]


@pytest.mark.asyncio
async def test_cleanup_ignores_surviving_foreign_open_quote(tmp_path: Path) -> None:
    result = await run_e2e(
        taker=FakeTaker([(quote(),), (quote(quote_id="q-foreign"),)]),
        maker=FakeMaker(),
        clock=FakeClock(),
        read_log=lambda: "Subscribed to x\nCreated quote q1 for RFQ r1\n",
        maker_environment={},
        run_dir=tmp_path,
    )

    assert result.passed
    assert result.cleanup_errors == ()


@pytest.mark.asyncio
async def test_cleanup_fails_on_surviving_maker_open_quote(tmp_path: Path) -> None:
    taker = FakeTaker([(quote(),), (quote(),)])
    result = await run_e2e(
        taker=taker,
        maker=FakeMaker(),
        clock=FakeClock(),
        read_log=lambda: f"{constants.WS_SUBSCRIBED_MARKER}x\nCreated quote q1 for RFQ r1\n",
        maker_environment={},
        run_dir=tmp_path,
    )

    assert not result.passed
    assert result.cleanup_errors == ("OPEN maker quote(s) remain for RFQ r1: q1",)


@pytest.mark.asyncio
async def test_unrelated_rfq_rejection_does_not_fail_this_run(tmp_path: Path) -> None:
    result = await run_e2e(
        taker=FakeTaker([(quote(),), ()]),
        maker=FakeMaker(),
        clock=FakeClock(),
        read_log=lambda: (
            "Subscribed to x\nNot quoting RFQ r-foreign: limit\nCreated quote q1 for RFQ r1\n"
        ),
        maker_environment={},
        run_dir=tmp_path,
    )

    assert result.passed


@pytest.mark.asyncio
async def test_correlated_rfq_rejection_fails_this_run(tmp_path: Path) -> None:
    result = await run_e2e(
        taker=FakeTaker([()]),
        maker=FakeMaker(),
        clock=FakeClock(),
        read_log=lambda: (
            "Subscribed to x\nNot quoting RFQ r1: limit\nCreated quote q1 for RFQ r1\n"
        ),
        maker_environment={},
        run_dir=tmp_path,
    )

    assert not result.passed
    assert result.reason == "maker log shows refusal to quote RFQ r1"


@pytest.mark.asyncio
async def test_terminal_cancel_and_sigkill_escalation_are_bounded(tmp_path: Path) -> None:
    class TerminalCancelTaker(FakeTaker):
        async def cancel_rfq(self, request_id: str) -> None:
            raise RuntimeError("RFQ already cancelled")

    maker = FakeMaker(exit_code=None)
    result = await run_e2e(
        taker=TerminalCancelTaker([(quote(),), ()]),
        maker=maker,
        clock=FakeClock(),
        read_log=lambda: "Subscribed to x\nCreated quote q1 for RFQ r1\n",
        maker_environment={},
        run_dir=tmp_path,
    )

    assert not result.passed
    assert maker.kills == 1
    assert maker.waits == [30.0, 30.0]
    assert result.cleanup_errors == (
        "maker required SIGKILL escalation",
        "maker exited with code -9",
    )


@pytest.mark.asyncio
async def test_nonzero_maker_exit_fails_cleanup(tmp_path: Path) -> None:
    result = await run_e2e(
        taker=FakeTaker([(quote(),), ()]),
        maker=FakeMaker(exit_code=1),
        clock=FakeClock(),
        read_log=lambda: "Subscribed to x\nCreated quote q1 for RFQ r1\n",
        maker_environment={},
        run_dir=tmp_path,
    )

    assert not result.passed
    assert result.cleanup_errors == ("maker exited with code 1",)
