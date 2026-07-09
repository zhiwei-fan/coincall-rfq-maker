import asyncio
import json
from contextlib import suppress
from pathlib import Path
from typing import Any

from coincall_rfq_maker.core.adapters.rest import CoincallAmbiguousError, CoincallApiError
from coincall_rfq_maker.core.adapters.schemas import (
    ExecuteQuoteResult,
    OptionInstrument,
    QuotePayload,
    RfqCreateResult,
)
from coincall_rfq_maker.taker import execute
from coincall_rfq_maker.taker.audit import AuditLog
from coincall_rfq_maker.taker.client import TakerClient
from coincall_rfq_maker.taker.settings import TakerSettings

REQUEST_ID = "REQ123"
QUOTE_ID = "QID-1234"  # last 4 == "1234"
INSTRUMENT_NAME = "BTCUSD-9JUL26-56000-C"
RAW_LEGS = [f"{INSTRUMENT_NAME}:BUY:0.2"]

INSTRUMENT = OptionInstrument.model_validate(
    {
        "symbolName": INSTRUMENT_NAME,
        "baseCurrency": "BTC",
        "strike": 56000.0,
        "expirationTimestamp": 1783584000000,
        "isActive": True,
        "minQty": 0.01,
        "tickSize": 1,
    }
)


def _quote(*, quote_id: str = QUOTE_ID, request_id: str = REQUEST_ID) -> QuotePayload:
    return QuotePayload.model_validate(
        {
            "quoteId": quote_id,
            "requestId": request_id,
            "state": "OPEN",
            "createTime": 1000,
            "expiryTime": None,
            "legs": [
                {
                    "instrumentName": INSTRUMENT_NAME,
                    "side": "SELL",
                    "price": "0.05",
                    "quantity": "0.2",
                }
            ],
        }
    )


def _exec_result() -> ExecuteQuoteResult:
    return ExecuteQuoteResult.model_validate(
        {
            "blockTradeId": "BT-1",
            "requestId": REQUEST_ID,
            "quoteId": QUOTE_ID,
            "legs": [
                {
                    "instrumentName": INSTRUMENT_NAME,
                    "side": "SELL",
                    "price": "0.05",
                    "quantity": "0.2",
                    "fee": "0.001",
                }
            ],
        }
    )


class FakeRest:
    """Full trade-flow REST port: instruments, create, quotes, execute, cancel."""

    def __init__(
        self,
        *,
        quotes: tuple[QuotePayload, ...] = (),
        execute_result: ExecuteQuoteResult | None = None,
        execute_error: Exception | None = None,
        create_error: Exception | None = None,
        cancel_error: Exception | None = None,
    ) -> None:
        self._quotes = quotes
        self._execute_result = execute_result
        self._execute_error = execute_error
        self._create_error = create_error
        self._cancel_error = cancel_error
        self.created_legs: list[dict[str, str]] | None = None
        self.cancelled: list[str] = []
        self.execute_calls: list[tuple[str, str]] = []

    async def get_option_instruments(self, base_currency: str) -> tuple[OptionInstrument, ...]:
        return (INSTRUMENT,)

    async def create_rfq(self, legs: list[dict[str, str]]) -> RfqCreateResult:
        self.created_legs = legs
        if self._create_error is not None:
            raise self._create_error
        return RfqCreateResult.model_validate({"requestId": REQUEST_ID, "state": "ACTIVE"})

    async def get_quotes_received(self, request_id: str | None = None) -> tuple[QuotePayload, ...]:
        return self._quotes

    async def execute_quote(self, request_id: str, quote_id: str) -> ExecuteQuoteResult:
        self.execute_calls.append((request_id, quote_id))
        if self._execute_error is not None:
            raise self._execute_error
        assert self._execute_result is not None
        return self._execute_result

    async def cancel_rfq(self, request_id: str) -> dict[str, Any]:
        if self._cancel_error is not None:
            raise self._cancel_error
        self.cancelled.append(request_id)
        return {"code": 0, "data": {"cancelled": True}}


def _settings(**overrides: Any) -> TakerSettings:
    base = {
        "_env_file": None,
        "TAKER_API_KEY": "taker-key",
        "TAKER_API_SECRET": "taker-secret",
        "REST_BASE_URL": "https://betaapi.coincall.com",
    }
    base.update(overrides)
    return TakerSettings(**base)  # type: ignore[arg-type]


def _inputs(*items: Any):  # type: ignore[no-untyped-def]
    stream = iter(items)

    def input_fn(_prompt: str) -> str:
        value = next(stream)
        if isinstance(value, type) and issubclass(value, BaseException):
            raise value
        if isinstance(value, BaseException):
            raise value
        assert isinstance(value, str)
        return value

    return input_fn


def _read_audit(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text().splitlines() if line]


def _run_trade(
    rest: FakeRest,
    audit: AuditLog,
    *,
    input_fn: Any,
    timeout: float = 30.0,
    assume_yes: bool = False,
) -> list[str]:
    out: list[str] = []
    asyncio.run(
        execute._cmd_trade(
            TakerClient(rest),  # type: ignore[arg-type]
            audit,
            _settings(),
            raw_legs=RAW_LEGS,
            timeout=timeout,
            poll=0.0,
            assume_yes=assume_yes,
            input_fn=input_fn,
            out=out.append,
        )
    )
    return out


# -- pure helpers -----------------------------------------------------------


def test_render_quote_table_numbers_rows_and_shows_legs() -> None:
    table = execute._render_quote_table((_quote(),), now_ms=2000)
    assert "[1] quoteId=QID-1234" in table
    assert INSTRUMENT_NAME in table
    assert "SELL" in table


def test_parse_selection_maps_actions_and_indices() -> None:
    assert execute._parse_selection("", 3) == "cancel"
    assert execute._parse_selection("c", 3) == "cancel"
    assert execute._parse_selection("  C ", 3) == "cancel"
    assert execute._parse_selection("r", 3) == "refresh"
    assert execute._parse_selection("2", 3) == 1  # 1-based -> 0-based
    assert execute._parse_selection("0", 3) == "invalid"  # out of range
    assert execute._parse_selection("4", 3) == "invalid"  # out of range
    assert execute._parse_selection("x", 3) == "invalid"  # garbage never means cancel


# -- trade loop: cancel-on-exit invariants ----------------------------------


def test_successful_fill_does_not_cancel(tmp_path: Path) -> None:
    rest = FakeRest(quotes=(_quote(),), execute_result=_exec_result())
    audit = AuditLog(tmp_path / "audit.jsonl")

    out = _run_trade(rest, audit, input_fn=_inputs("1"), assume_yes=True)

    assert rest.execute_calls == [(REQUEST_ID, QUOTE_ID)]
    assert rest.cancelled == []  # a filled RFQ is consumed, never cancelled
    actions = [r["action"] for r in _read_audit(tmp_path / "audit.jsonl")]
    # the audit-first breadcrumb precedes the fill record.
    assert actions == ["create_attempt", "create_rfq", "execute_attempt", "execute"]
    assert any("REQ123" in line for line in out)


def test_explicit_cancel_cancels_rfq(tmp_path: Path) -> None:
    rest = FakeRest(quotes=(_quote(),))
    audit = AuditLog(tmp_path / "audit.jsonl")

    _run_trade(rest, audit, input_fn=_inputs("c"))

    assert rest.cancelled == [REQUEST_ID]
    actions = [r["action"] for r in _read_audit(tmp_path / "audit.jsonl")]
    assert actions == ["create_attempt", "create_rfq", "cancel_attempt", "cancel_rfq"]


def test_empty_input_cancels_rfq(tmp_path: Path) -> None:
    rest = FakeRest(quotes=(_quote(),))
    audit = AuditLog(tmp_path / "audit.jsonl")

    _run_trade(rest, audit, input_fn=_inputs(""))

    assert rest.cancelled == [REQUEST_ID]


def test_eof_cancels_rfq(tmp_path: Path) -> None:
    rest = FakeRest(quotes=(_quote(),))
    audit = AuditLog(tmp_path / "audit.jsonl")

    _run_trade(rest, audit, input_fn=_inputs(EOFError))

    assert rest.cancelled == [REQUEST_ID]


def test_keyboard_interrupt_still_cancels_rfq(tmp_path: Path) -> None:
    rest = FakeRest(quotes=(_quote(),))
    audit = AuditLog(tmp_path / "audit.jsonl")

    # A Ctrl-C mid-loop must still run the cancel and return cleanly.
    _run_trade(rest, audit, input_fn=_inputs(KeyboardInterrupt))

    assert rest.cancelled == [REQUEST_ID]
    assert rest.execute_calls == []


def test_timeout_with_no_quotes_cancels_rfq(tmp_path: Path) -> None:
    rest = FakeRest(quotes=())  # never any quotes
    audit = AuditLog(tmp_path / "audit.jsonl")

    # timeout 0 -> the wait returns immediately; input_fn is never consulted.
    out = _run_trade(rest, audit, input_fn=_inputs(), timeout=0.0)

    assert rest.cancelled == [REQUEST_ID]
    assert any("no quotes" in line for line in out)


def test_refusal_keeps_loop_alive_then_cancels(tmp_path: Path) -> None:
    rest = FakeRest(quotes=(_quote(),), execute_result=_exec_result())
    audit = AuditLog(tmp_path / "audit.jsonl")

    # Select row 1, botch the typed confirmation (refused, no execute), then cancel.
    _run_trade(rest, audit, input_fn=_inputs("1", "wrong", "c"), assume_yes=False)

    assert rest.execute_calls == []
    assert rest.cancelled == [REQUEST_ID]
    actions = [r["action"] for r in _read_audit(tmp_path / "audit.jsonl")]
    assert actions == [
        "create_attempt",
        "create_rfq",
        "execute_refused",
        "cancel_attempt",
        "cancel_rfq",
    ]


def test_ambiguous_execution_breaks_loop_and_cancels(tmp_path: Path) -> None:
    rest = FakeRest(quotes=(_quote(),), execute_error=CoincallAmbiguousError("boom"))
    audit = AuditLog(tmp_path / "audit.jsonl")

    # An ambiguous accept yields STOP, so the loop breaks WITHOUT consulting the
    # input again (no double-fill risk); the finally-clause then cancels the RFQ.
    _run_trade(rest, audit, input_fn=_inputs("1"), assume_yes=True)

    assert rest.execute_calls == [(REQUEST_ID, QUOTE_ID)]  # exactly once
    actions = [r["action"] for r in _read_audit(tmp_path / "audit.jsonl")]
    assert actions == [
        "create_attempt",
        "create_rfq",
        "execute_attempt",
        "execute_ambiguous",
        "cancel_attempt",
        "cancel_rfq",
    ]


def test_stop_outcome_cannot_re_execute_the_same_quote(tmp_path: Path) -> None:
    # Even if the operator "selects" the same row twice, a STOP outcome must
    # break the loop so the quote is NEVER accepted a second time.
    rest = FakeRest(quotes=(_quote(),), execute_error=CoincallAmbiguousError("boom"))
    audit = AuditLog(tmp_path / "audit.jsonl")

    _run_trade(rest, audit, input_fn=_inputs("1", "1"), assume_yes=True)

    assert rest.execute_calls == [(REQUEST_ID, QUOTE_ID)]  # only ONCE despite "1","1"
    assert rest.cancelled == [REQUEST_ID]


def test_execute_error_breaks_loop_and_cancels(tmp_path: Path) -> None:
    # A non-ambiguous API error from execute is caught broadly, audited as
    # execute_error, and yields STOP (loop breaks, RFQ cancelled) — no traceback.
    rest = FakeRest(
        quotes=(_quote(),),
        execute_error=CoincallApiError(200, 4001, "quote no longer open"),
    )
    audit = AuditLog(tmp_path / "audit.jsonl")

    out = _run_trade(rest, audit, input_fn=_inputs("1"), assume_yes=True)

    assert rest.execute_calls == [(REQUEST_ID, QUOTE_ID)]  # exactly once
    assert any("execute failed" in line for line in out)
    actions = [r["action"] for r in _read_audit(tmp_path / "audit.jsonl")]
    assert actions == [
        "create_attempt",
        "create_rfq",
        "execute_attempt",
        "execute_error",
        "cancel_attempt",
        "cancel_rfq",
    ]


def test_cancel_failure_audits_attempt_and_failure(tmp_path: Path) -> None:
    rest = FakeRest(quotes=(_quote(),), cancel_error=CoincallApiError(200, 1, "already gone"))
    audit = AuditLog(tmp_path / "audit.jsonl")

    out = _run_trade(rest, audit, input_fn=_inputs("c"))

    assert any("WARNING" in line and "failed to cancel" in line for line in out)
    actions = [r["action"] for r in _read_audit(tmp_path / "audit.jsonl")]
    assert actions == ["create_attempt", "create_rfq", "cancel_attempt", "cancel_failed"]
    assert "already gone" in _read_audit(tmp_path / "audit.jsonl")[-1]["error"]


def test_create_rfq_audited_before_banner_prints(tmp_path: Path) -> None:
    # AUDIT-FIRST: the create_rfq record must land BEFORE the banner print, so a
    # raised print can never lose the sole orphan-recovery record of the RFQ.
    rest = FakeRest(quotes=())  # no quotes -> immediate timeout, then cancel
    events: list[str] = []

    class SpyAudit(AuditLog):
        def record(self, action: str, payload: dict[str, Any]) -> bool:
            events.append(f"audit:{action}")
            return super().record(action, payload)

    def out(line: str) -> None:
        events.append(f"out:{line}")

    asyncio.run(
        execute._cmd_trade(
            TakerClient(rest),  # type: ignore[arg-type]
            SpyAudit(tmp_path / "audit.jsonl"),
            _settings(),
            raw_legs=RAW_LEGS,
            timeout=0.0,
            poll=0.0,
            assume_yes=True,
            input_fn=_inputs(),
            out=out,
        )
    )

    create_idx = events.index("audit:create_rfq")
    banner_idx = next(i for i, event in enumerate(events) if "RFQ created" in event)
    assert create_idx < banner_idx


def test_create_ambiguous_records_attempt_and_returns_cleanly(tmp_path: Path) -> None:
    rest = FakeRest(quotes=(), create_error=CoincallAmbiguousError("maybe created"))
    audit = AuditLog(tmp_path / "audit.jsonl")

    out = _run_trade(rest, audit, input_fn=_inputs(), timeout=0.0)

    assert rest.created_legs == [{"instrumentName": INSTRUMENT_NAME, "side": "BUY", "qty": "0.2"}]
    assert rest.cancelled == []
    assert any("create outcome ambiguous" in line for line in out)
    actions = [r["action"] for r in _read_audit(tmp_path / "audit.jsonl")]
    assert actions == ["create_attempt"]


def test_create_attempt_audit_failure_refuses_to_create(tmp_path: Path) -> None:
    class FailingCreateAttemptAudit(AuditLog):
        def record(self, action: str, payload: dict[str, Any]) -> bool:
            return action != "create_attempt"

    rest = FakeRest(quotes=())
    out = _run_trade(
        rest,
        FailingCreateAttemptAudit(tmp_path / "audit.jsonl"),
        input_fn=_inputs(),
        timeout=0.0,
    )

    assert rest.created_legs is None
    assert rest.cancelled == []
    assert any("cannot persist the RFQ create audit breadcrumb" in line for line in out)


def test_cancel_on_exit_success_audits_attempt_and_success(tmp_path: Path) -> None:
    rest = FakeRest()
    audit = AuditLog(tmp_path / "audit.jsonl")
    out: list[str] = []

    asyncio.run(execute._cancel_on_exit(TakerClient(rest), audit, REQUEST_ID, out=out.append))  # type: ignore[arg-type]

    assert rest.cancelled == [REQUEST_ID]
    assert [r["action"] for r in _read_audit(tmp_path / "audit.jsonl")] == [
        "cancel_attempt",
        "cancel_rfq",
    ]


def test_cancel_on_exit_failure_audits_attempt_and_failure(tmp_path: Path) -> None:
    rest = FakeRest(cancel_error=CoincallApiError(200, 1, "already gone"))
    audit = AuditLog(tmp_path / "audit.jsonl")
    out: list[str] = []

    asyncio.run(execute._cancel_on_exit(TakerClient(rest), audit, REQUEST_ID, out=out.append))  # type: ignore[arg-type]

    assert [r["action"] for r in _read_audit(tmp_path / "audit.jsonl")] == [
        "cancel_attempt",
        "cancel_failed",
    ]
    assert any("WARNING" in line for line in out)


def test_pending_task_cancellation_still_cancels_rfq(tmp_path: Path) -> None:
    async def scenario() -> FakeRest:
        rest = FakeRest(quotes=(_quote(),))
        audit = AuditLog(tmp_path / "audit.jsonl")

        def input_fn(_prompt: str) -> str:
            task = asyncio.current_task()
            assert task is not None
            task.cancel()
            return "c"

        task = asyncio.create_task(
            execute._cmd_trade(
                TakerClient(rest),  # type: ignore[arg-type]
                audit,
                _settings(),
                raw_legs=RAW_LEGS,
                timeout=30.0,
                poll=0.0,
                assume_yes=True,
                input_fn=input_fn,
                out=lambda _line: None,
            )
        )
        with suppress(asyncio.CancelledError):
            await task
        return rest

    rest = asyncio.run(scenario())

    assert rest.cancelled == [REQUEST_ID]
    actions = [r["action"] for r in _read_audit(tmp_path / "audit.jsonl")]
    assert actions[-2:] == ["cancel_attempt", "cancel_rfq"]
