import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from coincall_rfq_maker.core.adapters.rest import (
    CoincallAmbiguousError,
    CoincallApiError,
    CoincallRestClient,
)
from coincall_rfq_maker.core.adapters.schemas import (
    ExecuteQuoteResult,
    QuotePayload,
)
from coincall_rfq_maker.settings import Settings
from coincall_rfq_maker.taker import execute
from coincall_rfq_maker.taker.audit import AuditLog
from coincall_rfq_maker.taker.client import TakerClient

REQUEST_ID = "r1"
QUOTE_ID = "QID-1234"  # last 4 == "1234"
INSTRUMENT_NAME = "BTCUSD-9JUL26-56000-C"


def _quote(
    *,
    quote_id: str = QUOTE_ID,
    request_id: str | None = REQUEST_ID,
    state: str = "OPEN",
    expiry_time: int | None = None,
    legs: list[dict[str, Any]] | None = None,
) -> QuotePayload:
    return QuotePayload.model_validate(
        {
            "quoteId": quote_id,
            "requestId": request_id,
            "state": state,
            "createTime": 1000,
            "expiryTime": expiry_time,
            # distinguish an explicit [] (no legs) from the default single leg.
            "legs": legs
            if legs is not None
            else [
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
    """Minimal REST port for the execute path (find_received_quote + execute)."""

    def __init__(
        self,
        *,
        quotes: tuple[QuotePayload, ...] = (),
        execute_result: ExecuteQuoteResult | None = None,
        execute_error: Exception | None = None,
    ) -> None:
        self._quotes = quotes
        self._execute_result = execute_result
        self._execute_error = execute_error
        self.execute_calls: list[tuple[str, str]] = []

    async def get_quotes_received(self, request_id: str | None = None) -> tuple[QuotePayload, ...]:
        return self._quotes

    async def execute_quote(self, request_id: str, quote_id: str) -> ExecuteQuoteResult:
        self.execute_calls.append((request_id, quote_id))
        if self._execute_error is not None:
            raise self._execute_error
        assert self._execute_result is not None
        return self._execute_result


def _read_audit(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text().splitlines() if line]


def _run(
    rest: FakeRest,
    audit: AuditLog,
    *,
    assume_yes: bool = False,
    entry: str | None = None,
    max_notional: float = 5000.0,
) -> tuple[execute.ExecuteOutcome, list[str]]:
    out: list[str] = []
    inputs = iter([entry] if entry is not None else [])

    def input_fn(_prompt: str) -> str:
        return next(inputs)

    result = asyncio.run(
        execute._confirm_and_execute(
            TakerClient(rest),  # type: ignore[arg-type]
            audit,
            request_id=REQUEST_ID,
            quote_id=QUOTE_ID,
            max_notional_usd=max_notional,
            assume_yes=assume_yes,
            input_fn=input_fn,
            out=out.append,
        )
    )
    return result, out


# -- (a) re-validate --------------------------------------------------------


def test_refuses_when_quote_not_found(tmp_path: Path) -> None:
    audit = AuditLog(tmp_path / "audit.jsonl")
    rest = FakeRest(quotes=())  # nothing to find

    result, out = _run(rest, audit, assume_yes=True)

    assert result is execute.ExecuteOutcome.REFUSED
    assert rest.execute_calls == []
    assert any("not found" in line for line in out)
    records = _read_audit(tmp_path / "audit.jsonl")
    assert records[-1]["action"] == "execute_refused"
    assert records[-1]["reason"] == "not_found"


def test_refuses_when_request_id_mismatches(tmp_path: Path) -> None:
    audit = AuditLog(tmp_path / "audit.jsonl")
    # A quote with the right quoteId but a DIFFERENT requestId must not match.
    rest = FakeRest(quotes=(_quote(request_id="OTHER"),))

    result, _out = _run(rest, audit, assume_yes=True)

    assert result is execute.ExecuteOutcome.REFUSED
    assert rest.execute_calls == []
    assert _read_audit(tmp_path / "audit.jsonl")[-1]["reason"] == "not_found"


def test_refuses_when_quote_expired(tmp_path: Path) -> None:
    audit = AuditLog(tmp_path / "audit.jsonl")
    rest = FakeRest(quotes=(_quote(expiry_time=1),))  # 1 ms after epoch == long past

    result, out = _run(rest, audit, assume_yes=True)

    assert result is execute.ExecuteOutcome.REFUSED
    assert rest.execute_calls == []
    assert any("expired" in line for line in out)
    assert _read_audit(tmp_path / "audit.jsonl")[-1]["reason"] == "expired"


def test_refuses_terminal_quote_state_without_executing(tmp_path: Path) -> None:
    audit = AuditLog(tmp_path / "audit.jsonl")
    rest = FakeRest(quotes=(_quote(state="FILLED"),), execute_result=_exec_result())

    result, out = _run(rest, audit, assume_yes=True)

    assert result is execute.ExecuteOutcome.REFUSED
    assert rest.execute_calls == []
    assert any("non-open quote" in line for line in out)
    assert _read_audit(tmp_path / "audit.jsonl")[-1]["reason"] == "not_open"


def test_open_quote_state_still_reaches_execute_path(tmp_path: Path) -> None:
    audit = AuditLog(tmp_path / "audit.jsonl")
    rest = FakeRest(quotes=(_quote(state="OPEN"),), execute_result=_exec_result())

    result, _out = _run(rest, audit, assume_yes=True)

    assert result is execute.ExecuteOutcome.EXECUTED
    assert rest.execute_calls == [(REQUEST_ID, QUOTE_ID)]


# -- (c) hard notional cap --------------------------------------------------


def test_notional_cap_refuses_even_with_assume_yes(tmp_path: Path) -> None:
    audit = AuditLog(tmp_path / "audit.jsonl")
    rest = FakeRest(quotes=(_quote(),), execute_result=_exec_result())

    # gross premium == abs(0.05 * 0.2) == 0.01, cap 0.001 -> breach, even with --yes.
    result, out = _run(rest, audit, assume_yes=True, max_notional=0.001)

    assert result is execute.ExecuteOutcome.REFUSED
    assert rest.execute_calls == []  # never reached the exchange
    refusal = "\n".join(out)
    assert "0.001" in refusal and "0.0100" in refusal  # names BOTH numbers
    records = _read_audit(tmp_path / "audit.jsonl")
    assert records[-1]["action"] == "execute_refused"
    assert records[-1]["reason"] == "notional_cap"


# -- (d) confirmation -------------------------------------------------------


def test_wrong_confirmation_aborts_without_executing(tmp_path: Path) -> None:
    audit = AuditLog(tmp_path / "audit.jsonl")
    rest = FakeRest(quotes=(_quote(),), execute_result=_exec_result())

    result, out = _run(rest, audit, assume_yes=False, entry="9999")

    assert result is execute.ExecuteOutcome.REFUSED
    assert rest.execute_calls == []
    assert any("aborted" in line for line in out)
    assert _read_audit(tmp_path / "audit.jsonl")[-1]["reason"] == "unconfirmed"


def test_correct_last4_executes(tmp_path: Path) -> None:
    audit = AuditLog(tmp_path / "audit.jsonl")
    rest = FakeRest(quotes=(_quote(),), execute_result=_exec_result())

    result, out = _run(rest, audit, assume_yes=False, entry=" 1234 ")  # trimmed match

    assert result is execute.ExecuteOutcome.EXECUTED
    assert rest.execute_calls == [(REQUEST_ID, QUOTE_ID)]
    assert any("FILLED" in line and "BT-1" in line for line in out)
    records = _read_audit(tmp_path / "audit.jsonl")
    # the fill is preceded by the audit-first breadcrumb.
    assert [r["action"] for r in records[-2:]] == ["execute_attempt", "execute"]
    assert records[-1]["blockTradeId"] == "BT-1"
    assert records[-1]["legs"][0]["instrumentName"] == INSTRUMENT_NAME


def test_assume_yes_skips_confirmation_and_executes(tmp_path: Path) -> None:
    audit = AuditLog(tmp_path / "audit.jsonl")
    rest = FakeRest(quotes=(_quote(),), execute_result=_exec_result())

    result, _out = _run(rest, audit, assume_yes=True)  # no input consumed

    assert result is execute.ExecuteOutcome.EXECUTED
    assert rest.execute_calls == [(REQUEST_ID, QUOTE_ID)]


# -- (e) ambiguity: never retry ---------------------------------------------


def test_ambiguous_outcome_is_not_retried(tmp_path: Path) -> None:
    audit = AuditLog(tmp_path / "audit.jsonl")
    rest = FakeRest(quotes=(_quote(),), execute_error=CoincallAmbiguousError("boom"))

    result, out = _run(rest, audit, assume_yes=True)

    assert result is execute.ExecuteOutcome.STOP
    assert rest.execute_calls == [(REQUEST_ID, QUOTE_ID)]  # exactly once, NO retry
    assert any("AMBIGUOUS" in line for line in out)
    records = _read_audit(tmp_path / "audit.jsonl")
    # the breadcrumb is written BEFORE the (ambiguous) accept, so a landed fill
    # can still be reconciled from the log.
    assert [r["action"] for r in records[-2:]] == ["execute_attempt", "execute_ambiguous"]


def test_execute_error_stops_cleanly_and_audits(tmp_path: Path) -> None:
    # A stale-quote (or any) CoincallApiError from execute must NOT crash: it is
    # caught broadly, reported, audited as execute_error, and yields STOP.
    audit = AuditLog(tmp_path / "audit.jsonl")
    rest = FakeRest(
        quotes=(_quote(),),
        execute_error=CoincallApiError(200, 4001, "quote no longer open"),
    )

    result, out = _run(rest, audit, assume_yes=True)

    assert result is execute.ExecuteOutcome.STOP
    assert rest.execute_calls == [(REQUEST_ID, QUOTE_ID)]
    assert any("execute failed" in line and "MAY have landed" in line for line in out)
    records = _read_audit(tmp_path / "audit.jsonl")
    # audit-first breadcrumb precedes the error record — no silent fill loss.
    assert [r["action"] for r in records[-2:]] == ["execute_attempt", "execute_error"]
    assert "quote no longer open" in records[-1]["error"]


def test_malformed_success_still_audits_the_fill(tmp_path: Path) -> None:
    # execute_quote returned a best-effort result (code==0 but no blockTradeId):
    # the fill must STILL be audited with the UNKNOWN sentinel, no crash.
    audit = AuditLog(tmp_path / "audit.jsonl")
    salvaged = ExecuteQuoteResult.model_construct(block_trade_id="UNKNOWN", legs=[])
    rest = FakeRest(quotes=(_quote(),), execute_result=salvaged)

    result, out = _run(rest, audit, assume_yes=True)

    assert result is execute.ExecuteOutcome.EXECUTED
    assert any("FILLED" in line and "UNKNOWN" in line for line in out)
    records = _read_audit(tmp_path / "audit.jsonl")
    assert [r["action"] for r in records[-2:]] == ["execute_attempt", "execute"]
    assert records[-1]["blockTradeId"] == "UNKNOWN"


def test_execute_attempt_audit_failure_refuses_before_exchange(tmp_path: Path) -> None:
    class FailingAttemptAudit(AuditLog):
        def record(self, action: str, payload: dict[str, Any]) -> bool:
            return action != "execute_attempt"

    rest = FakeRest(quotes=(_quote(),), execute_result=_exec_result())
    result, out = _run(rest, FailingAttemptAudit(tmp_path / "audit.jsonl"), assume_yes=True)

    assert result is execute.ExecuteOutcome.REFUSED
    assert rest.execute_calls == []
    assert any("cannot persist the pre-trade audit breadcrumb" in line for line in out)


def test_post_fill_execute_audit_failure_does_not_undo_fill(tmp_path: Path) -> None:
    class FailingFillAudit(AuditLog):
        def record(self, action: str, payload: dict[str, Any]) -> bool:
            return action != "execute"

    rest = FakeRest(quotes=(_quote(),), execute_result=_exec_result())
    result, out = _run(rest, FailingFillAudit(tmp_path / "audit.jsonl"), assume_yes=True)

    assert result is execute.ExecuteOutcome.EXECUTED
    assert rest.execute_calls == [(REQUEST_ID, QUOTE_ID)]
    assert any("FILLED" in line for line in out)


def test_malformed_response_ambiguous_error_stops_and_audits(tmp_path: Path) -> None:
    audit = AuditLog(tmp_path / "audit.jsonl")
    rest = FakeRest(
        quotes=(_quote(),),
        execute_error=CoincallAmbiguousError("malformed HTTP-200 response body"),
    )

    result, _out = _run(rest, audit, assume_yes=True)

    assert result is execute.ExecuteOutcome.STOP
    assert rest.execute_calls == [(REQUEST_ID, QUOTE_ID)]
    actions = [r["action"] for r in _read_audit(tmp_path / "audit.jsonl")]
    assert actions[-2:] == ["execute_attempt", "execute_ambiguous"]


# -- gross premium helper: the cap fails CLOSED -----------------------------


def test_gross_premium_multi_leg() -> None:
    quote = _quote(
        legs=[
            {"instrumentName": "A", "side": "SELL", "price": "0.05", "quantity": "0.2"},
            {"instrumentName": "B", "side": "BUY", "price": "100", "quantity": "2"},
        ]
    )
    assert execute._gross_premium(quote) == pytest.approx(0.01 + 200.0)


def test_gross_premium_refuses_missing_quantity() -> None:
    quote = _quote(legs=[{"instrumentName": "A", "side": "SELL", "price": "0.05"}])
    with pytest.raises(execute.UnpriceableQuote, match="quantity"):
        execute._gross_premium(quote)


def test_gross_premium_refuses_empty_quantity() -> None:
    quote = _quote(legs=[{"instrumentName": "A", "side": "SELL", "price": "0.05", "quantity": ""}])
    with pytest.raises(execute.UnpriceableQuote, match="quantity"):
        execute._gross_premium(quote)


def test_gross_premium_refuses_unparseable_price() -> None:
    quote = _quote(
        legs=[
            {"instrumentName": "A", "side": "SELL", "price": "", "quantity": "5"},
            {"instrumentName": "B", "side": "BUY", "price": "2", "quantity": "3"},
        ]
    )
    with pytest.raises(execute.UnpriceableQuote, match="price"):
        execute._gross_premium(quote)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("price", "nan"),
        ("quantity", "nan"),
        ("price", "inf"),
    ],
)
def test_gross_premium_refuses_non_finite_numbers(field: str, value: str) -> None:
    leg = {"instrumentName": "A", "side": "SELL", "price": "0.05", "quantity": "5"}
    leg[field] = value
    quote = _quote(legs=[leg])

    with pytest.raises(execute.UnpriceableQuote, match="non-finite"):
        execute._gross_premium(quote)


def test_gross_premium_refuses_no_legs() -> None:
    quote = _quote(legs=[])
    with pytest.raises(execute.UnpriceableQuote, match="no legs"):
        execute._gross_premium(quote)


@pytest.mark.parametrize(
    "legs",
    [
        [{"instrumentName": "A", "side": "SELL", "price": "bogus", "quantity": "5"}],
        [{"instrumentName": "A", "side": "SELL", "price": "0.05"}],  # missing qty
        [{"instrumentName": "A", "side": "SELL", "price": "0.05", "quantity": ""}],
        [{"instrumentName": "A", "side": "SELL", "price": "nan", "quantity": "5"}],
        [{"instrumentName": "A", "side": "SELL", "price": "0.05", "quantity": "nan"}],
        [{"instrumentName": "A", "side": "SELL", "price": "inf", "quantity": "5"}],
        [],  # no legs at all
    ],
)
def test_unpriceable_quote_refuses_even_with_yes(
    tmp_path: Path, legs: list[dict[str, Any]]
) -> None:
    # The cap fails CLOSED: an unpriceable quote is refused BEFORE the exchange
    # is touched, even with --yes.
    audit = AuditLog(tmp_path / "audit.jsonl")
    rest = FakeRest(quotes=(_quote(legs=legs),), execute_result=_exec_result())

    result, out = _run(rest, audit, assume_yes=True)

    assert result is execute.ExecuteOutcome.REFUSED
    assert rest.execute_calls == []  # never priced -> never executed
    assert any("cannot price quote" in line for line in out)
    assert _read_audit(tmp_path / "audit.jsonl")[-1]["reason"] == "unpriceable"


# -- TakerClient additions --------------------------------------------------


def test_find_received_quote_matches_both_ids() -> None:
    rest = FakeRest(quotes=(_quote(),))
    found = asyncio.run(TakerClient(rest).find_received_quote(REQUEST_ID, QUOTE_ID))  # type: ignore[arg-type]
    assert found is not None and found.quote_id == QUOTE_ID


def test_find_received_quote_none_on_quote_id_mismatch() -> None:
    rest = FakeRest(quotes=(_quote(quote_id="OTHER"),))
    assert asyncio.run(TakerClient(rest).find_received_quote(REQUEST_ID, QUOTE_ID)) is None  # type: ignore[arg-type]


def test_find_received_quote_tolerates_absent_request_id() -> None:
    # The live getQuotesReceived response may omit requestId per quote; a quote
    # with the right quoteId and NO requestId must still match.
    rest = FakeRest(quotes=(_quote(request_id=None),))
    found = asyncio.run(TakerClient(rest).find_received_quote(REQUEST_ID, QUOTE_ID))  # type: ignore[arg-type]
    assert found is not None and found.quote_id == QUOTE_ID


def test_find_received_quote_still_rejects_mismatched_request_id() -> None:
    # A PRESENT-but-wrong requestId is still rejected (not None-tolerant).
    rest = FakeRest(quotes=(_quote(request_id="OTHER"),))
    assert asyncio.run(TakerClient(rest).find_received_quote(REQUEST_ID, QUOTE_ID)) is None  # type: ignore[arg-type]


def test_execute_quote_delegates_to_rest() -> None:
    rest = FakeRest(execute_result=_exec_result())
    result = asyncio.run(TakerClient(rest).execute_quote(REQUEST_ID, QUOTE_ID))  # type: ignore[arg-type]
    assert result.block_trade_id == "BT-1"
    assert rest.execute_calls == [(REQUEST_ID, QUOTE_ID)]


# -- execute command wiring -------------------------------------------------


def _settings(**overrides: Any) -> Settings:
    base = {
        "_env_file": None,
        "API_KEY": "maker-key",
        "API_SECRET": "maker-secret",
        "REST_BASE_URL": "https://betaapi.coincall.com",
        "TAKER_MAX_NOTIONAL_USD": 4242.0,
    }
    base.update(overrides)
    return Settings(**base)  # type: ignore[arg-type]


def test_cmd_execute_passes_cap_and_yes_through(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: dict[str, Any] = {}

    async def fake_confirm(client: Any, audit: Any, **kwargs: Any) -> execute.ExecuteOutcome:
        seen.update(kwargs)
        return execute.ExecuteOutcome.EXECUTED

    monkeypatch.setattr(execute, "_confirm_and_execute", fake_confirm)
    audit = AuditLog(tmp_path / "audit.jsonl")

    asyncio.run(
        execute._cmd_execute(
            TakerClient(FakeRest()),  # type: ignore[arg-type]
            audit,
            _settings(),
            request_id=REQUEST_ID,
            quote_id=QUOTE_ID,
            assume_yes=True,
        )
    )

    assert seen["request_id"] == REQUEST_ID
    assert seen["quote_id"] == QUOTE_ID
    assert seen["max_notional_usd"] == 4242.0
    assert seen["assume_yes"] is True


def test_cmd_execute_exits_nonzero_on_stop(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_confirm(client: Any, audit: Any, **kwargs: Any) -> execute.ExecuteOutcome:
        return execute.ExecuteOutcome.STOP

    monkeypatch.setattr(execute, "_confirm_and_execute", fake_confirm)

    with pytest.raises(SystemExit) as exc_info:
        asyncio.run(
            execute._cmd_execute(
                TakerClient(FakeRest()),  # type: ignore[arg-type]
                AuditLog(tmp_path / "audit.jsonl"),
                _settings(),
                request_id=REQUEST_ID,
                quote_id=QUOTE_ID,
                assume_yes=True,
            )
        )
    assert exc_info.value.code == 1


@pytest.mark.parametrize(
    ("outcome", "expected_code"),
    [
        (execute.ExecuteOutcome.REFUSED, 3),
        (execute.ExecuteOutcome.STOP, 1),
    ],
)
def test_cmd_execute_exit_codes_for_non_executed_outcomes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    outcome: execute.ExecuteOutcome,
    expected_code: int,
) -> None:
    async def fake_confirm(client: Any, audit: Any, **kwargs: Any) -> execute.ExecuteOutcome:
        return outcome

    monkeypatch.setattr(execute, "_confirm_and_execute", fake_confirm)

    with pytest.raises(SystemExit) as exc_info:
        asyncio.run(
            execute._cmd_execute(
                TakerClient(FakeRest()),  # type: ignore[arg-type]
                AuditLog(tmp_path / "audit.jsonl"),
                _settings(),
                request_id=REQUEST_ID,
                quote_id=QUOTE_ID,
                assume_yes=True,
            )
        )
    assert exc_info.value.code == expected_code


def test_cmd_execute_returns_normally_when_executed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def fake_confirm(client: Any, audit: Any, **kwargs: Any) -> execute.ExecuteOutcome:
        return execute.ExecuteOutcome.EXECUTED

    monkeypatch.setattr(execute, "_confirm_and_execute", fake_confirm)

    asyncio.run(
        execute._cmd_execute(
            TakerClient(FakeRest()),  # type: ignore[arg-type]
            AuditLog(tmp_path / "audit.jsonl"),
            _settings(),
            request_id=REQUEST_ID,
            quote_id=QUOTE_ID,
            assume_yes=True,
        )
    )


# -- rest.execute_quote: a code==0 accept NEVER discards the fill -----------


def test_rest_execute_quote_salvages_fill_when_data_lacks_block_trade_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = CoincallRestClient("key", "secret")

    async def fake_request(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        # code==0 => the accept SUCCEEDED, but the data omits blockTradeId.
        return {"code": 0, "data": {"role": "TAKER"}}

    monkeypatch.setattr(client, "_request", fake_request)

    result = asyncio.run(client.execute_quote(REQUEST_ID, QUOTE_ID))

    assert result.block_trade_id == "UNKNOWN"  # sentinel — the fill is NOT lost
    assert result.legs == []
    assert result.request_id == REQUEST_ID
    assert result.quote_id == QUOTE_ID


def test_rest_execute_quote_salvages_present_id_when_legs_malformed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = CoincallRestClient("key", "secret")

    async def fake_request(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        # blockTradeId is present but legs is malformed -> salvage the real id.
        return {"code": 0, "data": {"blockTradeId": "BT-9", "legs": "not-a-list"}}

    monkeypatch.setattr(client, "_request", fake_request)

    result = asyncio.run(client.execute_quote(REQUEST_ID, QUOTE_ID))

    assert result.block_trade_id == "BT-9"
