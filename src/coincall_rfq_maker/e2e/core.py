"""Network-free-testable state machine for the beta live e2e harness."""

from __future__ import annotations

import math
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path
from typing import Protocol
from urllib.parse import urlparse

from coincall_rfq_maker.core.adapters.schemas import OptionInstrument, QuotePayload, RfqCreateResult
from coincall_rfq_maker.e2e.constants import (
    BETA_REST_HOST,
    BETA_WS_HOST,
    MIN_INSTRUMENT_TTE_HOURS,
    OVERALL_TIMEOUT_SECONDS,
    POLL_INTERVAL_SECONDS,
    QUOTE_CREATED_MARKER,
    QUOTE_WAIT_TIMEOUT_SECONDS,
    READINESS_TIMEOUT_SECONDS,
    RFQ_NOT_QUOTED_MARKER,
    SHUTDOWN_TIMEOUT_SECONDS,
    WS_SUBSCRIBED_MARKER,
)


class E2eFailure(RuntimeError):
    """A fail-closed harness verdict with an operator-actionable reason."""


class TakerPort(Protocol):
    async def list_instruments(self, base_currency: str) -> tuple[OptionInstrument, ...]: ...

    def preflight_legs(
        self, raw_legs: list[str], instruments: Sequence[OptionInstrument]
    ) -> list[dict[str, str]]: ...

    async def create_rfq(self, legs: list[dict[str, str]]) -> RfqCreateResult: ...

    async def get_quotes(self, request_id: str) -> tuple[QuotePayload, ...]: ...

    async def cancel_rfq(self, request_id: str) -> None: ...


class MakerProcessPort(Protocol):
    async def start(self, env: Mapping[str, str]) -> None: ...

    async def interrupt(self) -> None: ...

    async def kill(self) -> None: ...

    async def wait(self, timeout_seconds: float) -> int | None: ...


class ClockPort(Protocol):
    def now(self) -> float: ...

    def epoch_ms(self) -> int: ...

    async def sleep(self, seconds: float) -> None: ...


@dataclass(frozen=True, slots=True)
class SelectedInstrument:
    instrument: OptionInstrument
    quantity: str


@dataclass(frozen=True, slots=True)
class E2eResult:
    passed: bool
    reason: str
    request_id: str | None
    quote_id: str | None
    cleanup_errors: tuple[str, ...]
    run_dir: Path
    selected_instrument: str | None
    selected_quantity: str | None

    def report(self) -> dict[str, object]:
        data = asdict(self)
        data["run_dir"] = str(self.run_dir)
        return data

    def summary(self) -> str:
        verdict = "PASS" if self.passed else "FAIL"
        lines = [f"rfq-e2e: {verdict} — {self.reason}", f"run directory: {self.run_dir}"]
        if self.request_id:
            lines.append(f"requestId: {self.request_id}")
        if self.quote_id:
            lines.append(f"quoteId: {self.quote_id}")
        if self.selected_instrument:
            lines.append(f"instrument: {self.selected_instrument} qty={self.selected_quantity}")
        lines.extend(f"cleanup error: {error}" for error in self.cleanup_errors)
        return "\n".join(lines)


def is_exact_beta_host(url: str, expected_host: str, expected_scheme: str) -> bool:
    """Require the encrypted scheme and reject every non-exact beta authority."""
    try:
        parsed = urlparse(url)
        hostname = parsed.hostname
        _ = parsed.port
    except ValueError:
        return False
    return (
        hostname is not None
        and parsed.scheme == expected_scheme
        and parsed.username is None
        and parsed.password is None
        and hostname.lower() == expected_host
    )


def validate_beta_urls(*, maker_rest_url: str, maker_ws_url: str, taker_rest_url: str) -> None:
    if not is_exact_beta_host(maker_rest_url, BETA_REST_HOST, "https"):
        raise E2eFailure(
            f"maker REST URL must be https on exactly {BETA_REST_HOST}: {maker_rest_url!r}"
        )
    if not is_exact_beta_host(maker_ws_url, BETA_WS_HOST, "wss"):
        raise E2eFailure(f"maker WS URL must be wss on exactly {BETA_WS_HOST}: {maker_ws_url!r}")
    if not is_exact_beta_host(taker_rest_url, BETA_REST_HOST, "https"):
        raise E2eFailure(
            f"taker REST URL must be https on exactly {BETA_REST_HOST}: {taker_rest_url!r}"
        )


def select_instrument(instruments: Sequence[OptionInstrument], now_ms: int) -> SelectedInstrument:
    """Choose one active BTC option deterministically, without a spot-price dependency."""
    minimum_expiry_ms = now_ms + int(MIN_INSTRUMENT_TTE_HOURS * 60 * 60 * 1000)
    eligible = [
        item
        for item in instruments
        if item.is_active
        and item.expiration_timestamp >= minimum_expiry_ms
        and math.isfinite(item.strike)
        and math.isfinite(item.min_qty)
        and item.min_qty > 0
    ]
    if not eligible:
        raise E2eFailure("no active BTC instrument expires at least 24 hours from now")

    earliest_expiry = min(item.expiration_timestamp for item in eligible)
    same_expiry = [item for item in eligible if item.expiration_timestamp == earliest_expiry]
    strikes = sorted(item.strike for item in same_expiry)
    middle = len(strikes) // 2
    median = strikes[middle] if len(strikes) % 2 else (strikes[middle - 1] + strikes[middle]) / 2
    chosen = min(same_expiry, key=lambda item: (abs(item.strike - median), item.symbol_name))
    quantity = max(Decimal(str(chosen.min_qty)), Decimal("0.01"))
    return SelectedInstrument(chosen, _wire_decimal(quantity))


def _wire_decimal(value: Decimal) -> str:
    rendered = format(value.normalize(), "f")
    return rendered.rstrip("0").rstrip(".") if "." in rendered else rendered


def maker_env(base_env: Mapping[str, str], run_dir: Path) -> dict[str, str]:
    """Only safety/isolation overrides; risk settings intentionally remain untouched."""
    return {
        **base_env,
        "DRY_RUN": "false",
        "ALLOW_NO_EXPOSURE_LIMITS": "true",
        "CANCEL_ALL_ON_START": "true",
        "CANCEL_ALL_ON_STOP": "true",
        "LOG_LEVEL": "DEBUG",
        "LOG_FILE": str(run_dir / "maker.log"),
        "DB_PATH": str(run_dir / "maker.db"),
    }


async def run_e2e(
    *,
    taker: TakerPort,
    maker: MakerProcessPort,
    clock: ClockPort,
    read_log: Callable[[], str],
    maker_environment: Mapping[str, str],
    run_dir: Path,
) -> E2eResult:
    """Prove the quote path, then fail closed on imperfect cleanup or evidence."""
    request_id: str | None = None
    quote_id: str | None = None
    selected: SelectedInstrument | None = None
    cleanup_errors: list[str] = []
    started = False
    reason = "full beta RFQ-to-quote path proven and cleanup verified"
    passed = False
    overall_deadline = clock.now() + OVERALL_TIMEOUT_SECONDS

    try:
        instruments = await taker.list_instruments("BTC")
        selected = select_instrument(instruments, clock.epoch_ms())
        raw_leg = f"{selected.instrument.symbol_name}:BUY:{selected.quantity}"
        legs = taker.preflight_legs([raw_leg], instruments)

        await maker.start(maker_environment)
        started = True
        await _wait_for_log(
            read_log,
            WS_SUBSCRIBED_MARKER,
            clock,
            _deadline(clock, overall_deadline, READINESS_TIMEOUT_SECONDS),
            "maker did not subscribe before readiness timeout",
        )

        _require_within_overall(clock, overall_deadline)
        created = await taker.create_rfq(legs)
        request_id = created.request_id
        quote = await _wait_for_maker_quote(taker, request_id, read_log, clock, overall_deadline)
        quote_id = quote.quote_id
        passed = True
    except E2eFailure as exc:
        reason = str(exc)
    except Exception as exc:
        reason = f"unexpected {type(exc).__name__}: {exc}"
    finally:
        if request_id is not None:
            try:
                await taker.cancel_rfq(request_id)
            except Exception as exc:
                if not _is_terminal_cancel_error(exc):
                    cleanup_errors.append(f"cancel RFQ {request_id}: {type(exc).__name__}: {exc}")

        exit_code: int | None = None
        if started:
            try:
                await maker.interrupt()
                exit_code = await maker.wait(SHUTDOWN_TIMEOUT_SECONDS)
                if exit_code is None:
                    await maker.kill()
                    cleanup_errors.append("maker required SIGKILL escalation")
                    exit_code = await maker.wait(SHUTDOWN_TIMEOUT_SECONDS)
                if exit_code is None:
                    cleanup_errors.append("maker survived SIGKILL escalation")
                elif exit_code != 0:
                    cleanup_errors.append(f"maker exited with code {exit_code}")
            except Exception as exc:
                cleanup_errors.append(f"maker shutdown: {type(exc).__name__}: {exc}")

        if request_id is not None:
            try:
                remaining = await taker.get_quotes(request_id)
                maker_quote_ids = set(_created_quote_ids(read_log(), request_id))
                if quote_id is not None:
                    maker_quote_ids.add(quote_id)
                orphan_ids = sorted(
                    quote.quote_id
                    for quote in remaining
                    if quote.quote_id in maker_quote_ids
                    and _is_explicit_open_for_request(quote, request_id)
                )
                if orphan_ids:
                    cleanup_errors.append(
                        f"OPEN maker quote(s) remain for RFQ {request_id}: {', '.join(orphan_ids)}"
                    )
            except Exception as exc:
                cleanup_errors.append(
                    f"post-cleanup quote verification for {request_id}: {type(exc).__name__}: {exc}"
                )

    if cleanup_errors:
        passed = False
        reason = f"cleanup verification failed: {cleanup_errors[0]}"
    return E2eResult(
        passed=passed,
        reason=reason,
        request_id=request_id,
        quote_id=quote_id,
        cleanup_errors=tuple(cleanup_errors),
        run_dir=run_dir,
        selected_instrument=None if selected is None else selected.instrument.symbol_name,
        selected_quantity=None if selected is None else selected.quantity,
    )


async def _wait_for_log(
    read_log: Callable[[], str],
    marker: str,
    clock: ClockPort,
    deadline: float,
    failure: str,
) -> None:
    while marker not in read_log():
        if clock.now() >= deadline:
            raise E2eFailure(failure)
        await clock.sleep(min(POLL_INTERVAL_SECONDS, max(0.0, deadline - clock.now())))


async def _wait_for_maker_quote(
    taker: TakerPort,
    request_id: str,
    read_log: Callable[[], str],
    clock: ClockPort,
    overall_deadline: float,
) -> QuotePayload:
    deadline = _deadline(clock, overall_deadline, QUOTE_WAIT_TIMEOUT_SECONDS)
    while True:
        log = read_log()
        if f"{RFQ_NOT_QUOTED_MARKER}{request_id}:" in log:
            raise E2eFailure(f"maker log shows refusal to quote RFQ {request_id}")
        maker_quote_ids = set(_created_quote_ids(log, request_id))
        for quote in await taker.get_quotes(request_id):
            if quote.quote_id in maker_quote_ids and _is_explicit_open_for_request(
                quote, request_id
            ):
                return quote
        if clock.now() >= deadline:
            raise E2eFailure(
                f"no maker-attributed OPEN quote for requestId {request_id} before quote timeout"
            )
        await clock.sleep(min(POLL_INTERVAL_SECONDS, max(0.0, deadline - clock.now())))


def _created_quote_ids(log: str, request_id: str) -> tuple[str, ...]:
    pattern = re.compile(
        rf"{re.escape(QUOTE_CREATED_MARKER)}(?P<quote_id>\S+) "
        rf"for RFQ {re.escape(request_id)}(?=\s|$)"
    )
    return tuple(match.group("quote_id") for match in pattern.finditer(log))


def _is_explicit_open_for_request(quote: QuotePayload, request_id: str) -> bool:
    return (
        "state" in quote.model_fields_set
        and quote.state == "OPEN"
        and quote.request_id == request_id
    )


def _deadline(clock: ClockPort, overall_deadline: float, local_timeout: float) -> float:
    return min(overall_deadline, clock.now() + local_timeout)


def _require_within_overall(clock: ClockPort, overall_deadline: float) -> None:
    if clock.now() >= overall_deadline:
        raise E2eFailure("overall e2e deadline exceeded")


def _is_terminal_cancel_error(exc: Exception) -> bool:
    text = str(exc).lower()
    terminal_words = ("already cancelled", "cancelled", "expired", "filled", "terminal")
    return any(word in text for word in terminal_words)
