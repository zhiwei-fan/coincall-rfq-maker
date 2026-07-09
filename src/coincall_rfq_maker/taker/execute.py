"""Trade-execution path for the taker CLI.

The execute and trade commands place REAL block trades. This module owns that
safety-critical path, including RFQ create/cancel helpers used by the blocking
trade loop.
"""

import asyncio
import math
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from enum import StrEnum

from coincall_rfq_maker.core.adapters.rest import (
    CoincallAmbiguousError,
    CoincallError,
)
from coincall_rfq_maker.core.adapters.schemas import (
    OptionInstrument,
    QuotePayload,
    RfqCreateResult,
    quote_stage_from_wire,
)
from coincall_rfq_maker.core.clock import get_timestamp_ms
from coincall_rfq_maker.domain.quote import QuoteStage
from coincall_rfq_maker.settings import Settings
from coincall_rfq_maker.taker.audit import AuditLog
from coincall_rfq_maker.taker.client import TakerClient

_QUOTE_CURRENCIES = ("USDT", "USDC", "USD")
_TRADE_CAVEAT = (
    "NB: verify the side/price convention against the exchange trade record on your first fill."
)
_TRADE_PROMPT = "Enter a row number to execute, 'r' to refresh, or 'c' to cancel: "


class ExecuteOutcome(StrEnum):
    """Terminal state of one ``_confirm_and_execute`` attempt.

    ``EXECUTED`` — a block trade filled. ``REFUSED`` — a clean, intended non-fill
    (not_found / expired / cap / unpriceable / unconfirmed); the trade loop MAY
    continue. ``STOP`` — an ambiguous or errored accept; the loop MUST break,
    because re-executing the same quote risks a DOUBLE FILL.
    """

    EXECUTED = "executed"
    REFUSED = "refused"
    STOP = "stop"


class UnpriceableQuote(Exception):
    """A quote that cannot be priced for the notional cap — the cap fails CLOSED."""


async def _preflight_legs(client: TakerClient, raw_legs: list[str]) -> list[dict[str, str]]:
    """Preflight ``raw_legs`` against live instruments."""
    bases = sorted({_base_currency(_leg_symbol(raw)) for raw in raw_legs})
    instruments: list[OptionInstrument] = []
    for base in bases:
        instruments.extend(await client.list_instruments(base))
    return client.preflight_legs(raw_legs, instruments)


async def _create_rfq_from_legs(
    client: TakerClient, raw_legs: list[str]
) -> tuple[RfqCreateResult, list[dict[str, str]]]:
    """Preflight ``raw_legs`` against live instruments and create the RFQ."""
    legs = await _preflight_legs(client, raw_legs)
    result = await client.create_rfq(legs)
    return result, legs


async def _create_rfq_audited(
    client: TakerClient,
    audit: AuditLog,
    raw_legs: list[str],
    *,
    out: Callable[[str], None],
) -> tuple[RfqCreateResult, list[dict[str, str]]] | None:
    legs = await _preflight_legs(client, raw_legs)
    if not audit.record("create_attempt", {"legs": legs}):
        out(
            "cannot persist the RFQ create audit breadcrumb — refusing to create; "
            "the audit log is the sole recovery record"
        )
        return None
    try:
        result = await client.create_rfq(legs)
    except CoincallAmbiguousError:
        out(
            "create outcome ambiguous — an RFQ MAY exist on the exchange; check it "
            "(recorded as create_attempt)"
        )
        return None
    audit.record("create_rfq", {"requestId": result.request_id, "legs": legs})
    return result, legs


async def _cmd_create_rfq(client: TakerClient, audit: AuditLog, raw_legs: list[str]) -> None:
    created = await _create_rfq_audited(client, audit, raw_legs, out=print)
    if created is None:
        return
    result, _legs = created
    print(f"requestId: {result.request_id}")
    print(f"next: rfq-taker quotes --request-id {result.request_id}")


async def _cmd_cancel_rfq(client: TakerClient, audit: AuditLog, request_id: str) -> None:
    await _cancel_on_exit(client, audit, request_id, out=print)


async def _cmd_execute(
    client: TakerClient,
    audit: AuditLog,
    settings: Settings,
    *,
    request_id: str,
    quote_id: str,
    assume_yes: bool,
) -> None:
    outcome = await _confirm_and_execute(
        client,
        audit,
        request_id=request_id,
        quote_id=quote_id,
        max_notional_usd=settings.taker_max_notional_usd,
        assume_yes=assume_yes,
    )
    # `execute` runs once; _confirm_and_execute already emitted the detail line.
    # A STOP (ambiguous/errored accept) is NOT a clean finish — exit non-zero so
    # a wrapping script never treats an uncertain outcome as done.
    if outcome is ExecuteOutcome.STOP:
        raise SystemExit(1)
    if outcome is ExecuteOutcome.REFUSED:
        raise SystemExit(3)


async def _cmd_trade(
    client: TakerClient,
    audit: AuditLog,
    settings: Settings,
    *,
    raw_legs: list[str],
    timeout: float,
    poll: float,
    assume_yes: bool,
    input_fn: Callable[[str], str] = input,
    out: Callable[[str], None] = print,
) -> None:
    """Blocking primary loop: create -> watch quotes -> confirm & execute.

    CANCELS the RFQ on every exit that leaves it un-executed (timeout, explicit
    cancel, empty/EOF input, KeyboardInterrupt). A successful fill consumes the
    RFQ, so it is NEVER cancelled after a fill.
    """
    created = await _create_rfq_audited(client, audit, raw_legs, out=out)
    if created is None:
        return
    result, _legs = created
    request_id = result.request_id

    executed = False
    try:
        out("=" * 60)
        out(f"RFQ created — requestId: {request_id}")
        out("=" * 60)
        while True:
            quotes = await _await_quotes(client, request_id, timeout=timeout, poll=poll)
            if not quotes:
                out(f"no quotes received within {timeout:g}s")
                break
            out(_render_quote_table(quotes, _now_ms()))
            selection = _parse_selection(input_fn(_TRADE_PROMPT), len(quotes))
            if selection == "cancel":
                break
            if selection == "refresh":
                continue
            if selection == "invalid":
                out("invalid selection; enter a row number, 'r' to refresh, or 'c' to cancel")
                continue
            assert isinstance(selection, int)
            outcome = await _confirm_and_execute(
                client,
                audit,
                request_id=request_id,
                quote_id=quotes[selection].quote_id,
                max_notional_usd=settings.taker_max_notional_usd,
                assume_yes=assume_yes,
                input_fn=input_fn,
                out=out,
            )
            if outcome is ExecuteOutcome.EXECUTED:
                executed = True
                break
            if outcome is ExecuteOutcome.STOP:
                # Ambiguous/errored accept: MUST NOT re-execute the same quote
                # (double-fill risk). Break; the finally-clause cancels the RFQ.
                break
            # REFUSED -> re-render quotes and let the user choose again.
    except (KeyboardInterrupt, EOFError):
        out("\ninterrupted")
    finally:
        if not executed:
            await _cancel_on_exit_shielded(client, audit, request_id, out=out)


def _is_terminal_quote_state(state: str) -> bool:
    stage = quote_stage_from_wire(state)
    return stage in {QuoteStage.CANCELLED, QuoteStage.FILLED, QuoteStage.EXPIRED}


def _gross_premium(quote: QuotePayload) -> float:
    """Sum of ``abs(price * quantity)`` across legs (taker gross premium).

    Fails CLOSED so the notional cap can never UNDER-count: raises
    ``UnpriceableQuote`` if the quote has no legs, or if any leg has an
    unparseable price or a missing/empty/unparseable quantity.

    IMPORTANT: this compares ``sum(abs(price * quantity))`` directly against a
    USD cap and assumes each leg ``price`` is USD-denominated premium per
    contract. That denomination is UNVERIFIED against the live exchange and
    must be confirmed on the first live fill.
    """
    if not quote.legs:
        raise UnpriceableQuote("quote has no legs")
    gross = 0.0
    for leg in quote.legs:
        try:
            price = float(leg.price)
        except (TypeError, ValueError):
            raise UnpriceableQuote(
                f"leg {leg.instrument_name} has an unparseable price {leg.price!r}"
            ) from None
        if not math.isfinite(price):
            raise UnpriceableQuote(
                f"leg {leg.instrument_name} has a non-finite price {leg.price!r}"
            )
        if leg.quantity is None or leg.quantity == "":
            raise UnpriceableQuote(f"leg {leg.instrument_name} has no quantity")
        try:
            quantity = float(leg.quantity)
        except (TypeError, ValueError):
            raise UnpriceableQuote(
                f"leg {leg.instrument_name} has an unparseable quantity {leg.quantity!r}"
            ) from None
        if not math.isfinite(quantity):
            raise UnpriceableQuote(
                f"leg {leg.instrument_name} has a non-finite quantity {leg.quantity!r}"
            )
        gross += abs(price * quantity)
    return gross


async def _confirm_and_execute(
    client: TakerClient,
    audit: AuditLog,
    *,
    request_id: str,
    quote_id: str,
    max_notional_usd: float,
    assume_yes: bool,
    input_fn: Callable[[str], str] = input,
    out: Callable[[str], None] = print,
) -> ExecuteOutcome:
    """Re-validate, summarise, cap-check, confirm, then execute one quote.

    Returns an :class:`ExecuteOutcome`. Every non-fill path is recorded to the
    audit log. ``execute_quote`` is non-idempotent: an ambiguous or errored
    accept yields ``STOP`` (never retried) so the caller cannot double-fill.
    """
    # (a) RE-VALIDATE: the quote must still exist and belong to this RFQ.
    quote = await client.find_received_quote(request_id, quote_id)
    if quote is None:
        out(f"quote {quote_id} not found for RFQ {request_id} (expired or wrong id)")
        audit.record(
            "execute_refused",
            {"requestId": request_id, "quoteId": quote_id, "reason": "not_found"},
        )
        return ExecuteOutcome.REFUSED
    now_ms = get_timestamp_ms()
    # NB: a no-op when the quote omits expiryTime (the fake exchange does). A
    # stale accept cannot be caught client-side without the field; the exchange
    # rejects it, and the broad CoincallError handling below audits it as execute_error.
    if quote.expiry_time is not None and quote.expiry_time < now_ms:
        out(f"quote {quote_id} for RFQ {request_id} has expired — refusing to execute")
        audit.record(
            "execute_refused",
            {"requestId": request_id, "quoteId": quote_id, "reason": "expired"},
        )
        return ExecuteOutcome.REFUSED
    if _is_terminal_quote_state(quote.state):
        out(
            f"quote {quote_id} for RFQ {request_id} is {quote.state}; "
            "refusing to execute a non-open quote"
        )
        audit.record(
            "execute_refused",
            {"requestId": request_id, "quoteId": quote_id, "reason": "not_open"},
        )
        return ExecuteOutcome.REFUSED

    # (b) SUMMARY (taker perspective, plain sentences).
    out(f"About to execute quote {quote_id} for RFQ {request_id}:")
    for leg in quote.legs:
        side = leg.side.value if leg.side is not None else "-"
        quantity = leg.quantity if leg.quantity is not None else "0"
        out(f"  {side} {quantity} {leg.instrument_name} @ {leg.price}")

    # Price the quote for the cap. Fails CLOSED: an unpriceable quote is REFUSED,
    # never under-counted against the notional cap.
    try:
        gross_premium = _gross_premium(quote)
    except UnpriceableQuote as exc:
        out(f"cannot price quote {quote_id} ({exc}) — refusing to execute")
        audit.record(
            "execute_refused",
            {"requestId": request_id, "quoteId": quote_id, "reason": "unpriceable"},
        )
        return ExecuteOutcome.REFUSED
    out(f"gross premium: {gross_premium:.4f}  (cap {max_notional_usd})")
    out(_TRADE_CAVEAT)

    # (c) HARD NOTIONAL CAP — refuses EVEN IF assume_yes is True.
    if gross_premium > max_notional_usd:
        out(
            f"REFUSED: gross premium {gross_premium:.4f} exceeds the hard notional cap "
            f"{max_notional_usd} — not executing (the cap applies even with --yes)."
        )
        audit.record(
            "execute_refused",
            {"requestId": request_id, "quoteId": quote_id, "reason": "notional_cap"},
        )
        return ExecuteOutcome.REFUSED

    # (d) CONFIRM: require the last 4 of the quoteId unless --yes.
    if not assume_yes:
        entry = input_fn(
            f"Type the last 4 of the quoteId (...{quote_id[-4:]}) to execute, "
            "or anything else to abort: "
        )
        if entry.strip() != quote_id[-4:]:
            out("aborted")
            audit.record(
                "execute_refused",
                {"requestId": request_id, "quoteId": quote_id, "reason": "unconfirmed"},
            )
            return ExecuteOutcome.REFUSED

    # (e) EXECUTE (non-idempotent — never retried). AUDIT-FIRST breadcrumb: a
    # possibly-landed fill must always leave a trace no matter what happens next
    # (crash, interrupt, malformed response, raised print).
    if not audit.record("execute_attempt", {"requestId": request_id, "quoteId": quote_id}):
        out(
            "cannot persist the pre-trade audit breadcrumb — refusing to execute; "
            "the audit log is the sole recovery record"
        )
        return ExecuteOutcome.REFUSED
    try:
        executed = await client.execute_quote(request_id, quote_id)
    except CoincallAmbiguousError:
        out(
            "EXECUTION OUTCOME AMBIGUOUS — the accept may or may not have landed. "
            "Check the exchange (block trades / positions) BEFORE any retry."
        )
        audit.record("execute_ambiguous", {"requestId": request_id, "quoteId": quote_id})
        return ExecuteOutcome.STOP
    except CoincallError as exc:
        out(
            f"execute failed: {exc}. The accept MAY have landed — check the exchange "
            f"for a fill on quote {quote_id} before retrying."
        )
        audit.record(
            "execute_error",
            {"requestId": request_id, "quoteId": quote_id, "error": str(exc)},
        )
        return ExecuteOutcome.STOP

    # SUCCESS — a block trade FILLED. Audit the fill BEFORE any print/format so a
    # raised out() can never lose the fill record.
    audit.record(
        "execute",
        {
            "requestId": request_id,
            "quoteId": quote_id,
            "blockTradeId": executed.block_trade_id,
            "legs": [
                {
                    "instrumentName": fill.instrument_name,
                    "side": fill.side.value if fill.side is not None else None,
                    "price": fill.price,
                    "quantity": fill.quantity,
                    "fee": fill.fee,
                }
                for fill in executed.legs
            ],
        },
    )
    out(f"FILLED — blockTradeId {executed.block_trade_id}")
    for fill in executed.legs:
        side = fill.side.value if fill.side is not None else "-"
        out(
            f"  {fill.instrument_name} {side} price={fill.price} qty={fill.quantity} fee={fill.fee}"
        )
    return ExecuteOutcome.EXECUTED


async def _await_quotes(
    client: TakerClient, request_id: str, *, timeout: float, poll: float
) -> tuple[QuotePayload, ...]:
    """Poll received quotes every ``poll`` seconds until >=1 arrives or timeout."""
    deadline_ms = _now_ms() + int(timeout * 1000)
    while True:
        quotes = await client.get_quotes(request_id)
        if quotes:
            return quotes
        if _now_ms() >= deadline_ms:
            return ()
        await asyncio.sleep(poll)


def _parse_selection(entry: str, num_quotes: int) -> int | str:
    """Map a trade-loop prompt entry to a 0-based index or an action token.

    Returns the 0-based row index, or one of ``"cancel"`` (empty/``c``),
    ``"refresh"`` (``r``), or ``"invalid"`` (anything else, including an
    out-of-range row) — an unrecognised entry NEVER means cancel.
    """
    token = entry.strip().lower()
    if token in ("", "c"):
        return "cancel"
    if token == "r":
        return "refresh"
    try:
        row = int(token)
    except ValueError:
        return "invalid"
    if 1 <= row <= num_quotes:
        return row - 1
    return "invalid"


async def _cancel_on_exit(
    client: TakerClient, audit: AuditLog, request_id: str, *, out: Callable[[str], None]
) -> None:
    """Best-effort cancel of an un-executed RFQ so it is never left orphaned."""
    audit.record("cancel_attempt", {"requestId": request_id})
    try:
        await client.cancel_rfq(request_id)
    except CoincallError as exc:
        audit.record("cancel_failed", {"requestId": request_id, "error": str(exc)})
        out(f"WARNING: failed to cancel RFQ {request_id}: {exc}")
        return
    audit.record("cancel_rfq", {"requestId": request_id})
    out(f"cancelled RFQ {request_id}")


async def _cancel_on_exit_shielded(
    client: TakerClient, audit: AuditLog, request_id: str, *, out: Callable[[str], None]
) -> None:
    """Run cancel to completion even if this task already has a pending cancellation."""
    task = asyncio.ensure_future(_cancel_on_exit(client, audit, request_id, out=out))
    was_cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            was_cancelled = True
            if task.done():
                break
            continue
    await task
    if was_cancelled:
        raise asyncio.CancelledError


def _render_quote_table(quotes: Sequence[QuotePayload], now_ms: int) -> str:
    """Render received quotes as a numbered table for the trade loop (pure)."""
    lines: list[str] = []
    for idx, quote in enumerate(quotes, start=1):
        created = quote.create_time
        expiry = quote.expiry_time
        age = _fmt_delta(now_ms - created) if created else "-"
        ttl = _fmt_delta(expiry - now_ms) if expiry else "-"
        lines.append(f"[{idx}] quoteId={quote.quote_id} age={age} ttl={ttl}")
        for leg in quote.legs:
            side = leg.side.value if leg.side is not None else "-"
            qty = leg.quantity if leg.quantity is not None else "-"
            lines.append(f"      {leg.instrument_name} {side} price={leg.price} qty={qty}")
    return "\n".join(lines)


def _leg_symbol(raw_leg: str) -> str:
    return raw_leg.split(":", 1)[0].strip()


def _base_currency(symbol: str) -> str:
    head = symbol.split("-", 1)[0].strip().upper()
    for quote in _QUOTE_CURRENCIES:
        if head.endswith(quote) and len(head) > len(quote):
            return head[: -len(quote)]
    return head


def _now_ms() -> int:
    return int(datetime.now(UTC).timestamp() * 1000)


def _fmt_delta(ms: int) -> str:
    negative = ms < 0
    total = abs(ms) // 1000
    if total < 60:
        text = f"{total}s"
    elif total < 3600:
        text = f"{total // 60}m{total % 60:02d}s"
    else:
        hours, remainder = divmod(total, 3600)
        text = f"{hours}h{remainder // 60:02d}m"
    return f"-{text}" if negative else text
