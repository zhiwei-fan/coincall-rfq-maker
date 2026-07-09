"""Real taker CLI (`rfq-taker`): the RFQ surface, including the TRADE path.

Lists option instruments, creates and cancels RFQs, views quotes received, and
— in Phase 2b — executes quotes. `execute` accepting a quote is a REAL block
trade even on beta, so it is guarded by quote re-validation, a plain-language
summary, a typed confirmation, and a HARD notional cap. The `trade` command is
the blocking primary loop (create -> watch -> confirm -> execute) that cancels
the RFQ on every non-execute exit so nothing is orphaned.

A taker cannot list its own RFQs (rfqList 404s for the taker account); the JSONL
audit log is the orphan-recovery mechanism instead.

Every networked command goes through a shared safety gate that:
  1. requires dedicated TAKER_API_KEY / TAKER_API_SECRET and NEVER falls back
     to the maker API_KEY / API_SECRET; and
  2. refuses a non-beta REST host unless the global `--allow-prod` flag is passed.
"""

import argparse
import asyncio
import logging
import math
import sys
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from pydantic import ValidationError

from coincall_rfq_maker.adapters.rest import (
    CoincallAmbiguousError,
    CoincallError,
    CoincallRestClient,
)
from coincall_rfq_maker.adapters.schemas import (
    OptionInstrument,
    QuotePayload,
    RfqCreateResult,
    quote_stage_from_wire,
)
from coincall_rfq_maker.clock import get_timestamp_ms
from coincall_rfq_maker.domain.quote import QuoteStage
from coincall_rfq_maker.settings import Settings
from coincall_rfq_maker.taker.audit import AuditLog
from coincall_rfq_maker.taker.client import TakerClient

logger = logging.getLogger(__name__)

_QUOTE_CURRENCIES = ("USDT", "USDC", "USD")
_TRADE_CAVEAT = (
    "NB: verify the side/price convention against the exchange trade record on your first fill."
)
_TRADE_PROMPT = "Enter a row number to execute, 'r' to refresh, or 'c' to cancel: "


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="rfq-taker",
        description=(
            "Coincall RFQ taker — instruments / create-rfq / quotes / cancel-rfq / execute / "
            "trade. The execute and trade commands place REAL block trades."
        ),
    )
    parser.add_argument(
        "--allow-prod",
        action="store_true",
        help="Permit running against a non-beta REST host (otherwise refused).",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    instruments = subparsers.add_parser("instruments", help="List option instruments.")
    instruments.add_argument("--base", required=True, help="Base currency, e.g. BTC.")
    instruments.add_argument(
        "--active-only", action="store_true", help="Only show active instruments."
    )

    create = subparsers.add_parser("create-rfq", help="Create an RFQ from one or more legs.")
    create.add_argument(
        "--leg",
        dest="legs",
        action="append",
        required=True,
        metavar="SYMBOL:SIDE:QTY",
        help="A leg, e.g. BTCUSD-9JUL26-56000-C:BUY:0.2 (repeat for multi-leg).",
    )

    quotes = subparsers.add_parser("quotes", help="View quotes received for an RFQ.")
    quotes.add_argument("--request-id", required=True, help="The RFQ requestId.")

    cancel = subparsers.add_parser("cancel-rfq", help="Cancel one of your RFQs.")
    cancel.add_argument("--request-id", required=True, help="The RFQ requestId.")

    execute = subparsers.add_parser(
        "execute", help="Accept a received quote — places a REAL block trade."
    )
    execute.add_argument("--request-id", required=True, help="The RFQ requestId.")
    execute.add_argument("--quote-id", required=True, help="The quoteId to accept.")
    execute.add_argument(
        "--yes",
        action="store_true",
        help="Skip the typed confirmation (the HARD notional cap still applies).",
    )

    trade = subparsers.add_parser(
        "trade",
        help="Create an RFQ, watch quotes, then confirm & execute (cancels the RFQ on exit).",
    )
    trade.add_argument(
        "--leg",
        dest="legs",
        action="append",
        required=True,
        metavar="SYMBOL:SIDE:QTY",
        help="A leg, e.g. BTCUSD-9JUL26-56000-C:BUY:0.2 (repeat for multi-leg).",
    )
    trade.add_argument(
        "--timeout",
        type=_positive_float,
        default=120.0,
        help="Seconds to wait for quotes before cancelling the RFQ (default 120).",
    )
    trade.add_argument(
        "--poll",
        type=_positive_float,
        default=3.0,
        help="Seconds between quote polls (default 3).",
    )
    trade.add_argument(
        "--yes",
        action="store_true",
        help="Skip the typed confirmation (the HARD notional cap still applies).",
    )

    return parser.parse_args(argv)


def _positive_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("must be a finite value > 0")
    return parsed


def load_settings_or_exit() -> Settings:
    try:
        return Settings()  # type: ignore[call-arg]
    except ValidationError as exc:
        details = "\n".join(
            f"- {'.'.join(str(part) for part in error['loc']) or 'settings'}: "
            f"{error['msg']} ({error['type']})"
            for error in exc.errors(include_input=False)
        )
        sys.stderr.write(
            "Configuration error: API_KEY and API_SECRET must be set (via environment "
            f"or .env) before starting rfq-taker.\n{details}\n"
        )
        raise SystemExit(1) from exc


def _build_taker_client(settings: Settings, *, allow_prod: bool) -> CoincallRestClient:
    """Return a REST client built from the TAKER creds, after the safety gates.

    Raises ``SystemExit(2)`` if taker creds are absent (never falling back to
    the maker creds) or if the REST host is non-beta without ``--allow-prod``.
    """
    creds = settings.taker_credentials()
    if creds is None:
        sys.stderr.write(
            "TAKER_API_KEY and TAKER_API_SECRET must be set (env or .env) to run the taker. "
            "It will NOT fall back to the maker API_KEY/API_SECRET.\n"
        )
        raise SystemExit(2)

    if "beta" not in settings.rest_base_url.lower() and not allow_prod:
        sys.stderr.write(
            f"Refusing to run the taker against non-beta host {settings.rest_base_url!r}. "
            "Pass --allow-prod if you really intend to trade on this host.\n"
        )
        raise SystemExit(2)

    return CoincallRestClient(
        creds.api_key, creds.api_secret.get_secret_value(), settings.rest_base_url
    )


# -- command handlers -------------------------------------------------------


async def _cmd_instruments(client: TakerClient, base: str, active_only: bool) -> None:
    instruments = await client.list_instruments(base, active_only=active_only)
    _print_instruments(instruments)


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


async def _cmd_quotes(client: TakerClient, request_id: str) -> None:
    quotes = await client.get_quotes(request_id)
    _print_quotes(quotes, _now_ms())


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


async def _run_command(args: argparse.Namespace, settings: Settings) -> None:
    rest = _build_taker_client(settings, allow_prod=args.allow_prod)
    async with rest:
        client = TakerClient(rest)
        if args.command == "instruments":
            await _cmd_instruments(client, args.base, args.active_only)
        elif args.command == "create-rfq":
            await _cmd_create_rfq(client, _build_audit(settings), args.legs)
        elif args.command == "quotes":
            await _cmd_quotes(client, args.request_id)
        elif args.command == "cancel-rfq":
            await _cmd_cancel_rfq(client, _build_audit(settings), args.request_id)
        elif args.command == "execute":
            await _cmd_execute(
                client,
                _build_audit(settings),
                settings,
                request_id=args.request_id,
                quote_id=args.quote_id,
                assume_yes=args.yes,
            )
        elif args.command == "trade":
            await _cmd_trade(
                client,
                _build_audit(settings),
                settings,
                raw_legs=args.legs,
                timeout=args.timeout,
                poll=args.poll,
                assume_yes=args.yes,
            )
        else:  # pragma: no cover - argparse enforces a valid command
            raise SystemExit(2)


# -- the safety core --------------------------------------------------------


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


def _is_terminal_quote_state(state: str) -> bool:
    stage = quote_stage_from_wire(state)
    return stage in {QuoteStage.CANCELLED, QuoteStage.FILLED, QuoteStage.EXPIRED}


def _build_audit(settings: Settings) -> AuditLog:
    return AuditLog(Path(settings.taker_audit_path).expanduser())


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


# -- rendering helpers ------------------------------------------------------


def _print_instruments(instruments: Sequence[OptionInstrument]) -> None:
    if not instruments:
        print("(no instruments)")
        return
    print(
        f"{'symbolName':<26} {'strike':>12} {'expiry':>10} "
        f"{'active':>6} {'minQty':>10} {'tickSize':>10}"
    )
    for inst in instruments:
        print(
            f"{inst.symbol_name:<26} {_num(inst.strike):>12} "
            f"{_fmt_ms_date(inst.expiration_timestamp):>10} "
            f"{('yes' if inst.is_active else 'no'):>6} "
            f"{_num(inst.min_qty):>10} {_num(inst.tick_size):>10}"
        )


def _print_quotes(quotes: Sequence[QuotePayload], now_ms: int) -> None:
    if not quotes:
        print("No quotes received yet.")
        return
    for idx, quote in enumerate(quotes, start=1):
        created = quote.create_time
        expiry = quote.expiry_time
        age = _fmt_delta(now_ms - created) if created else "-"
        ttl = _fmt_delta(expiry - now_ms) if expiry else "-"
        print(f"[{idx}] quoteId={quote.quote_id} state={quote.state} age={age} ttl={ttl}")
        for leg in quote.legs:
            side = leg.side.value if leg.side is not None else "-"
            qty = leg.quantity if leg.quantity is not None else "-"
            print(f"      {leg.instrument_name} {side} price={leg.price} qty={qty}")


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


def _num(value: float) -> str:
    return f"{value:g}"


def _now_ms() -> int:
    return int(datetime.now(UTC).timestamp() * 1000)


def _fmt_ms_date(ms: int | None) -> str:
    if not ms:
        return "-"
    return datetime.fromtimestamp(ms / 1000, tz=UTC).strftime("%Y-%m-%d")


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


def run(argv: Sequence[str] | None = None) -> None:
    """Console-script entry point (`rfq-taker`)."""
    args = parse_args(argv)
    settings = load_settings_or_exit()
    asyncio.run(_run_command(args, settings))


if __name__ == "__main__":
    run()
