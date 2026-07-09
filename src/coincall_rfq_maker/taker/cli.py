"""Real taker CLI (`rfq-taker`): thin command dispatch and safe-surface rendering.

Lists option instruments, creates and cancels RFQs, views quotes received, and
dispatches to the trade-execution path in ``taker.execute``.

A taker cannot list its own RFQs (rfqList 404s for the taker account); the JSONL
audit log is the orphan-recovery mechanism instead.

Every networked command goes through a shared safety gate that:
  1. requires dedicated TAKER_API_KEY / TAKER_API_SECRET and NEVER falls back
     to the maker API_KEY / API_SECRET; and
  2. refuses a non-beta REST host unless the global `--allow-prod` flag is passed.
"""

import argparse
import asyncio
import math
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

from pydantic import ValidationError

from coincall_rfq_maker.core.adapters.rest import CoincallRestClient
from coincall_rfq_maker.core.adapters.schemas import OptionInstrument, QuotePayload
from coincall_rfq_maker.settings import Settings
from coincall_rfq_maker.taker.audit import AuditLog
from coincall_rfq_maker.taker.client import TakerClient
from coincall_rfq_maker.taker.execute import (
    _cmd_cancel_rfq,
    _cmd_create_rfq,
    _cmd_execute,
    _cmd_trade,
    _fmt_delta,
    _now_ms,
)


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


async def _cmd_quotes(client: TakerClient, request_id: str) -> None:
    quotes = await client.get_quotes(request_id)
    _print_quotes(quotes, _now_ms())


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


def _build_audit(settings: Settings) -> AuditLog:
    return AuditLog(Path(settings.taker_audit_path).expanduser())


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


def _num(value: float) -> str:
    return f"{value:g}"


def _fmt_ms_date(ms: int | None) -> str:
    if not ms:
        return "-"
    return datetime.fromtimestamp(ms / 1000, tz=UTC).strftime("%Y-%m-%d")


def run(argv: Sequence[str] | None = None) -> None:
    """Console-script entry point (`rfq-taker`)."""
    args = parse_args(argv)
    settings = load_settings_or_exit()
    asyncio.run(_run_command(args, settings))


if __name__ == "__main__":
    run()
