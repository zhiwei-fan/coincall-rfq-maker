"""Real taker CLI (`rfq-taker`): the SAFE, non-trade RFQ surface.

Lists option instruments, creates and cancels RFQs, and views quotes received
and your open RFQs. Trade execution is intentionally NOT here (Phase 2b).

Every networked command goes through a shared safety gate that:
  1. requires dedicated TAKER_API_KEY / TAKER_API_SECRET and NEVER falls back
     to the maker API_KEY / API_SECRET; and
  2. refuses a non-beta REST host unless the global `--allow-prod` flag is passed.
"""

import argparse
import asyncio
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Literal

from pydantic import ValidationError

from coincall_rfq_maker.adapters.rest import CoincallRestClient
from coincall_rfq_maker.adapters.schemas import OptionInstrument, QuotePayload, RfqListSnapshot
from coincall_rfq_maker.settings import Settings
from coincall_rfq_maker.taker.client import TakerClient

_QUOTE_CURRENCIES = ("USDT", "USDC", "USD")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="rfq-taker",
        description="Coincall RFQ taker — safe RFQ surface (list, create, quotes, cancel).",
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

    rfqs = subparsers.add_parser("rfqs", help="List your RFQs.")
    rfqs.add_argument(
        "--state", choices=("OPEN", "CLOSED"), default="OPEN", help="RFQ state filter."
    )

    return parser.parse_args(argv)


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


async def _cmd_create_rfq(client: TakerClient, raw_legs: list[str]) -> None:
    bases = sorted({_base_currency(_leg_symbol(raw)) for raw in raw_legs})
    instruments: list[OptionInstrument] = []
    for base in bases:
        instruments.extend(await client.list_instruments(base))
    legs = client.preflight_legs(raw_legs, instruments)
    result = await client.create_rfq(legs)
    print(f"requestId: {result.request_id}")
    print(f"next: rfq-taker quotes --request-id {result.request_id}")


async def _cmd_quotes(client: TakerClient, request_id: str) -> None:
    quotes = await client.get_quotes(request_id)
    _print_quotes(quotes, _now_ms())


async def _cmd_cancel_rfq(client: TakerClient, request_id: str) -> None:
    await client.cancel_rfq(request_id)
    print(f"cancelled RFQ {request_id}")


async def _cmd_rfqs(client: TakerClient, state: Literal["OPEN", "CLOSED"]) -> None:
    snapshot = await client.list_rfqs(state=state)
    _print_rfqs(snapshot)


async def _run_command(args: argparse.Namespace, settings: Settings) -> None:
    rest = _build_taker_client(settings, allow_prod=args.allow_prod)
    async with rest:
        client = TakerClient(rest)
        if args.command == "instruments":
            await _cmd_instruments(client, args.base, args.active_only)
        elif args.command == "create-rfq":
            await _cmd_create_rfq(client, args.legs)
        elif args.command == "quotes":
            await _cmd_quotes(client, args.request_id)
        elif args.command == "cancel-rfq":
            await _cmd_cancel_rfq(client, args.request_id)
        elif args.command == "rfqs":
            await _cmd_rfqs(client, args.state)
        else:  # pragma: no cover - argparse enforces a valid command
            raise SystemExit(2)


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


def _print_rfqs(snapshot: RfqListSnapshot) -> None:
    if not snapshot:
        print("No RFQs.")
        return
    for idx, rfq in enumerate(snapshot, start=1):
        created = _fmt_ms_datetime(rfq.create_time)
        print(f"[{idx}] requestId={rfq.request_id} state={rfq.state} created={created}")
        for leg in rfq.legs:
            print(f"      {leg.side.value} {leg.quantity} {leg.instrument_name}")


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


def _fmt_ms_datetime(ms: int | None) -> str:
    if not ms:
        return "-"
    return datetime.fromtimestamp(ms / 1000, tz=UTC).strftime("%Y-%m-%d %H:%M:%SZ")


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
