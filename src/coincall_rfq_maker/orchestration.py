"""Orchestration glue: RFQ store + instrument tracking, and the dispatcher
that wires WS/market-data events to the RFQ, quote, and trade consumers.

Deviation from the spec's package layout: the Data flow section describes an
"RFQ service (owns RFQ store + instrument tracking, prunes orphaned
instruments)" but the layout lists no module for it. Bolting it onto the
pure `domain/rfq.py` state machine would give it I/O side effects; cramming
it into `cli.py` would blow past a readable file size. It lives here
instead, as the single-writer owner of RFQ state and the glue between
pricing, risk, and the quote lifecycle actor.

Event handling is deliberately serial: a slow REST call delays other queued
events for its duration. That is the accepted tradeoff at this quoting cadence
because it makes interleaving races structurally impossible.
"""

import logging
import time
from typing import Any

from pydantic import ValidationError

from coincall_rfq_maker.adapters.rest import CoincallError, CoincallRestClient
from coincall_rfq_maker.adapters.schemas import RfqPayload, rfq_from_payload
from coincall_rfq_maker.domain.instruments import Instrument, InstrumentParseError, parse_instrument
from coincall_rfq_maker.domain.quote import QuoteStage
from coincall_rfq_maker.domain.rfq import Rfq, RfqStage, RfqStatus
from coincall_rfq_maker.events import (
    PricesRefreshed,
    QuoteUpdated,
    ReconcileTick,
    RepriceTick,
    RfqReceived,
    RfqTerminated,
    TradeExecuted,
)
from coincall_rfq_maker.marketdata.service import MarketDataService
from coincall_rfq_maker.persistence.store import PersistenceStore
from coincall_rfq_maker.pricing.engine import PricingModel
from coincall_rfq_maker.quoting.lifecycle import QuoteLifecycle, quote_items
from coincall_rfq_maker.quoting.strategy import build_quote_intent
from coincall_rfq_maker.risk.gate import RiskGate

logger = logging.getLogger(__name__)

DispatchEvent = (
    RfqReceived
    | RfqTerminated
    | QuoteUpdated
    | TradeExecuted
    | PricesRefreshed
    | RepriceTick
    | ReconcileTick
)


class RfqStore:
    """In-memory RFQ repository + instrument tracking.

    Single-writer: only `Orchestrator` (running on the dispatcher task)
    mutates this.
    """

    def __init__(self) -> None:
        self._rfqs: dict[str, Rfq] = {}
        self._instrument_holders: dict[str, set[str]] = {}

    def upsert(self, rfq: Rfq) -> None:
        self._rfqs[rfq.request_id] = rfq
        for name in rfq.instrument_names():
            self._instrument_holders.setdefault(name, set()).add(rfq.request_id)

    def get(self, request_id: str) -> Rfq | None:
        return self._rfqs.get(request_id)

    def active(self) -> list[Rfq]:
        return [rfq for rfq in self._rfqs.values() if not rfq.is_terminal_status]

    def terminal(self) -> list[Rfq]:
        return [rfq for rfq in self._rfqs.values() if rfq.is_terminal_status]

    def active_for_underlying(self, underlying: str) -> list[Rfq]:
        matches = []
        for rfq in self.active():
            for name in rfq.instrument_names():
                try:
                    if parse_instrument(name).underlying == underlying:
                        matches.append(rfq)
                        break
                except InstrumentParseError:
                    continue
        return matches

    def mark_terminal(self, request_id: str, status: RfqStatus, now_ms: int) -> Rfq | None:
        rfq = self._rfqs.get(request_id)
        if rfq is None:
            return None
        updated = rfq.with_status(status, now_ms)
        self._rfqs[request_id] = updated
        return updated

    def orphaned_instruments(self, request_id: str) -> set[str]:
        """Instruments referenced ONLY by `request_id` (call after marking it terminal)."""
        rfq = self._rfqs.get(request_id)
        if rfq is None:
            return set()
        orphaned = set()
        for name in rfq.instrument_names():
            holders = self._instrument_holders.get(name)
            if holders is None:
                continue
            holders.discard(request_id)
            if holders:
                self._instrument_holders[name] = holders
            else:
                del self._instrument_holders[name]
                orphaned.add(name)
        return orphaned

    def evict(self, request_id: str) -> None:
        rfq = self._rfqs.pop(request_id, None)
        if rfq is None:
            return
        for name in rfq.instrument_names():
            holders = self._instrument_holders.get(name)
            if holders is None:
                continue
            holders.discard(request_id)
            if holders:
                self._instrument_holders[name] = holders
            else:
                del self._instrument_holders[name]


class Orchestrator:
    """Owns the RFQ store and drives price -> risk -> quote for active RFQs."""

    def __init__(
        self,
        rest_client: CoincallRestClient,
        market_data: MarketDataService,
        pricing_model: PricingModel,
        risk_gate: RiskGate,
        quote_lifecycle: QuoteLifecycle,
        persistence: PersistenceStore | None = None,
    ) -> None:
        self._rest = rest_client
        self._market_data = market_data
        self._pricing_model = pricing_model
        self._risk_gate = risk_gate
        self._quotes = quote_lifecycle
        self._persistence = persistence
        self.rfq_store = RfqStore()

    async def handle_event(self, event: object) -> None:
        match event:
            case RfqReceived(rfq=rfq):
                await self._handle_rfq_received(rfq)
            case RfqTerminated():
                await self._handle_rfq_terminated(event)
            case QuoteUpdated():
                await self._handle_quote_updated(event)
            case TradeExecuted():
                await self._handle_trade(event)
            case PricesRefreshed():
                await self._handle_prices_refreshed(event)
            case RepriceTick():
                await self.reprice_all_active()
            case ReconcileTick():
                await self.reconcile_with_exchange()

    async def _handle_rfq_received(self, rfq: Rfq) -> None:
        self.rfq_store.upsert(rfq)
        for name in rfq.instrument_names():
            try:
                underlying = parse_instrument(name).underlying
            except InstrumentParseError:
                logger.warning("Skipping unparseable instrument %s on RFQ %s", name, rfq.request_id)
                continue
            self._market_data.track(underlying)
        if self._persistence is not None:
            await self._persistence.record_rfq(rfq, _now_ms())
        await self._reprice_and_quote(rfq.request_id)

    async def _handle_rfq_terminated(self, event: RfqTerminated) -> None:
        now_ms = _now_ms()
        updated = self.rfq_store.mark_terminal(event.request_id, event.status, now_ms)
        if updated is None:
            logger.debug("RFQ termination for unknown or evicted RFQ %s ignored", event.request_id)
            return
        settled = await self._settle_terminal_quote(event.request_id, event.status)
        if self._persistence is not None:
            await self._persistence.record_rfq(updated, now_ms)
        if not settled:
            return
        self._cleanup_and_evict_terminal_rfq(event.request_id)

    async def _settle_terminal_quote(self, request_id: str, status: RfqStatus) -> bool:
        try:
            if status is RfqStatus.FILLED:
                quote = await self._quotes.settle_filled_rfq(request_id)
                if quote is not None and quote.is_terminal and self._persistence is not None:
                    await self._persistence.record_quote(quote, None, _now_ms())
            else:
                await self._quotes.withdraw_for_rfq(request_id)
        except CoincallError:
            logger.exception("REST error cancelling quote for RFQ %s", request_id)
            if self._record_ambiguous_quote_failures() == 0:
                self._risk_gate.record_api_failure()
            return False
        except ValidationError as exc:
            logger.warning(
                "Malformed remote quote payload while settling RFQ %s: %s",
                request_id,
                exc,
            )
            return False
        else:
            self._record_ambiguous_quote_failures()
        return not self._quotes.has_open_or_pending_quote_for_rfq(request_id)

    def _cleanup_and_evict_terminal_rfq(self, request_id: str) -> None:
        for instrument_name in self.rfq_store.orphaned_instruments(request_id):
            try:
                underlying = parse_instrument(instrument_name).underlying
            except InstrumentParseError:
                continue
            if not self.rfq_store.active_for_underlying(underlying):
                self._market_data.untrack(underlying)
        self._quotes.evict_for_rfq(request_id)
        self.rfq_store.evict(request_id)

    async def _handle_quote_updated(self, event: QuoteUpdated) -> None:
        if (
            event.request_id
            and self.rfq_store.get(event.request_id) is None
            and self._quotes.get_by_quote_id(event.quote_id) is None
        ):
            logger.debug(
                "Quote update for unknown quote on evicted RFQ %s ignored (quote=%s)",
                event.request_id,
                event.quote_id,
            )
            return
        updated = self._quotes.apply_ws_update(event)
        if updated is not None and self._persistence is not None:
            await self._persistence.record_quote(updated, None, _now_ms())

    async def _handle_trade(self, event: TradeExecuted) -> None:
        logger.info("Trade executed: block_trade=%s quote=%s", event.block_trade_id, event.quote_id)
        if self._persistence is not None:
            await self._persistence.record_fill(event, _now_ms())

    async def _handle_prices_refreshed(self, event: PricesRefreshed) -> None:
        for underlying in event.underlyings:
            for rfq in self.rfq_store.active_for_underlying(underlying):
                await self._reprice_and_quote(rfq.request_id)

    async def reprice_all_active(self) -> None:
        """Force a reprice/requote pass over every active RFQ (the quote-refresh timer)."""
        for rfq in self.rfq_store.active():
            await self._reprice_and_quote(rfq.request_id)

    async def _reprice_and_quote(self, request_id: str) -> None:
        rfq = self.rfq_store.get(request_id)
        if rfq is None or rfq.is_terminal_status:
            return

        leg_prices = {}
        ages: dict[str, float] = {}
        for name in rfq.instrument_names():
            try:
                instrument = parse_instrument(name)
            except InstrumentParseError:
                logger.warning("Cannot price unparseable instrument %s", name)
                return
            underlying_price = self._market_data.get_price(instrument.underlying)
            if underlying_price is None:
                logger.debug("No price yet for %s (RFQ %s)", instrument.underlying, request_id)
                return
            leg_prices[name] = self._pricing_model.price(instrument, underlying_price)
            ages[name] = self._market_data.age_seconds(instrument.underlying)

        intent = build_quote_intent(rfq, leg_prices)
        if intent is None:
            return

        if rfq.stage is RfqStage.RECEIVED:
            rfq = rfq.with_stage(RfqStage.PRICED)
            self.rfq_store.upsert(rfq)

        now_ms = _now_ms()
        decision = self._risk_gate.evaluate(rfq, intent, ages, now_ms)
        if not decision.approved:
            logger.warning("Not quoting RFQ %s: %s", request_id, decision.reason)
            await self._withdraw_rejected_quote(request_id, decision.reason)
            return

        try:
            quote = await self._quotes.reconcile(intent)
            self._record_ambiguous_quote_failures()
            self._risk_gate.record_api_success()
        except CoincallError:
            logger.exception("REST error quoting RFQ %s", request_id)
            if self._record_ambiguous_quote_failures() == 0:
                self._risk_gate.record_api_failure()
            return

        if rfq.stage is RfqStage.PRICED:
            rfq = rfq.with_stage(RfqStage.QUOTED)
            self.rfq_store.upsert(rfq)

        if self._persistence is not None:
            snapshot: dict[str, float] = {}
            for name in rfq.instrument_names():
                maybe_instrument = _safe_parse(name)
                if maybe_instrument is None:
                    continue
                underlying_price = self._market_data.get_price(maybe_instrument.underlying)
                if underlying_price is not None:
                    snapshot[maybe_instrument.underlying] = underlying_price
            await self._persistence.record_quote(quote, snapshot, now_ms)

    async def _withdraw_rejected_quote(self, request_id: str, reason: str | None) -> None:
        before = self._quotes.get_for_rfq(request_id)
        try:
            withdrawn = await self._quotes.withdraw_for_rfq(request_id)
        except CoincallError:
            logger.exception("REST error cancelling rejected quote for RFQ %s", request_id)
            if self._record_ambiguous_quote_failures() == 0:
                self._risk_gate.record_api_failure()
        else:
            if (
                before is not None
                and not before.is_terminal
                and withdrawn is not None
                and withdrawn.stage is QuoteStage.CANCELLED
                and withdrawn.stage is not before.stage
            ):
                logger.warning(
                    "Withdrew quote for RFQ %s after risk rejection: %s",
                    request_id,
                    reason or "unknown reason",
                )
            self._record_ambiguous_quote_failures()

    async def reconcile_with_exchange(self) -> None:
        """Periodic reconciler: true local RFQ/quote state against exchange open lists."""
        try:
            response = await self._rest.get_rfq_list(state="OPEN")
        except CoincallError:
            logger.exception("Reconciler failed to fetch RFQ list")
            self._risk_gate.record_api_failure()
            return

        remote_ids = set()
        seen_remote_ids = set()
        for item in _rfq_items(response):
            request_id = _string_field(item, "requestId")
            if request_id is None or request_id in seen_remote_ids:
                continue
            seen_remote_ids.add(request_id)
            remote_ids.add(request_id)
            if self.rfq_store.get(request_id) is not None:
                continue
            try:
                rfq = rfq_from_payload(RfqPayload.model_validate(item))
            except ValueError as exc:
                logger.warning("Reconciler ignored malformed RFQ payload: %s", exc)
                continue
            if rfq.status is not RfqStatus.ACTIVE:
                continue
            logger.info("Reconciler: backfilling open RFQ %s from exchange", rfq.request_id)
            await self._handle_rfq_received(rfq)

        for rfq in self.rfq_store.active():
            if rfq.request_id not in remote_ids:
                logger.info(
                    "Reconciler: RFQ %s no longer open on exchange, marking terminal",
                    rfq.request_id,
                )
                await self._handle_rfq_terminated(RfqTerminated(rfq.request_id, RfqStatus.EXPIRED))
        for rfq in self.rfq_store.terminal():
            if self._quotes.has_open_or_pending_quote_for_rfq(rfq.request_id):
                logger.info(
                    "Reconciler: retrying terminal cleanup for RFQ %s",
                    rfq.request_id,
                )
                await self._handle_rfq_terminated(RfqTerminated(rfq.request_id, rfq.status))

        await self._reconcile_quote_state()

    async def _reconcile_quote_state(self) -> None:
        try:
            response = await self._rest.get_quote_list(state="OPEN")
        except CoincallError:
            logger.exception("Reconciler failed to fetch quote list")
            self._risk_gate.record_api_failure()
            return

        remote_open_quote_ids = set()
        for item in quote_items(response):
            quote_id = _string_field(item, "quoteId")
            if quote_id is None:
                continue
            remote_open_quote_ids.add(quote_id)
            local = self._quotes.get_by_quote_id(quote_id)
            if local is None or local.is_terminal:
                request_id = _string_field(item, "requestId")
                adopted = (
                    None
                    if request_id is None
                    else self._quotes.adopt_open_exchange_quote(request_id, quote_id)
                )
                if adopted is None:
                    await self._cancel_orphan_exchange_quote(quote_id)

        for quote in self._quotes.open_quotes():
            if quote.quote_id is None or quote.quote_id in remote_open_quote_ids:
                continue
            try:
                resolved = await self._quotes.resolve_remote_quote(quote)
            except CoincallError:
                logger.exception("Reconciler failed to resolve quote %s", quote.quote_id)
                self._risk_gate.record_api_failure()
                continue
            except ValidationError as exc:
                logger.warning(
                    "Reconciler ignored malformed quote payload for %s: %s",
                    quote.quote_id,
                    exc,
                )
                continue
            if resolved is not None and resolved.is_terminal and self._persistence is not None:
                await self._persistence.record_quote(resolved, None, _now_ms())

    async def _cancel_orphan_exchange_quote(self, quote_id: str) -> None:
        try:
            await self._quotes.cancel_exchange_quote(quote_id)
        except CoincallError:
            logger.exception("Reconciler failed to cancel orphan exchange quote %s", quote_id)
            self._risk_gate.record_api_failure()

    def _record_ambiguous_quote_failures(self) -> int:
        count = self._quotes.consume_ambiguous_transport_failures()
        for _ in range(count):
            self._risk_gate.record_api_failure()
        return count


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_parse(name: str) -> Instrument | None:
    try:
        return parse_instrument(name)
    except InstrumentParseError:
        return None


def _rfq_items(response: dict[str, Any]) -> list[dict[str, Any]]:
    data = response.get("data") or {}
    if not isinstance(data, dict):
        return []
    raw_items = data.get("rfqList", [])
    if not isinstance(raw_items, list):
        return []
    return [item for item in raw_items if isinstance(item, dict)]


def _string_field(item: dict[str, Any], field: str) -> str | None:
    value = item.get(field)
    return value if isinstance(value, str) and value else None
