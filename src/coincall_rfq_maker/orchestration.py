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
from collections import OrderedDict
from dataclasses import dataclass

from coincall_rfq_maker.core.adapters.rest import (
    ApiFailureKind,
    CoincallError,
    CoincallRestClient,
    classify_api_failure,
    track_exchange_io,
)
from coincall_rfq_maker.core.clock import get_timestamp_ms
from coincall_rfq_maker.domain.instruments import (
    ExpiryMismatchError,
    InstrumentParseError,
    ParsedInstrument,
    parse_instrument,
    resolve_instrument,
)
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
from coincall_rfq_maker.marketdata.instruments import InstrumentCatalog
from coincall_rfq_maker.marketdata.service import MarketDataService
from coincall_rfq_maker.persistence.outbox import AuditOutbox
from coincall_rfq_maker.pricing.engine import PricingModel
from coincall_rfq_maker.quoting.lifecycle import QuoteLifecycle
from coincall_rfq_maker.quoting.strategy import build_quote_intent
from coincall_rfq_maker.reconciler import Reconciler
from coincall_rfq_maker.risk.gate import RiskGate

logger = logging.getLogger(__name__)

RFQ_TOMBSTONE_CAPACITY = 10_000

DispatchEvent = (
    RfqReceived
    | RfqTerminated
    | QuoteUpdated
    | TradeExecuted
    | PricesRefreshed
    | RepriceTick
    | ReconcileTick
)


@dataclass(slots=True)
class TransientOutageGate:
    cooldown_until_ms: int = 0
    consecutive_failures: int = 0
    jitter_counter: int = 0

    def in_cooldown(self, now_ms: int) -> bool:
        return now_ms < self.cooldown_until_ms

    def record_transient(self, now_ms: int) -> None:
        base_delay_ms = min(60_000, 1_000 * (2**self.consecutive_failures))
        jitter_ms = (self.jitter_counter % 5) * 50
        self.cooldown_until_ms = now_ms + base_delay_ms + jitter_ms
        self.consecutive_failures += 1
        self.jitter_counter += 1

    def record_success(self) -> None:
        self.cooldown_until_ms = 0
        self.consecutive_failures = 0


class RfqStore:
    """In-memory RFQ repository + instrument tracking.

    Single-writer: only `Orchestrator` (running on the dispatcher task)
    mutates this.
    """

    def __init__(self, tombstone_capacity: int = RFQ_TOMBSTONE_CAPACITY) -> None:
        if tombstone_capacity < 0:
            raise ValueError("tombstone_capacity must be non-negative")
        self._rfqs: dict[str, Rfq] = {}
        self._received_at_ms: dict[str, int] = {}
        self._instrument_holders: dict[str, set[str]] = {}
        self._tombstone_capacity = tombstone_capacity
        self._tombstones: OrderedDict[str, None] = OrderedDict()

    def upsert(self, rfq: Rfq, received_at_ms: int | None = None) -> None:
        existing = self._rfqs.get(rfq.request_id)
        if (
            rfq.request_id in self._tombstones
            or (existing is not None and existing.is_terminal_status)
        ) and not rfq.is_terminal_status:
            logger.warning(
                "refusing to resurrect terminal RFQ %s from a stale observation", rfq.request_id
            )
            return

        if existing is not None:
            stale_instruments = set(existing.instrument_names()) - set(rfq.instrument_names())
            for name in stale_instruments:
                holders = self._instrument_holders[name]
                holders.discard(rfq.request_id)
                if not holders:
                    del self._instrument_holders[name]
        else:
            self._received_at_ms[rfq.request_id] = (
                received_at_ms if received_at_ms is not None else get_timestamp_ms()
            )
        self._rfqs[rfq.request_id] = rfq
        for name in rfq.instrument_names():
            self._instrument_holders.setdefault(name, set()).add(rfq.request_id)
        if rfq.is_terminal_status:
            self._record_tombstone(rfq.request_id)

    def get(self, request_id: str) -> Rfq | None:
        return self._rfqs.get(request_id)

    def active(self) -> list[Rfq]:
        return [rfq for rfq in self._rfqs.values() if not rfq.is_terminal_status]

    def received_at_ms(self, request_id: str) -> int | None:
        return self._received_at_ms.get(request_id)

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
        if updated.is_terminal_status:
            self._record_tombstone(request_id)
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
        self._record_tombstone(request_id)
        rfq = self._rfqs.pop(request_id, None)
        if rfq is None:
            return
        self._received_at_ms.pop(request_id, None)
        for name in rfq.instrument_names():
            holders = self._instrument_holders.get(name)
            if holders is None:
                continue
            holders.discard(request_id)
            if holders:
                self._instrument_holders[name] = holders
            else:
                del self._instrument_holders[name]

    def _record_tombstone(self, request_id: str) -> None:
        """Remember terminal RFQs after eviction without unbounded process growth."""
        if request_id in self._tombstones:
            return
        self._tombstones[request_id] = None
        while len(self._tombstones) > self._tombstone_capacity:
            self._tombstones.popitem(last=False)


class Orchestrator:
    """Owns the RFQ store and drives price -> risk -> quote for active RFQs."""

    def __init__(
        self,
        rest_client: CoincallRestClient,
        market_data: MarketDataService,
        pricing_model: PricingModel,
        risk_gate: RiskGate,
        quote_lifecycle: QuoteLifecycle,
        instrument_catalog: InstrumentCatalog,
        audit_outbox: AuditOutbox | None = None,
        outage_gate: TransientOutageGate | None = None,
    ) -> None:
        self._market_data = market_data
        self._pricing_model = pricing_model
        self._risk_gate = risk_gate
        self._quotes = quote_lifecycle
        self._instruments = instrument_catalog
        self._audit = audit_outbox
        self._outage_gate = outage_gate if outage_gate is not None else TransientOutageGate()
        self.rfq_store = RfqStore()
        self._reconciler = Reconciler(
            rest_client=rest_client,
            rfq_store=self.rfq_store,
            quote_lifecycle=quote_lifecycle,
            risk_gate=risk_gate,
            on_rfq_backfill=self._handle_rfq_received,
            on_terminal_rfq=self._handle_reconciled_terminal_rfq,
            on_api_recovery=self._outage_gate.record_success,
            audit_outbox=audit_outbox,
        )

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

    async def reconcile_with_exchange(self) -> None:
        await self._reconciler.reconcile_with_exchange()

    @property
    def reconciler_last_cycle_ms(self) -> int | None:
        return self._reconciler.last_cycle_completed_ms

    async def _handle_rfq_received(self, rfq: Rfq) -> None:
        now_ms = get_timestamp_ms()
        self.rfq_store.upsert(rfq, received_at_ms=now_ms)
        for name in rfq.instrument_names():
            try:
                underlying = parse_instrument(name).underlying
            except InstrumentParseError:
                logger.warning("Skipping unparseable instrument %s on RFQ %s", name, rfq.request_id)
                continue
            self._market_data.track(underlying)
        if self._audit is not None:
            self._audit.enqueue_rfq(rfq, now_ms)
        await self._reprice_and_quote(rfq.request_id)

    async def _handle_rfq_terminated(self, event: RfqTerminated) -> None:
        now_ms = get_timestamp_ms()
        updated = self.rfq_store.mark_terminal(event.request_id, event.status, now_ms)
        if updated is None:
            logger.debug("RFQ termination for unknown or evicted RFQ %s ignored", event.request_id)
            return
        settled = await self._settle_terminal_quote(event.request_id, event.status)
        if self._audit is not None:
            self._audit.enqueue_rfq(updated, now_ms)
        if not settled:
            return
        self._cleanup_and_evict_terminal_rfq(event.request_id)

    async def _settle_terminal_quote(self, request_id: str, status: RfqStatus) -> bool:
        try:
            if status is RfqStatus.FILLED:
                quote = await self._quotes.settle_filled_rfq(request_id)
                if quote is not None and quote.is_terminal and self._audit is not None:
                    self._audit.enqueue_quote(quote, None, get_timestamp_ms())
            else:
                await self._quotes.withdraw_for_rfq(request_id)
        except CoincallError:
            logger.exception("REST error cancelling quote for RFQ %s", request_id)
            return False
        return not self._quotes.has_open_or_pending_quote_for_rfq(request_id)

    async def _handle_reconciled_terminal_rfq(self, request_id: str, status: RfqStatus) -> None:
        await self._handle_rfq_terminated(RfqTerminated(request_id, status))

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
        if updated is not None and self._audit is not None:
            self._audit.enqueue_quote(updated, None, get_timestamp_ms())

    async def _handle_trade(self, event: TradeExecuted) -> None:
        logger.info("Trade executed: block_trade=%s quote=%s", event.block_trade_id, event.quote_id)
        if self._audit is not None:
            self._audit.enqueue_fill(event, get_timestamp_ms())

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
                parsed = parse_instrument(name)
            except InstrumentParseError:
                logger.warning("Cannot price unparseable instrument %s", name)
                return
            expiration_ms = await self._instruments.expiration_ms(name)
            if expiration_ms is None:
                logger.warning(
                    "Not quoting RFQ %s: expiry metadata unavailable for %s (fail-closed)",
                    request_id,
                    name,
                )
                await self._withdraw_rejected_quote(request_id, f"no expiry metadata for {name}")
                return
            try:
                instrument = resolve_instrument(parsed, expiration_ms)
            except ExpiryMismatchError:
                logger.error(
                    "Not quoting RFQ %s: exchange expiry for %s contradicts its symbol date",
                    request_id,
                    name,
                    exc_info=True,
                )
                await self._withdraw_rejected_quote(request_id, f"expiry mismatch for {name}")
                return
            # Pricing only receives exchange expiry metadata that passed the symbol-date
            # cross-check. ParsedInstrument has no expiry instant, making 00:00 UTC
            # fabrication a type error; missing or contradictory metadata withdraws.
            underlying_price = self._market_data.get_price(instrument.underlying)
            if underlying_price is None:
                logger.debug("No price yet for %s (RFQ %s)", instrument.underlying, request_id)
                return
            leg_price = self._pricing_model.price(instrument, underlying_price)
            if leg_price is None:
                logger.warning(
                    "Not quoting RFQ %s: cannot price %s (expired or unpriceable)",
                    request_id,
                    name,
                )
                await self._withdraw_rejected_quote(request_id, f"cannot price {name}")
                return
            leg_prices[name] = leg_price
            ages[name] = self._market_data.age_seconds(instrument.underlying)

        intent = build_quote_intent(rfq, leg_prices)
        if intent is None:
            return

        if rfq.stage is RfqStage.RECEIVED:
            rfq = rfq.with_stage(RfqStage.PRICED)
            self.rfq_store.upsert(rfq)

        now_ms = get_timestamp_ms()
        decision = self._risk_gate.evaluate(rfq, intent, ages, now_ms)
        if not decision.approved or decision.plan is None:
            reason = decision.reason or "risk approval did not produce a plan"
            logger.warning("Not quoting RFQ %s: %s", request_id, reason)
            await self._withdraw_rejected_quote(request_id, reason)
            return

        if self._outage_gate.in_cooldown(now_ms):
            logger.info(
                "Quoting paused during transient Coincall outage cooldown for RFQ %s until %d",
                request_id,
                self._outage_gate.cooldown_until_ms,
            )
            return

        with track_exchange_io() as exchange_io:
            try:
                quote = await self._quotes.reconcile(decision.plan)
            except CoincallError as exc:
                kind = classify_api_failure(exc)
                if kind in {ApiFailureKind.TRANSIENT, ApiFailureKind.AMBIGUOUS}:
                    self._outage_gate.record_transient(get_timestamp_ms())
                    logger.warning(
                        "Quoting paused after %s Coincall failure for RFQ %s until %d",
                        kind.name.lower(),
                        request_id,
                        self._outage_gate.cooldown_until_ms,
                    )
                logger.exception("REST error quoting RFQ %s", request_id)
                return
        if exchange_io.attempted:
            self._outage_gate.record_success()

        if rfq.stage is RfqStage.PRICED:
            rfq = rfq.with_stage(RfqStage.QUOTED)
            self.rfq_store.upsert(rfq)

        if self._audit is not None:
            snapshot: dict[str, float] = {}
            for name in rfq.instrument_names():
                maybe_instrument = _safe_parse(name)
                if maybe_instrument is None:
                    continue
                underlying_price = self._market_data.get_price(maybe_instrument.underlying)
                if underlying_price is not None:
                    snapshot[maybe_instrument.underlying] = underlying_price
            self._audit.enqueue_quote(quote, snapshot, now_ms)

    async def _withdraw_rejected_quote(self, request_id: str, reason: str | None) -> None:
        before = self._quotes.get_for_rfq(request_id)
        try:
            withdrawn = await self._quotes.withdraw_for_rfq(request_id)
        except CoincallError:
            logger.exception("REST error cancelling rejected quote for RFQ %s", request_id)
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


def _safe_parse(name: str) -> ParsedInstrument | None:
    try:
        return parse_instrument(name)
    except InstrumentParseError:
        return None
