"""Periodic reconciliation against exchange RFQ and quote snapshots."""

import logging
from collections.abc import Awaitable, Callable
from typing import Protocol

from coincall_rfq_maker.core.adapters.rest import (
    ApiFailureKind,
    CoincallError,
    CoincallRestClient,
    classify_api_failure,
)
from coincall_rfq_maker.core.adapters.schemas import rfq_from_payload
from coincall_rfq_maker.core.clock import get_timestamp_ms
from coincall_rfq_maker.domain.quote import Quote
from coincall_rfq_maker.domain.rfq import Rfq, RfqStatus
from coincall_rfq_maker.persistence.store import PersistenceStore
from coincall_rfq_maker.quoting.lifecycle import QuoteLifecycle
from coincall_rfq_maker.risk.gate import RiskGate

logger = logging.getLogger(__name__)

RFQ_ABSENT_FROM_OPEN_GRACE_SECONDS = 120.0


class RfqRepository(Protocol):
    def get(self, request_id: str) -> Rfq | None: ...

    def active(self) -> list[Rfq]: ...

    def terminal(self) -> list[Rfq]: ...

    def received_at_ms(self, request_id: str) -> int | None: ...


RfqBackfillHandler = Callable[[Rfq], Awaitable[None]]
TerminalRfqHandler = Callable[[str, RfqStatus], Awaitable[None]]
ApiRecoveryHandler = Callable[[], None]


class Reconciler:
    """True local RFQ/quote state against exchange open lists."""

    def __init__(
        self,
        rest_client: CoincallRestClient,
        rfq_store: RfqRepository,
        quote_lifecycle: QuoteLifecycle,
        risk_gate: RiskGate,
        on_rfq_backfill: RfqBackfillHandler,
        on_terminal_rfq: TerminalRfqHandler,
        on_api_recovery: ApiRecoveryHandler,
        persistence: PersistenceStore | None = None,
    ) -> None:
        self._rest = rest_client
        self._rfqs = rfq_store
        self._quotes = quote_lifecycle
        self._risk_gate = risk_gate
        self._on_rfq_backfill = on_rfq_backfill
        self._on_terminal_rfq = on_terminal_rfq
        self._on_api_recovery = on_api_recovery
        self._persistence = persistence

    async def reconcile_with_exchange(self) -> None:
        # Treats the OPEN rfqList response as a COMPLETE snapshot: RFQs absent from it
        # (past the grace window) are expired locally. Owner-confirmed 2026-07-09 that
        # Coincall's rfqList does not paginate. If it ever gains paging, this single-page
        # fetch would wrongly expire RFQs on later pages — page through all results first.
        try:
            remote_rfqs = await self._rest.get_rfq_list(state="OPEN")
        except CoincallError as exc:
            logger.exception("Reconciler failed to fetch RFQ list")
            if classify_api_failure(exc) is ApiFailureKind.PERSISTENT:
                self._risk_gate.record_api_failure()
            return
        remote_ids = set(remote_rfqs.malformed_request_ids)
        for request_id in remote_rfqs.malformed_request_ids:
            logger.warning(
                "Reconciler: RFQ %s present in exchange OPEN snapshot but payload was malformed",
                request_id,
            )
        seen_remote_ids = set()
        for payload in remote_rfqs.payloads:
            if payload.request_id in seen_remote_ids:
                continue
            seen_remote_ids.add(payload.request_id)
            remote_ids.add(payload.request_id)
            if self._rfqs.get(payload.request_id) is not None:
                continue
            rfq = rfq_from_payload(payload)
            if rfq.status is not RfqStatus.ACTIVE:
                continue
            logger.info("Reconciler: backfilling open RFQ %s from exchange", rfq.request_id)
            await self._on_rfq_backfill(rfq)

        await self._expire_rfqs_absent_from_open_snapshot(remote_ids)
        for rfq in self._rfqs.terminal():
            if self._quotes.has_open_or_pending_quote_for_rfq(rfq.request_id):
                logger.info(
                    "Reconciler: retrying terminal cleanup for RFQ %s",
                    rfq.request_id,
                )
                await self._on_terminal_rfq(rfq.request_id, rfq.status)

        if not await self._reconcile_quote_state():
            return

        self._risk_gate.record_api_success()
        self._on_api_recovery()

    async def _expire_rfqs_absent_from_open_snapshot(self, remote_ids: set[str]) -> None:
        now_ms = get_timestamp_ms()
        for rfq in self._rfqs.active():
            if rfq.request_id in remote_ids:
                continue
            received_at_ms = self._rfqs.received_at_ms(rfq.request_id)
            age_seconds = None if received_at_ms is None else (now_ms - received_at_ms) / 1000
            if age_seconds is not None and age_seconds < RFQ_ABSENT_FROM_OPEN_GRACE_SECONDS:
                logger.debug(
                    "Reconciler: RFQ %s absent from exchange OPEN snapshot for %.1fs; "
                    "within %.1fs grace",
                    rfq.request_id,
                    age_seconds,
                    RFQ_ABSENT_FROM_OPEN_GRACE_SECONDS,
                )
                continue
            logger.info(
                "Reconciler: RFQ %s no longer open on exchange, marking terminal",
                rfq.request_id,
            )
            await self._on_terminal_rfq(rfq.request_id, RfqStatus.EXPIRED)

    async def _reconcile_quote_state(self) -> bool:
        # Same complete-snapshot assumption as reconcile_with_exchange: quoteList OPEN does
        # not paginate (owner-confirmed 2026-07-09). Revisit if the endpoint gains paging.
        try:
            remote_quotes = await self._rest.get_quote_list(state="OPEN")
        except CoincallError as exc:
            logger.exception("Reconciler failed to fetch quote list")
            if classify_api_failure(exc) is ApiFailureKind.PERSISTENT:
                self._risk_gate.record_api_failure()
            return False

        remote_open_quote_ids = {quote_id for _, quote_id in remote_quotes.malformed_id_pairs}
        for request_id, quote_id in remote_quotes.malformed_id_pairs:
            logger.warning(
                "Reconciler: quote %s for RFQ %s present in exchange OPEN snapshot "
                "but payload was malformed",
                quote_id,
                request_id,
            )
            self._quotes.adopt_open_exchange_quote(request_id, quote_id)

        for item in remote_quotes.payloads:
            quote_id = item.quote_id
            remote_open_quote_ids.add(quote_id)
            local = self._quotes.get_by_quote_id(quote_id)
            if local is None or local.is_terminal:
                item_request_id = item.request_id
                adopted = (
                    None
                    if item_request_id is None
                    else self._quotes.adopt_open_exchange_quote(item_request_id, quote_id, item)
                )
                if adopted is None:
                    await self._cancel_orphan_exchange_quote(quote_id)

        for quote in self._quotes.non_terminal_quotes():
            if quote.is_open and quote.quote_id in remote_open_quote_ids:
                continue
            try:
                resolved = await self._quotes.resolve_remote_quote(quote)
            except CoincallError:
                logger.exception(
                    "Reconciler failed to resolve quote %s for RFQ %s",
                    quote.quote_id,
                    quote.request_id,
                )
                continue
            await self._record_terminal_quote(resolved)
        return True

    async def _cancel_orphan_exchange_quote(self, quote_id: str) -> None:
        try:
            await self._quotes.cancel_exchange_quote(quote_id)
        except CoincallError:
            logger.exception("Reconciler failed to cancel orphan exchange quote %s", quote_id)

    async def _record_terminal_quote(self, quote: Quote | None) -> None:
        if quote is not None and quote.is_terminal and self._persistence is not None:
            await self._persistence.record_quote(quote, None, get_timestamp_ms())
