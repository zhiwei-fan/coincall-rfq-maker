"""Quote lifecycle actor: the single writer of the quote store.

Owns idempotent create/cancel/replace against the REST API and reacts to WS
quote updates. In `DRY_RUN` (the default) nothing is submitted to the
exchange — intents are computed and logged only.
"""

import logging
from dataclasses import replace

from coincall_rfq_maker.core.adapters.rest import (
    CoincallAmbiguousError,
    CoincallApiError,
    CoincallError,
    CoincallRestClient,
)
from coincall_rfq_maker.core.adapters.schemas import (
    QuotePayload,
    coalesce,
    find_remote_quote,
    find_salvaged_quote_id,
    quote_from_payload,
)
from coincall_rfq_maker.core.clock import current_time_ms
from coincall_rfq_maker.domain.quote import IllegalQuoteTransition, Quote, QuoteLeg, QuoteStage
from coincall_rfq_maker.events import QuoteUpdated
from coincall_rfq_maker.quoting.api_accounting import ApiOutcomeBoundary, ApiOutcomeReporter
from coincall_rfq_maker.quoting.store import QuoteStore
from coincall_rfq_maker.quoting.strategy import QuoteIntent, matches
from coincall_rfq_maker.risk.gate import ApprovedQuotePlan

logger = logging.getLogger(__name__)

# Matches reconciler.RFQ_ABSENT_FROM_OPEN_GRACE_SECONDS (120s). One read proves nothing,
# but sustained absence across several reads over 120s does.
PENDING_CREATE_UNRESOLVED_GRACE_MS = 120_000


class QuoteLifecycle:
    """In-memory quote store plus create/cancel/replace against the exchange."""

    def __init__(
        self,
        rest_client: CoincallRestClient,
        dry_run: bool = True,
        api_reporter: ApiOutcomeReporter | None = None,
    ) -> None:
        if not dry_run and api_reporter is None:
            raise ValueError("live mode requires an api_reporter (kill-switch accounting)")
        self._rest = rest_client
        self._dry_run = dry_run
        self._store = QuoteStore()
        self._api_boundary = ApiOutcomeBoundary(api_reporter)

    def get_for_rfq(self, request_id: str) -> Quote | None:
        return self._store.get_for_rfq(request_id)

    def get_by_quote_id(self, quote_id: str) -> Quote | None:
        return self._store.get_by_quote_id(quote_id)

    def open_quotes(self) -> list[Quote]:
        return self._store.open_quotes()

    def non_terminal_quotes(self) -> list[Quote]:
        return self._store.non_terminal_quotes()

    def has_open_or_pending_quote_for_rfq(self, request_id: str) -> bool:
        quote = self._store.get_for_rfq(request_id)
        if quote is None or quote.is_terminal:
            return False
        return not (self._dry_run and quote.stage is QuoteStage.PENDING_CREATE)

    def evict_for_rfq(self, request_id: str) -> None:
        self._store.evict_for_rfq(request_id)

    async def reconcile(self, plan: ApprovedQuotePlan) -> Quote:
        if not isinstance(plan, ApprovedQuotePlan):
            raise TypeError("quote submission requires a risk-approved plan")
        return await self._api_boundary.run(lambda: self._reconcile(plan.intent))

    async def _reconcile(self, intent: QuoteIntent) -> Quote:
        """Idempotently ensure our live quote for `intent.request_id` matches `intent`."""
        existing = self._store.get_for_rfq(intent.request_id)
        if existing is not None and existing.stage is QuoteStage.FILLED:
            return existing
        if existing is not None and existing.stage is QuoteStage.PENDING_CANCEL:
            resolved = await self._resolve_pending_cancel(existing)
            if resolved is None:
                raise CoincallAmbiguousError(
                    f"Pending cancel for RFQ {intent.request_id} remains ambiguous"
                )
            if resolved.stage is QuoteStage.FILLED:
                return resolved
            if not resolved.is_terminal:
                return resolved
            return await self._create(intent)
        pending_create = existing is not None and existing.stage is QuoteStage.PENDING_CREATE
        if existing is not None and pending_create and not self._dry_run:
            opened = await self._verify_pending_create(existing)
            if opened is not None:
                if matches(opened, intent):
                    return opened
                await self._cancel(opened)
            return await self._create(intent)
        if existing is not None and existing.is_open and matches(existing, intent):
            return existing
        if existing is not None and existing.is_open:
            await self._cancel(existing)
        return await self._create(intent)

    async def cancel_for_rfq(self, request_id: str) -> None:
        await self._api_boundary.run(lambda: self._cancel_for_rfq(request_id))

    async def _cancel_for_rfq(self, request_id: str) -> None:
        existing = self._store.get_for_rfq(request_id)
        if existing is not None and existing.is_open:
            await self._cancel(existing)

    async def withdraw_for_rfq(self, request_id: str) -> Quote | None:
        return await self._api_boundary.run(lambda: self._withdraw_for_rfq(request_id))

    async def _withdraw_for_rfq(self, request_id: str) -> Quote | None:
        """Fail-closed withdrawal for every non-terminal local quote stage."""
        existing = self._store.get_for_rfq(request_id)
        if existing is None or existing.is_terminal:
            return existing
        if existing.stage is QuoteStage.OPEN:
            return await self._cancel(existing)
        if existing.stage is QuoteStage.PENDING_CREATE:
            opened = None if self._dry_run else await self._verify_pending_create(existing)
            if opened is not None:
                return await self._cancel(opened)
            # Absence from ONE OPEN-list read never proves a create failed. Only sustained
            # absence past the grace window does.
            if not self._dry_run:
                age_ms = current_time_ms() - existing.create_time_ms
                if age_ms < PENDING_CREATE_UNRESOLVED_GRACE_MS:
                    raise CoincallAmbiguousError(
                        f"Create for RFQ {request_id} unresolved; absence from one OPEN "
                        f"read is not proof (age {age_ms}ms)"
                    )
            cancelled = existing.with_stage(QuoteStage.CANCELLED)
            self._store.store(cancelled)
            return cancelled
        if existing.stage is QuoteStage.PENDING_CANCEL:
            if existing.quote_id is None:
                cancelled = existing.with_stage(QuoteStage.CANCELLED)
                self._store.store(cancelled)
                return cancelled
            try:
                resolved = await self._resolve_pending_cancel(existing)
            except CoincallAmbiguousError:
                current = self._store.get_for_rfq(request_id)
                if current is not None and current.is_open:
                    return await self._cancel(current)
                raise
            if resolved is None:
                current = self._store.get_for_rfq(request_id)
                if current is not None and current.is_open:
                    return await self._cancel(current)
                raise CoincallAmbiguousError(
                    f"Pending cancel for RFQ {request_id} remains ambiguous"
                )
            if resolved.is_open:
                return await self._cancel(resolved)
            return resolved
        return existing

    async def settle_filled_rfq(self, request_id: str) -> Quote | None:
        return await self._api_boundary.run(lambda: self._settle_filled_rfq(request_id))

    async def _settle_filled_rfq(self, request_id: str) -> Quote | None:
        """Mirror exchange quote state for an RFQ reported FILLED by the exchange."""
        existing = self._store.get_for_rfq(request_id)
        if existing is None:
            return None
        if existing.is_terminal:
            return existing
        if self._dry_run:
            logger.info(
                "[DRY RUN] would settle filled RFQ %s by withdrawing local quote",
                request_id,
            )
            cancelled = existing.with_stage(QuoteStage.CANCELLED)
            self._store.store(cancelled)
            return cancelled

        resolved: Quote | None
        if existing.quote_id is not None:
            resolved = await self.resolve_remote_quote(existing)
        elif existing.stage is QuoteStage.PENDING_CREATE:
            resolved = await self._resolve_remote_quote_by_request(existing)
        else:
            resolved = None
        if resolved is not None and resolved.stage is QuoteStage.OPEN:
            logger.warning(
                "RFQ %s terminated FILLED but quote %s remains OPEN on exchange; cancelling it",
                request_id,
                resolved.quote_id,
            )
            return await self._cancel(resolved)
        return resolved

    async def cancel_all(self) -> None:
        await self._api_boundary.run(self._cancel_all)

    async def _cancel_all(self) -> None:
        if self._dry_run:
            logger.info("[DRY RUN] would cancel all quotes")
            return
        await self._rest.cancel_all_quotes()

    async def cancel_exchange_quote(self, quote_id: str) -> None:
        await self._api_boundary.run(lambda: self._cancel_exchange_quote(quote_id))

    async def _cancel_exchange_quote(self, quote_id: str) -> None:
        """Cancel an exchange-open quote that should not be represented locally."""
        if self._dry_run:
            logger.info("[DRY RUN] would cancel orphan exchange quote %s", quote_id)
            return
        await self._rest.cancel_quote(quote_id)

    def adopt_open_exchange_quote(
        self,
        request_id: str,
        quote_id: str,
        payload: QuotePayload | None = None,
    ) -> Quote | None:
        """Adopt an exchange-open quote that matches a known local RFQ quote."""
        if not request_id or not quote_id:
            return None
        existing = self._store.get_for_rfq(request_id)
        if existing is None or existing.stage not in {QuoteStage.PENDING_CREATE, QuoteStage.OPEN}:
            return None
        if existing.quote_id is not None and existing.quote_id != quote_id:
            logger.warning(
                "Refusing to adopt exchange quote %s for RFQ %s; local quote %s remains open",
                quote_id,
                request_id,
                existing.quote_id,
            )
            return None
        if payload is not None:
            return self._mirror_remote_quote(existing, payload)
        return self._adopt_open_quote(existing, quote_id)

    async def resolve_remote_quote(self, quote: Quote) -> Quote | None:
        if quote.quote_id is None:
            return await self._api_boundary.run(
                lambda: self._resolve_remote_quote_by_request(quote)
            )
        return await self._api_boundary.run(lambda: self._resolve_remote_quote(quote))

    async def _resolve_remote_quote(self, quote: Quote) -> Quote | None:
        """Fetch one quote by id and mirror the exchange state into the local store."""
        if quote.quote_id is None:
            return None
        response = await self._rest.get_quote_list(quote_id=quote.quote_id)
        remote = find_remote_quote(response, quote_id=quote.quote_id)
        if remote is None:
            return None
        return self._mirror_remote_quote(quote, remote)

    def apply_ws_update(self, event: QuoteUpdated) -> Quote | None:
        """Mirror an exchange-reported quote state change into the local store."""
        existing = self._store.get_by_quote_id(event.quote_id)
        if existing is None:
            logger.debug("Quote update for unknown quote %s, ignoring", event.quote_id)
            return None
        filled_price = coalesce(event.filled_price, existing.filled_price)
        filled_quantity = coalesce(event.filled_quantity, existing.filled_quantity)
        fill_time_ms = coalesce(event.fill_time_ms, existing.fill_time_ms)
        block_trade_id = event.block_trade_id or existing.block_trade_id
        if event.stage is existing.stage:
            if (
                filled_price == existing.filled_price
                and filled_quantity == existing.filled_quantity
                and fill_time_ms == existing.fill_time_ms
                and block_trade_id == existing.block_trade_id
            ):
                return None
            updated = existing
        else:
            try:
                updated = existing.with_stage(event.stage)
            except IllegalQuoteTransition:
                logger.warning(
                    "Ignoring illegal quote transition %s -> %s for %s",
                    existing.stage,
                    event.stage,
                    event.quote_id,
                )
                return None
        updated = replace(
            updated,
            filled_price=filled_price,
            filled_quantity=filled_quantity,
            fill_time_ms=fill_time_ms,
            block_trade_id=block_trade_id,
            update_time_ms=current_time_ms(),
        )
        self._store.store(updated)
        return updated

    async def _create(self, intent: QuoteIntent, _conflict_retry: bool = False) -> Quote:
        now = current_time_ms()
        pending = Quote(
            request_id=intent.request_id,
            stage=QuoteStage.PENDING_CREATE,
            legs=tuple(
                QuoteLeg(instrument_name=leg.instrument_name, price=leg.price)
                for leg in intent.legs
            ),
            create_time_ms=now,
        )
        self._store.store(pending)

        if self._dry_run:
            logger.info(
                "[DRY RUN] would create quote for RFQ %s legs=%s", intent.request_id, intent.legs
            )
            return pending

        payload_legs = [
            {"instrumentName": leg.instrument_name, "price": str(leg.price)} for leg in intent.legs
        ]
        try:
            result = await self._rest.create_quote(intent.request_id, payload_legs)
        except CoincallAmbiguousError as exc:
            logger.warning(
                "Create quote for RFQ %s is ambiguous; verifying open quotes",
                intent.request_id,
            )
            return await self._resolve_ambiguous_create(pending, exc)
        except CoincallApiError as exc:
            if exc.code != 50012:
                logger.exception("Failed to create quote for RFQ %s", intent.request_id)
                raise

            # The exchange enforces <=1 OPEN quote per account per RFQ; 50012 proves one
            # exists, so it must never strike the kill switch or pause unrelated RFQs.
            logger.info(
                "create for RFQ %s rejected 50012 (quote exists); resolving exchange truth",
                intent.request_id,
            )
            opened = await self._verify_pending_create(pending)
            if opened is None:
                # Absence from one OPEN-list read never proves a create failed, so do not
                # blind-create after a 50012; a future reprice pass will resolve it again.
                raise exc
            if matches(opened, intent):
                return opened
            # Exactly one re-create attempt per pass: a second 50012 must not loop or repeat
            # the cancellation of an exchange quote that is still reported as OPEN.
            if _conflict_retry:
                raise exc
            await self._cancel(opened)
            return await self._create(intent, _conflict_retry=True)
        except CoincallError:
            logger.exception("Failed to create quote for RFQ %s", intent.request_id)
            raise

        opened = replace(pending.with_stage(QuoteStage.OPEN), quote_id=result.quote_id)
        self._store.store(opened)
        logger.info("Created quote %s for RFQ %s", result.quote_id, intent.request_id)
        return opened

    async def _cancel(self, quote: Quote) -> Quote:
        pending_cancel = quote.with_stage(QuoteStage.PENDING_CANCEL)
        self._store.store(pending_cancel)

        if self._dry_run:
            logger.info("[DRY RUN] would cancel quote %s", quote.quote_id)
        elif quote.quote_id:
            try:
                await self._rest.cancel_quote(quote.quote_id)
            except CoincallAmbiguousError as exc:
                logger.warning(
                    "Cancel quote %s is ambiguous; verifying open quotes",
                    quote.quote_id,
                )
                return await self._resolve_ambiguous_cancel(pending_cancel, exc)
            except CoincallError:
                logger.exception("Failed to cancel quote %s", quote.quote_id)
                reverted = pending_cancel.with_stage(QuoteStage.OPEN)
                self._store.store(reverted)
                raise

        cancelled = pending_cancel.with_stage(QuoteStage.CANCELLED)
        self._store.store(cancelled)
        return cancelled

    async def _resolve_ambiguous_create(self, pending: Quote, exc: CoincallAmbiguousError) -> Quote:
        opened = await self._verify_pending_create(pending)
        if opened is None:
            # One OPEN-list read may lag the exchange, so absence does not prove failure.
            raise exc
        return opened

    async def _verify_pending_create(self, pending: Quote) -> Quote | None:
        remote_quotes = await self._rest.get_quote_list(request_id=pending.request_id, state="OPEN")
        remote = find_remote_quote(remote_quotes, request_id=pending.request_id)
        if remote is not None:
            logger.info(
                "Adopting quote %s for RFQ %s from well-formed open-quote payload",
                remote.quote_id,
                pending.request_id,
            )
            return self._mirror_remote_quote(pending, remote)

        quote_id = find_salvaged_quote_id(remote_quotes, request_id=pending.request_id)
        if quote_id is None:
            return None
        logger.warning(
            "Adopting quote %s for RFQ %s from malformed open-quote payload",
            quote_id,
            pending.request_id,
        )
        return self._adopt_open_quote(pending, quote_id)

    async def _resolve_remote_quote_by_request(self, quote: Quote) -> Quote | None:
        remote_quotes = await self._rest.get_quote_list(request_id=quote.request_id)
        remote = find_remote_quote(remote_quotes, request_id=quote.request_id)
        if remote is None:
            return None
        return self._mirror_remote_quote(quote, remote)

    async def _resolve_ambiguous_cancel(
        self, pending_cancel: Quote, exc: CoincallAmbiguousError
    ) -> Quote:
        resolved = await self._resolve_pending_cancel(pending_cancel)
        if resolved is None:
            raise exc
        return resolved

    async def _resolve_pending_cancel(self, pending_cancel: Quote) -> Quote | None:
        if pending_cancel.quote_id is None:
            return None
        remote_quotes = await self._rest.get_quote_list(quote_id=pending_cancel.quote_id)
        remote = find_remote_quote(remote_quotes, quote_id=pending_cancel.quote_id)
        if remote is None:
            return None

        resolved = quote_from_payload(pending_cancel, remote)
        self._store.store(resolved)
        if resolved.stage is QuoteStage.OPEN:
            return None
        logger.info(
            "Verified quote %s resolved to exchange state %s",
            pending_cancel.quote_id,
            resolved.stage,
        )
        return resolved

    def _mirror_remote_quote(self, quote: Quote, remote: QuotePayload) -> Quote:
        resolved = quote_from_payload(quote, remote)
        self._store.store(resolved)
        logger.info(
            "Reconciled quote %s to exchange state %s",
            resolved.quote_id,
            resolved.stage,
        )
        return resolved

    def _adopt_open_quote(self, quote: Quote, quote_id: str) -> Quote:
        opened = quote if quote.stage is QuoteStage.OPEN else quote.with_stage(QuoteStage.OPEN)
        # An adopted quote stores exchange-reported legs when available; malformed payloads use
        # legs=() to mean price unknown. Attempted legs survive only when the quote id matches.
        legs = quote.legs if quote.quote_id == quote_id else ()
        opened = replace(opened, quote_id=quote_id, legs=legs)
        self._store.store(opened)
        logger.info("Adopted exchange quote %s for RFQ %s", quote_id, quote.request_id)
        return opened
