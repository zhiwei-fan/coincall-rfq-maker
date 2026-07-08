"""Quote lifecycle actor: the single writer of the quote store.

Owns idempotent create/cancel/replace against the REST API and reacts to WS
quote updates. In `DRY_RUN` (the default) nothing is submitted to the
exchange — intents are computed and logged only.
"""

import logging
from dataclasses import replace

from coincall_rfq_maker.adapters.rest import CoincallApiError, CoincallRestClient
from coincall_rfq_maker.adapters.signing import get_timestamp_ms
from coincall_rfq_maker.domain.quote import IllegalQuoteTransition, Quote, QuoteLeg, QuoteStage
from coincall_rfq_maker.events import QuoteUpdated
from coincall_rfq_maker.quoting.strategy import QuoteIntent

logger = logging.getLogger(__name__)

_PRICE_TOLERANCE = 1e-9


class QuoteLifecycle:
    """In-memory quote store plus create/cancel/replace against the exchange."""

    def __init__(self, rest_client: CoincallRestClient, dry_run: bool = True) -> None:
        self._rest = rest_client
        self._dry_run = dry_run
        self._by_request: dict[str, Quote] = {}
        self._by_quote_id: dict[str, Quote] = {}

    def get_for_rfq(self, request_id: str) -> Quote | None:
        return self._by_request.get(request_id)

    def get_by_quote_id(self, quote_id: str) -> Quote | None:
        return self._by_quote_id.get(quote_id)

    async def reconcile(self, intent: QuoteIntent) -> Quote:
        """Idempotently ensure our live quote for `intent.request_id` matches `intent`."""
        existing = self._by_request.get(intent.request_id)
        if existing is not None and existing.is_open and _matches(existing, intent):
            return existing
        if existing is not None and existing.is_open:
            await self._cancel(existing)
        return await self._create(intent)

    async def cancel_for_rfq(self, request_id: str) -> None:
        existing = self._by_request.get(request_id)
        if existing is not None and existing.is_open:
            await self._cancel(existing)

    async def cancel_all(self) -> None:
        if self._dry_run:
            logger.info("[DRY RUN] would cancel all quotes")
            return
        await self._rest.cancel_all_quotes()

    def apply_ws_update(self, event: QuoteUpdated) -> Quote | None:
        """Mirror an exchange-reported quote state change into the local store."""
        existing = self._by_quote_id.get(event.quote_id)
        if existing is None:
            logger.debug("Quote update for unknown quote %s, ignoring", event.quote_id)
            return None
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
            filled_price=_coalesce(event.filled_price, updated.filled_price),
            filled_quantity=_coalesce(event.filled_quantity, updated.filled_quantity),
            fill_time_ms=_coalesce(event.fill_time_ms, updated.fill_time_ms),
            block_trade_id=event.block_trade_id or updated.block_trade_id,
            update_time_ms=get_timestamp_ms(),
        )
        self._store(updated)
        return updated

    async def _create(self, intent: QuoteIntent) -> Quote:
        now = get_timestamp_ms()
        pending = Quote(
            request_id=intent.request_id,
            stage=QuoteStage.PENDING_CREATE,
            legs=tuple(
                QuoteLeg(instrument_name=leg.instrument_name, price=leg.price)
                for leg in intent.legs
            ),
            create_time_ms=now,
        )
        self._store(pending)

        if self._dry_run:
            logger.info(
                "[DRY RUN] would create quote for RFQ %s legs=%s", intent.request_id, intent.legs
            )
            return pending

        payload_legs = [
            {"instrumentName": leg.instrument_name, "price": str(leg.price)} for leg in intent.legs
        ]
        try:
            response = await self._rest.create_quote(intent.request_id, payload_legs)
        except CoincallApiError:
            logger.exception("Failed to create quote for RFQ %s", intent.request_id)
            failed = pending.with_stage(QuoteStage.CANCELLED)
            self._store(failed)
            return failed

        quote_id = (response.get("data") or {}).get("quoteId")
        if not quote_id:
            logger.error(
                "Create-quote response missing quoteId for RFQ %s: %s",
                intent.request_id,
                response,
            )
            failed = pending.with_stage(QuoteStage.CANCELLED)
            self._store(failed)
            return failed

        opened = replace(pending.with_stage(QuoteStage.OPEN), quote_id=quote_id)
        self._store(opened)
        logger.info("Created quote %s for RFQ %s", quote_id, intent.request_id)
        return opened

    async def _cancel(self, quote: Quote) -> Quote:
        pending_cancel = quote.with_stage(QuoteStage.PENDING_CANCEL)
        self._store(pending_cancel)

        if self._dry_run:
            logger.info("[DRY RUN] would cancel quote %s", quote.quote_id)
        elif quote.quote_id:
            try:
                await self._rest.cancel_quote(quote.quote_id)
            except CoincallApiError:
                logger.exception("Failed to cancel quote %s", quote.quote_id)

        cancelled = pending_cancel.with_stage(QuoteStage.CANCELLED)
        self._store(cancelled)
        return cancelled

    def _store(self, quote: Quote) -> None:
        self._by_request[quote.request_id] = quote
        if quote.quote_id:
            self._by_quote_id[quote.quote_id] = quote


def _coalesce[T](new: T | None, old: T | None) -> T | None:
    return new if new is not None else old


def _matches(existing: Quote, intent: QuoteIntent) -> bool:
    if len(existing.legs) != len(intent.legs):
        return False
    existing_prices = {leg.instrument_name: leg.price for leg in existing.legs}
    for leg in intent.legs:
        current = existing_prices.get(leg.instrument_name)
        if current is None or abs(current - leg.price) > _PRICE_TOLERANCE:
            return False
    return True
