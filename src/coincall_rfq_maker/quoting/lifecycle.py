"""Quote lifecycle actor: the single writer of the quote store.

Owns idempotent create/cancel/replace against the REST API and reacts to WS
quote updates. In `DRY_RUN` (the default) nothing is submitted to the
exchange — intents are computed and logged only.
"""

import logging
from dataclasses import replace

from coincall_rfq_maker.adapters.rest import (
    CoincallAmbiguousError,
    CoincallError,
    CoincallRequestError,
    CoincallRestClient,
)
from coincall_rfq_maker.adapters.schemas import (
    QuoteListSnapshot,
    QuotePayload,
    quote_stage_from_wire,
)
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
        self._ambiguous_transport_failures = 0

    def get_for_rfq(self, request_id: str) -> Quote | None:
        return self._by_request.get(request_id)

    def get_by_quote_id(self, quote_id: str) -> Quote | None:
        return self._by_quote_id.get(quote_id)

    def open_quotes(self) -> list[Quote]:
        return [quote for quote in self._by_request.values() if quote.is_open]

    def has_open_or_pending_quote_for_rfq(self, request_id: str) -> bool:
        quote = self._by_request.get(request_id)
        if quote is None or quote.is_terminal:
            return False
        return not (self._dry_run and quote.stage is QuoteStage.PENDING_CREATE)

    def consume_ambiguous_transport_failures(self) -> int:
        count = self._ambiguous_transport_failures
        self._ambiguous_transport_failures = 0
        return count

    def evict_for_rfq(self, request_id: str) -> None:
        self._by_request.pop(request_id, None)
        stale_quote_ids = [
            quote_id
            for quote_id, quote in self._by_quote_id.items()
            if quote.request_id == request_id
        ]
        for quote_id in stale_quote_ids:
            del self._by_quote_id[quote_id]

    async def reconcile(self, intent: QuoteIntent) -> Quote:
        """Idempotently ensure our live quote for `intent.request_id` matches `intent`."""
        existing = self._by_request.get(intent.request_id)
        if existing is not None and existing.stage is QuoteStage.FILLED:
            return existing
        if existing is not None and existing.stage is QuoteStage.PENDING_CANCEL:
            resolved = await self._resolve_ambiguous_cancel(
                existing,
                CoincallAmbiguousError(
                    f"Pending cancel for RFQ {intent.request_id} remains ambiguous"
                ),
            )
            if resolved.stage is QuoteStage.FILLED:
                return resolved
            if not resolved.is_terminal:
                return resolved
            return await self._create(intent)
        if (
            existing is not None
            and existing.stage is QuoteStage.PENDING_CREATE
            and not self._dry_run
        ):
            opened = await self._verify_pending_create(existing)
            if opened is not None:
                if _matches(opened, intent):
                    return opened
                await self._cancel(opened)
            return await self._create(intent)
        if existing is not None and existing.is_open and _matches(existing, intent):
            return existing
        if existing is not None and existing.is_open:
            await self._cancel(existing)
        return await self._create(intent)

    async def cancel_for_rfq(self, request_id: str) -> None:
        existing = self._by_request.get(request_id)
        if existing is not None and existing.is_open:
            await self._cancel(existing)

    async def withdraw_for_rfq(self, request_id: str) -> Quote | None:
        """Fail-closed withdrawal for every non-terminal local quote stage."""
        existing = self._by_request.get(request_id)
        if existing is None or existing.is_terminal:
            return existing
        if existing.stage is QuoteStage.OPEN:
            return await self._cancel(existing)
        if existing.stage is QuoteStage.PENDING_CREATE:
            opened = None if self._dry_run else await self._verify_pending_create(existing)
            if opened is not None:
                return await self._cancel(opened)
            cancelled = existing.with_stage(QuoteStage.CANCELLED)
            self._store(cancelled)
            return cancelled
        if existing.stage is QuoteStage.PENDING_CANCEL:
            if existing.quote_id is None:
                cancelled = existing.with_stage(QuoteStage.CANCELLED)
                self._store(cancelled)
                return cancelled
            try:
                resolved = await self._resolve_ambiguous_cancel(
                    existing,
                    CoincallAmbiguousError(
                        f"Pending cancel for RFQ {request_id} remains ambiguous"
                    ),
                )
            except CoincallAmbiguousError:
                current = self._by_request.get(request_id)
                if current is not None and current.is_open:
                    return await self._cancel(current)
                raise
            if resolved.is_open:
                return await self._cancel(resolved)
            return resolved
        return existing

    async def settle_filled_rfq(self, request_id: str) -> Quote | None:
        """Mirror exchange quote state for an RFQ reported FILLED by the exchange."""
        existing = self._by_request.get(request_id)
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
            self._store(cancelled)
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
        if self._dry_run:
            logger.info("[DRY RUN] would cancel all quotes")
            return
        await self._rest.cancel_all_quotes()

    async def cancel_exchange_quote(self, quote_id: str) -> None:
        """Cancel an exchange-open quote that should not be represented locally."""
        if self._dry_run:
            logger.info("[DRY RUN] would cancel orphan exchange quote %s", quote_id)
            return
        await self._rest.cancel_quote(quote_id)

    def adopt_open_exchange_quote(self, request_id: str, quote_id: str) -> Quote | None:
        """Adopt an exchange-open quote that matches a known local RFQ quote."""
        if not request_id or not quote_id:
            return None
        existing = self._by_request.get(request_id)
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
        return self._adopt_open_quote(existing, quote_id)

    async def resolve_remote_quote(self, quote: Quote) -> Quote | None:
        """Fetch one quote by id and mirror the exchange state into the local store."""
        if quote.quote_id is None:
            return None
        response = await self._rest.get_quote_list(quote_id=quote.quote_id)
        remote = _find_remote_quote(response, quote_id=quote.quote_id)
        if remote is None:
            return None
        return self._mirror_remote_quote(quote, remote)

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
            result = await self._rest.create_quote(intent.request_id, payload_legs)
        except CoincallAmbiguousError as exc:
            self._ambiguous_transport_failures += 1
            logger.warning(
                "Create quote for RFQ %s is ambiguous; verifying open quotes",
                intent.request_id,
            )
            return await self._resolve_ambiguous_create(pending, exc)
        except CoincallError:
            logger.exception("Failed to create quote for RFQ %s", intent.request_id)
            raise

        opened = replace(pending.with_stage(QuoteStage.OPEN), quote_id=result.quote_id)
        self._store(opened)
        logger.info("Created quote %s for RFQ %s", result.quote_id, intent.request_id)
        return opened

    async def _cancel(self, quote: Quote) -> Quote:
        pending_cancel = quote.with_stage(QuoteStage.PENDING_CANCEL)
        self._store(pending_cancel)

        if self._dry_run:
            logger.info("[DRY RUN] would cancel quote %s", quote.quote_id)
        elif quote.quote_id:
            try:
                await self._rest.cancel_quote(quote.quote_id)
            except CoincallAmbiguousError as exc:
                self._ambiguous_transport_failures += 1
                logger.warning(
                    "Cancel quote %s is ambiguous; verifying open quotes",
                    quote.quote_id,
                )
                return await self._resolve_ambiguous_cancel(pending_cancel, exc)
            except CoincallError:
                logger.exception("Failed to cancel quote %s", quote.quote_id)
                reverted = pending_cancel.with_stage(QuoteStage.OPEN)
                self._store(reverted)
                raise

        cancelled = pending_cancel.with_stage(QuoteStage.CANCELLED)
        self._store(cancelled)
        return cancelled

    async def _resolve_ambiguous_create(self, pending: Quote, exc: CoincallAmbiguousError) -> Quote:
        opened = await self._verify_pending_create(pending)
        if opened is None:
            raise CoincallRequestError(
                f"Ambiguous create for RFQ {pending.request_id} was not found open on exchange"
            ) from exc
        return opened

    async def _verify_pending_create(self, pending: Quote) -> Quote | None:
        remote_quotes = await self._rest.get_quote_list(request_id=pending.request_id, state="OPEN")
        remote = _find_remote_quote(remote_quotes, request_id=pending.request_id)
        if remote is not None:
            return self._adopt_open_quote(pending, remote.quote_id)

        quote_id = _find_salvaged_quote_id(remote_quotes, request_id=pending.request_id)
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
        remote = _find_remote_quote(remote_quotes, request_id=quote.request_id)
        if remote is None:
            return None
        return self._mirror_remote_quote(quote, remote)

    async def _resolve_ambiguous_cancel(
        self, pending_cancel: Quote, exc: CoincallAmbiguousError
    ) -> Quote:
        if pending_cancel.quote_id is None:
            raise exc
        remote_quotes = await self._rest.get_quote_list(quote_id=pending_cancel.quote_id)
        remote = _find_remote_quote(remote_quotes, quote_id=pending_cancel.quote_id)
        if remote is None:
            raise exc

        resolved = _quote_from_remote(pending_cancel, remote)
        self._store(resolved)
        if resolved.stage is QuoteStage.OPEN:
            raise exc
        logger.info(
            "Verified quote %s resolved to exchange state %s",
            pending_cancel.quote_id,
            resolved.stage,
        )
        return resolved

    def _store(self, quote: Quote) -> None:
        self._by_request[quote.request_id] = quote
        if quote.quote_id:
            self._by_quote_id[quote.quote_id] = quote

    def _mirror_remote_quote(self, quote: Quote, remote: QuotePayload) -> Quote:
        resolved = _quote_from_remote(quote, remote)
        self._store(resolved)
        logger.info(
            "Reconciled quote %s to exchange state %s",
            resolved.quote_id,
            resolved.stage,
        )
        return resolved

    def _adopt_open_quote(self, quote: Quote, quote_id: str) -> Quote:
        opened = quote if quote.stage is QuoteStage.OPEN else quote.with_stage(QuoteStage.OPEN)
        opened = replace(opened, quote_id=quote_id)
        self._store(opened)
        logger.info("Adopted exchange quote %s for RFQ %s", quote_id, quote.request_id)
        return opened


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


def _find_remote_quote(
    remote_quotes: QuoteListSnapshot,
    request_id: str | None = None,
    quote_id: str | None = None,
) -> QuotePayload | None:
    for item in remote_quotes.payloads:
        if request_id is not None and item.request_id != request_id:
            continue
        if quote_id is not None and item.quote_id != quote_id:
            continue
        return item
    return None


def _find_salvaged_quote_id(remote_quotes: QuoteListSnapshot, request_id: str) -> str | None:
    for salvaged_request_id, quote_id in remote_quotes.malformed_id_pairs:
        if salvaged_request_id == request_id:
            return quote_id
    return None


def _quote_from_remote(current: Quote, payload: QuotePayload) -> Quote:
    stage = quote_stage_from_wire(payload.state)
    if stage is None:
        raise CoincallRequestError(
            f"Unknown quote state {payload.state!r} for quote {payload.quote_id}"
        )
    base = current if current.stage is stage else _with_remote_stage(current, stage)
    return replace(
        base,
        request_id=payload.request_id or current.request_id,
        quote_id=payload.quote_id,
        update_time_ms=_coalesce(payload.update_time, current.update_time_ms),
        expiry_time_ms=_coalesce(payload.expiry_time, current.expiry_time_ms),
        filled_price=_coalesce(payload.filled_price, current.filled_price),
        filled_quantity=_coalesce(payload.filled_quantity, current.filled_quantity),
        fill_time_ms=_coalesce(payload.fill_time, current.fill_time_ms),
        block_trade_id=payload.block_trade_id or current.block_trade_id,
    )


def _with_remote_stage(current: Quote, stage: QuoteStage) -> Quote:
    try:
        return current.with_stage(stage)
    except IllegalQuoteTransition:
        if current.stage is QuoteStage.PENDING_CREATE and stage is QuoteStage.FILLED:
            return current.with_stage(QuoteStage.OPEN).with_stage(QuoteStage.FILLED)
        raise
