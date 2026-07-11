"""Quote indexes owned by the quote lifecycle actor."""

import logging

from coincall_rfq_maker.domain.quote import Quote

logger = logging.getLogger(__name__)


class QuoteStore:
    """Monotonic quote reducer: new generations move the current pointer; historical IDs do not."""

    def __init__(self) -> None:
        self._by_request: dict[str, Quote] = {}
        self._by_quote_id: dict[str, Quote] = {}

    def get_for_rfq(self, request_id: str) -> Quote | None:
        return self._by_request.get(request_id)

    def get_by_quote_id(self, quote_id: str) -> Quote | None:
        return self._by_quote_id.get(quote_id)

    def open_quotes(self) -> list[Quote]:
        return [quote for quote in self._by_request.values() if quote.is_open]

    def non_terminal_quotes(self) -> list[Quote]:
        return [quote for quote in self._by_request.values() if not quote.is_terminal]

    def store(self, quote: Quote) -> None:
        """Store a quote without allowing stale observations to move state backward."""
        current = self._by_request.get(quote.request_id)
        known = bool(quote.quote_id) and quote.quote_id in self._by_quote_id

        if (
            current is not None
            and quote.quote_id
            and current.quote_id
            and quote.quote_id != current.quote_id
            and known
        ):
            historical = self._by_quote_id[quote.quote_id]
            if historical.is_terminal and not quote.is_terminal:
                logger.warning(
                    "refusing to reopen terminal quote %s from a stale observation", quote.quote_id
                )
                return
            self._by_quote_id[quote.quote_id] = quote
            return

        if (
            current is not None
            and quote.quote_id == current.quote_id
            and current.is_terminal
            and not quote.is_terminal
        ):
            logger.warning(
                "refusing to reopen terminal quote %s from a stale observation", quote.quote_id
            )
            return

        self._by_request[quote.request_id] = quote
        if quote.quote_id:
            self._by_quote_id[quote.quote_id] = quote

    def evict_for_rfq(self, request_id: str) -> None:
        self._by_request.pop(request_id, None)
        stale_quote_ids = [
            quote_id
            for quote_id, quote in self._by_quote_id.items()
            if quote.request_id == request_id
        ]
        for quote_id in stale_quote_ids:
            del self._by_quote_id[quote_id]
