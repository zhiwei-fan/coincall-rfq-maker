"""Quote indexes owned by the quote lifecycle actor."""

from coincall_rfq_maker.domain.quote import Quote


class QuoteStore:
    def __init__(self) -> None:
        self._by_request: dict[str, Quote] = {}
        self._by_quote_id: dict[str, Quote] = {}

    def get_for_rfq(self, request_id: str) -> Quote | None:
        return self._by_request.get(request_id)

    def get_by_quote_id(self, quote_id: str) -> Quote | None:
        return self._by_quote_id.get(quote_id)

    def open_quotes(self) -> list[Quote]:
        return [quote for quote in self._by_request.values() if quote.is_open]

    def store(self, quote: Quote) -> None:
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
