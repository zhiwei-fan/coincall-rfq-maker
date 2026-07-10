"""Exchange-validated option expiry metadata cache."""

import logging
import time
from collections.abc import Callable

from coincall_rfq_maker.core.adapters.rest import CoincallRestClient
from coincall_rfq_maker.domain.instruments import InstrumentParseError, parse_instrument

logger = logging.getLogger(__name__)

NEGATIVE_FETCH_COOLDOWN_SECONDS = 30.0


class InstrumentCatalog:
    """Cache immutable expiries; single-writer dispatcher use requires no locking."""

    def __init__(
        self,
        rest_client: CoincallRestClient,
        negative_cache_seconds: float = NEGATIVE_FETCH_COOLDOWN_SECONDS,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._rest = rest_client
        self._negative_cache_seconds = negative_cache_seconds
        self._monotonic = monotonic
        self._expirations: dict[str, int] = {}
        self._negative_fetches: dict[str, float] = {}

    async def expiration_ms(self, symbol: str) -> int | None:
        """Return the exchange expiry for `symbol`, fetching its base catalog on a miss."""
        cached = self._expirations.get(symbol)
        if cached is not None:
            return cached

        try:
            parsed = parse_instrument(symbol)
        except InstrumentParseError:
            logger.warning("Cannot resolve expiry for unparseable instrument %s", symbol)
            return None

        base = parsed.underlying.removesuffix("USD")
        if base == parsed.underlying:
            logger.warning(
                "Cannot map instrument underlying %s to an option base", parsed.underlying
            )
            return None

        now = self._monotonic()
        last_failure = self._negative_fetches.get(base)
        if last_failure is not None and now - last_failure < self._negative_cache_seconds:
            return None

        try:
            instruments = await self._rest.get_option_instruments(base)
        except Exception:
            logger.exception("Failed to fetch option instruments for %s", base)
            self._negative_fetches[base] = self._monotonic()
            return None

        for instrument in instruments:
            self._expirations[instrument.symbol_name] = instrument.expiration_timestamp

        expiration = self._expirations.get(symbol)
        if expiration is None:
            self._negative_fetches[base] = self._monotonic()
        return expiration
