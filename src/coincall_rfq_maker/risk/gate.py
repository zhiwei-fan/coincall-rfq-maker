"""Pre-trade risk gate.

Fail-closed: any check that cannot be positively confirmed (missing price,
malformed quantity, an internal error) results in rejection, never approval.
Every rejection is logged with its reason. A kill switch trips after repeated
consecutive PERSISTENT API failures; on trip it fires `on_trip`, which the CLI
wires to a one-shot flatten (cancel every live quote), and thereafter rejects
everything for the life of the process. There is no runtime reset: recovery
means a restart.

Only PERSISTENT failures count. Transient and ambiguous outcomes are the outage
cooldown's business, not the kill switch's.
"""

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Protocol

from coincall_rfq_maker.domain.rfq import Rfq
from coincall_rfq_maker.quoting.strategy import QuoteIntent

logger = logging.getLogger(__name__)

DEFAULT_KILL_SWITCH_THRESHOLD = 5
KillSwitchTripHandler = Callable[[], None]


@dataclass(frozen=True, slots=True)
class RiskDecision:
    approved: bool
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class ExposureSnapshot:
    # Seam for future position/greek limits; today it only carries provider
    # health. The fail-closed check mirrors the stale-market-data pattern.
    usable: bool = True
    reason: str | None = None


class ExposureProvider(Protocol):
    def current_exposure(self) -> ExposureSnapshot: ...


class NullExposureProvider:
    def current_exposure(self) -> ExposureSnapshot:
        return ExposureSnapshot()


class RiskGate:
    def __init__(
        self,
        max_quote_notional_usd: float,
        max_leg_qty: float,
        min_time_to_expiry_hours: float,
        stale_market_data_seconds: float,
        kill_switch_threshold: int = DEFAULT_KILL_SWITCH_THRESHOLD,
        exposure_provider: ExposureProvider | None = None,
        on_trip: KillSwitchTripHandler | None = None,
    ) -> None:
        self._max_notional = max_quote_notional_usd
        self._max_leg_qty = max_leg_qty
        self._min_tte_hours = min_time_to_expiry_hours
        self._stale_seconds = stale_market_data_seconds
        self._kill_switch_threshold = kill_switch_threshold
        self._on_trip = on_trip
        self._exposure_provider = (
            exposure_provider if exposure_provider is not None else NullExposureProvider()
        )
        self._consecutive_failures = 0
        self._kill_switch_tripped = False

    @property
    def kill_switch_tripped(self) -> bool:
        return self._kill_switch_tripped

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    def record_api_failure(self) -> None:
        self._consecutive_failures += 1
        already_tripped = self._kill_switch_tripped
        if self._consecutive_failures >= self._kill_switch_threshold and not already_tripped:
            self._kill_switch_tripped = True
            logger.error(
                "Kill switch TRIPPED after %d consecutive API failures", self._consecutive_failures
            )
            if self._on_trip is not None:
                self._on_trip()

    def record_api_success(self) -> None:
        self._consecutive_failures = 0

    def reset_kill_switch(self) -> None:
        self._kill_switch_tripped = False
        self._consecutive_failures = 0

    def evaluate(
        self,
        rfq: Rfq,
        intent: QuoteIntent,
        market_data_ages_seconds: Mapping[str, float],
        now_ms: int,
    ) -> RiskDecision:
        try:
            return self._evaluate(rfq, intent, market_data_ages_seconds, now_ms)
        except Exception as exc:  # fail closed on any unexpected error
            return self._reject(f"risk evaluation error: {exc}")

    def _evaluate(
        self,
        rfq: Rfq,
        intent: QuoteIntent,
        market_data_ages_seconds: Mapping[str, float],
        now_ms: int,
    ) -> RiskDecision:
        if self._kill_switch_tripped:
            return self._reject("kill switch tripped")

        exposure = self._exposure_provider.current_exposure()
        if not exposure.usable:
            reason = exposure.reason or "unusable or stale exposure data"
            return self._reject(f"exposure data unavailable: {reason}")

        time_to_expiry_hours = (rfq.expiry_time_ms - now_ms) / 1000 / 3600
        if time_to_expiry_hours < self._min_tte_hours:
            return self._reject(
                f"RFQ {rfq.request_id} time-to-expiry {time_to_expiry_hours:.2f}h "
                f"below minimum {self._min_tte_hours}h"
            )

        for leg in rfq.legs:
            quantity = float(leg.quantity)
            if quantity > self._max_leg_qty:
                return self._reject(
                    f"leg {leg.instrument_name} quantity {quantity} exceeds max {self._max_leg_qty}"
                )

        prices_by_instrument = {leg.instrument_name: leg.price for leg in intent.legs}
        notional = 0.0
        for leg in rfq.legs:
            price = prices_by_instrument.get(leg.instrument_name)
            if price is None:
                return self._reject(f"missing quote price for leg {leg.instrument_name}")
            notional += float(leg.quantity) * price
        if notional > self._max_notional:
            return self._reject(f"quote notional {notional:.2f} exceeds max {self._max_notional}")

        checked_instruments: set[str] = set()
        for intent_leg in intent.legs:
            if intent_leg.instrument_name in checked_instruments:
                continue
            checked_instruments.add(intent_leg.instrument_name)
            age = market_data_ages_seconds.get(intent_leg.instrument_name)
            if age is None:
                return self._reject(f"missing market data age for {intent_leg.instrument_name}")
            if age > self._stale_seconds:
                return self._reject(
                    f"market data for {intent_leg.instrument_name} stale ({age:.1f}s)"
                )

        return RiskDecision(approved=True)

    @staticmethod
    def _reject(reason: str) -> RiskDecision:
        logger.warning("Risk gate rejected quote: %s", reason)
        return RiskDecision(approved=False, reason=reason)
