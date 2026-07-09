"""Pre-trade risk gate.

Fail-closed: any check that cannot be positively confirmed (missing price,
malformed quantity, an internal error) results in rejection, never approval.
Every rejection is logged with its reason. A kill switch trips after
repeated consecutive API failures and rejects everything until reset.
"""

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from coincall_rfq_maker.domain.rfq import Rfq
from coincall_rfq_maker.quoting.strategy import QuoteIntent

logger = logging.getLogger(__name__)

DEFAULT_KILL_SWITCH_THRESHOLD = 5


@dataclass(frozen=True, slots=True)
class RiskDecision:
    approved: bool
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class UnderlyingExposure:
    net_position_quantity: float = 0.0
    premium_notional: float = 0.0


@dataclass(frozen=True, slots=True)
class ExposureSnapshot:
    exposures_by_underlying: Mapping[str, UnderlyingExposure]
    usable: bool = True
    reason: str | None = None


class ExposureProvider(Protocol):
    def current_exposure(self) -> ExposureSnapshot: ...


class NullExposureProvider:
    def current_exposure(self) -> ExposureSnapshot:
        return ExposureSnapshot(exposures_by_underlying={})


@dataclass(frozen=True, slots=True)
class _RiskEvaluationContext:
    exposure: ExposureSnapshot


class RiskGate:
    def __init__(
        self,
        max_quote_notional_usd: float,
        max_leg_qty: float,
        min_time_to_expiry_hours: float,
        stale_market_data_seconds: float,
        kill_switch_threshold: int = DEFAULT_KILL_SWITCH_THRESHOLD,
        exposure_provider: ExposureProvider | None = None,
    ) -> None:
        self._max_notional = max_quote_notional_usd
        self._max_leg_qty = max_leg_qty
        self._min_tte_hours = min_time_to_expiry_hours
        self._stale_seconds = stale_market_data_seconds
        self._kill_switch_threshold = kill_switch_threshold
        self._exposure_provider = (
            exposure_provider if exposure_provider is not None else NullExposureProvider()
        )
        self._consecutive_failures = 0
        self._kill_switch_tripped = False

    @property
    def kill_switch_tripped(self) -> bool:
        return self._kill_switch_tripped

    def record_api_failure(self) -> None:
        self._consecutive_failures += 1
        already_tripped = self._kill_switch_tripped
        if self._consecutive_failures >= self._kill_switch_threshold and not already_tripped:
            self._kill_switch_tripped = True
            logger.error(
                "Kill switch TRIPPED after %d consecutive API failures", self._consecutive_failures
            )

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

        context = _RiskEvaluationContext(exposure=self._exposure_provider.current_exposure())
        if not context.exposure.usable:
            reason = context.exposure.reason or "unusable or stale exposure data"
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

        for instrument_name, age in market_data_ages_seconds.items():
            if age > self._stale_seconds:
                return self._reject(f"market data for {instrument_name} stale ({age:.1f}s)")

        return RiskDecision(approved=True)

    @staticmethod
    def _reject(reason: str) -> RiskDecision:
        logger.warning("Risk gate rejected quote: %s", reason)
        return RiskDecision(approved=False, reason=reason)
