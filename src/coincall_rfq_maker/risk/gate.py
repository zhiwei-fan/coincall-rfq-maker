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
import math
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Protocol

from coincall_rfq_maker.domain.rfq import Rfq
from coincall_rfq_maker.quoting.strategy import QuoteIntent

logger = logging.getLogger(__name__)

DEFAULT_KILL_SWITCH_THRESHOLD = 5
KillSwitchTripHandler = Callable[[], None]


def _finite_positive(value: str) -> float | None:
    """Return a value only when it proves to be finite and strictly positive."""
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) and parsed > 0 else None


def _validated_finite_positive_config(name: str, value: float) -> float:
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
        or value <= 0
    ):
        raise ValueError(f"{name} must be finite and > 0")
    return float(value)


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
        self._max_notional = _validated_finite_positive_config(
            "max_quote_notional_usd", max_quote_notional_usd
        )
        self._max_leg_qty = _validated_finite_positive_config("max_leg_qty", max_leg_qty)
        self._min_tte_hours = _validated_finite_positive_config(
            "min_time_to_expiry_hours", min_time_to_expiry_hours
        )
        self._stale_seconds = _validated_finite_positive_config(
            "stale_market_data_seconds", stale_market_data_seconds
        )
        if not isinstance(kill_switch_threshold, int) or isinstance(kill_switch_threshold, bool):
            raise ValueError("kill_switch_threshold must be an int >= 1")
        if kill_switch_threshold < 1:
            raise ValueError("kill_switch_threshold must be an int >= 1")
        self._kill_switch_threshold = kill_switch_threshold
        self._on_trip = on_trip
        self._exposure_provider = (
            exposure_provider if exposure_provider is not None else NullExposureProvider()
        )
        self._consecutive_failures = 0
        # Monotonic; used to detect whether a reconcile cycle recorded any persistent failure.
        self._failures_total = 0
        self._kill_switch_tripped = False

    @property
    def kill_switch_tripped(self) -> bool:
        return self._kill_switch_tripped

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    @property
    def failures_total(self) -> int:
        return self._failures_total

    def record_api_failure(self) -> None:
        self._consecutive_failures += 1
        self._failures_total += 1
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

    def trip_kill_switch(self, reason: str) -> None:
        """Trip for an escalation that is not countered by consecutive API failures."""
        if self._kill_switch_tripped:
            return
        self._kill_switch_tripped = True
        logger.error("Kill switch TRIPPED: %s", reason)
        if self._on_trip is not None:
            self._on_trip()

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

        quantities: list[float] = []
        for leg in rfq.legs:
            # Wire schemas validate this too, but the gate is the final control and must
            # hold even when a caller constructs an Rfq without using those schemas.
            quantity = _finite_positive(leg.quantity)
            if quantity is None:
                return self._reject(
                    f"leg {leg.instrument_name} has invalid quantity {leg.quantity!r}"
                )
            if quantity > self._max_leg_qty:
                return self._reject(
                    f"leg {leg.instrument_name} quantity {quantity} exceeds max {self._max_leg_qty}"
                )
            quantities.append(quantity)

        prices_by_instrument: dict[str, float] = {}
        for intent_leg in intent.legs:
            price = _finite_positive(str(intent_leg.price))
            if price is None:
                return self._reject(
                    f"leg {intent_leg.instrument_name} has invalid price {intent_leg.price!r}"
                )
            prices_by_instrument[intent_leg.instrument_name] = price
        notional = 0.0
        for leg, quantity in zip(rfq.legs, quantities, strict=True):
            price = prices_by_instrument.get(leg.instrument_name)
            if price is None:
                return self._reject(f"missing quote price for leg {leg.instrument_name}")
            notional += quantity * price
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
