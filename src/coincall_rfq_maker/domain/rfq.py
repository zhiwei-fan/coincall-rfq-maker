"""RFQ domain model and its explicit processing-stage state machine.

`RfqStatus` mirrors the exchange's wire states exactly (ACTIVE, CANCELLED,
EXPIRED, FILLED, TRADED_AWAY). `RfqStage` is our *local* processing pipeline
stage (received -> priced -> quoted -> terminal) used to decide what work is
still owed on an RFQ; it is independent of the exchange status.
"""

from dataclasses import dataclass, field, replace
from enum import StrEnum


class RfqStatus(StrEnum):
    """Exchange-reported RFQ state (wire values, ported exactly)."""

    ACTIVE = "ACTIVE"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"
    FILLED = "FILLED"
    TRADED_AWAY = "TRADED_AWAY"


_TERMINAL_STATUSES = frozenset(
    {RfqStatus.CANCELLED, RfqStatus.EXPIRED, RfqStatus.FILLED, RfqStatus.TRADED_AWAY}
)


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class RfqStage(StrEnum):
    """Local processing pipeline stage for an RFQ."""

    RECEIVED = "received"
    PRICED = "priced"
    QUOTED = "quoted"
    TERMINAL = "terminal"


_ALLOWED_STAGE_TRANSITIONS: dict[RfqStage, frozenset[RfqStage]] = {
    RfqStage.RECEIVED: frozenset({RfqStage.PRICED, RfqStage.TERMINAL}),
    RfqStage.PRICED: frozenset({RfqStage.PRICED, RfqStage.QUOTED, RfqStage.TERMINAL}),
    RfqStage.QUOTED: frozenset({RfqStage.PRICED, RfqStage.QUOTED, RfqStage.TERMINAL}),
    RfqStage.TERMINAL: frozenset(),
}


class IllegalRfqTransition(ValueError):
    def __init__(self, current: RfqStage, target: RfqStage) -> None:
        super().__init__(f"Illegal RFQ stage transition: {current} -> {target}")
        self.current = current
        self.target = target


@dataclass(frozen=True, slots=True)
class RfqLeg:
    instrument_name: str
    side: Side
    quantity: str


@dataclass(frozen=True, slots=True)
class Rfq:
    """A single request-for-quote, as tracked locally."""

    request_id: str
    status: RfqStatus
    legs: tuple[RfqLeg, ...]
    create_time_ms: int
    expiry_time_ms: int
    stage: RfqStage = RfqStage.RECEIVED
    taker_name: str | None = None
    counterparty: str | None = None
    last_update_time_ms: int | None = field(default=None)

    @property
    def is_terminal_status(self) -> bool:
        return self.status in _TERMINAL_STATUSES

    def with_stage(self, target: RfqStage) -> "Rfq":
        """Return a copy transitioned to `target`, or raise if illegal."""
        if target not in _ALLOWED_STAGE_TRANSITIONS[self.stage]:
            raise IllegalRfqTransition(self.stage, target)
        return replace(self, stage=target)

    def with_status(self, status: RfqStatus, last_update_time_ms: int) -> "Rfq":
        """Apply a new exchange-reported status (and terminal stage if applicable)."""
        updated = replace(self, status=status, last_update_time_ms=last_update_time_ms)
        if status in _TERMINAL_STATUSES and updated.stage is not RfqStage.TERMINAL:
            updated = updated.with_stage(RfqStage.TERMINAL)
        return updated

    def instrument_names(self) -> tuple[str, ...]:
        return tuple(leg.instrument_name for leg in self.legs)
