"""Quote domain model and its state machine.

Defined exactly once (the old code duplicated Quote/QuoteLeg/QuoteState
between `quote_manager.py` and `models.py` — never again).
"""

from dataclasses import dataclass, replace
from enum import StrEnum

from coincall_rfq_maker.domain.rfq import Side


class QuoteStage(StrEnum):
    """Local quote lifecycle, from submission intent through to a terminal state."""

    PENDING_CREATE = "pending_create"
    OPEN = "open"
    PENDING_CANCEL = "pending_cancel"
    CANCELLED = "cancelled"
    FILLED = "filled"
    EXPIRED = "expired"


_TERMINAL_QUOTE_STAGES = frozenset({QuoteStage.CANCELLED, QuoteStage.FILLED, QuoteStage.EXPIRED})

_ALLOWED_QUOTE_TRANSITIONS: dict[QuoteStage, frozenset[QuoteStage]] = {
    QuoteStage.PENDING_CREATE: frozenset(
        {QuoteStage.OPEN, QuoteStage.CANCELLED, QuoteStage.EXPIRED}
    ),
    QuoteStage.OPEN: frozenset(
        {QuoteStage.PENDING_CANCEL, QuoteStage.FILLED, QuoteStage.CANCELLED, QuoteStage.EXPIRED}
    ),
    QuoteStage.PENDING_CANCEL: frozenset(
        {QuoteStage.CANCELLED, QuoteStage.FILLED, QuoteStage.EXPIRED, QuoteStage.OPEN}
    ),
    QuoteStage.CANCELLED: frozenset(),
    QuoteStage.FILLED: frozenset(),
    QuoteStage.EXPIRED: frozenset(),
}


class IllegalQuoteTransition(ValueError):
    def __init__(self, current: QuoteStage, target: QuoteStage) -> None:
        super().__init__(f"Illegal quote stage transition: {current} -> {target}")
        self.current = current
        self.target = target


@dataclass(frozen=True, slots=True)
class QuoteLeg:
    instrument_name: str
    price: float
    side: Side | None = None
    quantity: str | None = None


@dataclass(frozen=True, slots=True)
class Quote:
    """A maker quote against one RFQ. `quote_id` is unset until the exchange assigns one."""

    request_id: str
    stage: QuoteStage
    legs: tuple[QuoteLeg, ...]
    create_time_ms: int
    quote_id: str | None = None
    update_time_ms: int | None = None
    expiry_time_ms: int | None = None
    filled_price: float | None = None
    filled_quantity: float | None = None
    fill_time_ms: int | None = None
    block_trade_id: str | None = None

    @property
    def is_open(self) -> bool:
        return self.stage is QuoteStage.OPEN

    @property
    def is_terminal(self) -> bool:
        return self.stage in _TERMINAL_QUOTE_STAGES

    def with_stage(self, target: QuoteStage) -> "Quote":
        if target not in _ALLOWED_QUOTE_TRANSITIONS[self.stage]:
            raise IllegalQuoteTransition(self.stage, target)
        return replace(self, stage=target)
