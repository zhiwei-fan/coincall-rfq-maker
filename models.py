"""
Data models for RFQ Market Maker.
Pure data classes with no business logic, following clean architecture principles.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Set


# ============================================================================
# Enums
# ============================================================================

class RFQState(Enum):
    """RFQ States"""
    ACTIVE = "ACTIVE"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"
    FILLED = "FILLED"
    TRADED_AWAY = "TRADED_AWAY"


class QuoteState(Enum):
    """Quote States"""
    OPEN = "OPEN"
    CANCELLED = "CANCELLED"
    FILLED = "FILLED"
    EXPIRED = "EXPIRED"


# ============================================================================
# Instrument Models
# ============================================================================

@dataclass(frozen=True)
class Instrument:
    """
    Immutable option instrument definition.
    Pure data model with no business logic.
    """
    instrument_str: str
    underlying: str
    expiry_date: datetime
    strike: float
    option_type: str  # 'C' or 'P'


@dataclass
class PricedInstrument:
    """
    Instrument with pricing data.
    Mutable to allow price updates.
    """
    instrument: Instrument
    index_price: Optional[float] = None
    bid_price: Optional[float] = None
    ask_price: Optional[float] = None
    last_priced: Optional[datetime] = None
    
    @property
    def mid_price(self) -> Optional[float]:
        """Calculate mid price from bid and ask"""
        if self.bid_price is not None and self.ask_price is not None:
            return (self.bid_price + self.ask_price) / 2
        return None
    
    @property
    def spread(self) -> Optional[float]:
        """Calculate spread between bid and ask"""
        if self.bid_price is not None and self.ask_price is not None:
            return self.ask_price - self.bid_price
        return None
    
    @property
    def is_priced(self) -> bool:
        """Check if instrument has been priced"""
        return self.last_priced is not None


# ============================================================================
# RFQ Models
# ============================================================================

@dataclass(frozen=True)
class RFQLeg:
    """Immutable RFQ leg representation"""
    instrumentName: str
    side: str  # 'BUY' or 'SELL'
    quantity: str


@dataclass
class RFQ:
    """
    Request for Quote data model.
    Contains minimal business logic (properties only).
    """
    requestId: str
    state: RFQState
    legs: List[RFQLeg]
    createTime: int
    expiryTime: int
    takerName: Optional[str] = None
    counterparty: Optional[str] = None
    lastUpdateTime: Optional[int] = None
    quoteIds: Set[str] = field(default_factory=set)
    raw_data: dict = field(default_factory=dict)


# ============================================================================
# Quote Models
# ============================================================================

@dataclass(frozen=True)
class QuoteLeg:
    """Immutable quote leg representation"""
    instrumentName: str
    price: str
    side: Optional[str] = None
    quantity: Optional[str] = None


@dataclass
class Quote:
    """
    Quote data model.
    Contains minimal business logic (properties only).
    """
    quoteId: str
    requestId: str  # Associated RFQ ID
    state: QuoteState
    legs: List[QuoteLeg]
    createTime: int
    updateTime: Optional[int] = None
    expiryTime: Optional[int] = None
    
    # Fill details (when quote is filled)
    filledPrice: Optional[float] = None
    filledQuantity: Optional[float] = None
    fillTime: Optional[int] = None
    blockTradeId: Optional[str] = None
    
    # Metadata
    raw_data: dict = field(default_factory=dict)
    
    @property
    def is_open(self) -> bool:
        """Check if quote is open"""
        return self.state == QuoteState.OPEN
    
    @property
    def is_terminal(self) -> bool:
        """Check if quote is in terminal state"""
        return self.state in [QuoteState.FILLED, QuoteState.CANCELLED, QuoteState.EXPIRED]


# ============================================================================
# Price Models
# ============================================================================

@dataclass(frozen=True)
class PriceQuote:
    """Immutable price quote for an instrument"""
    instrument_name: str
    bid: float
    ask: float
    mid: float
    spread: float
    timestamp: datetime


@dataclass(frozen=True)
class MarketData:
    """Immutable market data snapshot"""
    symbol: str
    index_price: float
    volatility: float
    risk_free_rate: float
    timestamp: datetime


# ============================================================================
# Event Models (for future event-driven architecture)
# ============================================================================

@dataclass(frozen=True)
class RFQReceivedEvent:
    """Event emitted when RFQ is received"""
    rfq: RFQ
    source: str  # 'websocket' or 'api'
    timestamp: datetime


@dataclass(frozen=True)
class QuoteCreatedEvent:
    """Event emitted when quote is created"""
    quote: Quote
    rfq_id: str
    timestamp: datetime


@dataclass(frozen=True)
class PriceUpdateEvent:
    """Event emitted when prices are updated"""
    instruments: List[str]
    timestamp: datetime