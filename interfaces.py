"""
Abstract interfaces for RFQ Market Maker components.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Set
from datetime import datetime

from models import (
    RFQ, Quote, Instrument, PricedInstrument, 
    PriceQuote, MarketData, QuoteLeg
)


# ============================================================================
# Pricing Interfaces
# ============================================================================

class IPricingService(ABC):
    """Interface for pricing operations"""
    
    @abstractmethod
    async def calculate_price(self, instrument_name: str) -> Optional[PriceQuote]:
        """Calculate current price for an instrument"""
        pass
    
    @abstractmethod
    async def update_market_data(self) -> int:
        """Update market data for all instruments. Returns number updated."""
        pass
    
    @abstractmethod
    def get_instrument(self, instrument_name: str) -> Optional[PricedInstrument]:
        """Get instrument with current pricing"""
        pass


class IInstrumentRepository(ABC):
    """Interface for instrument storage and retrieval"""
    
    @abstractmethod
    def add_instrument(self, instrument_name: str) -> bool:
        """Add instrument by name. Returns success."""
        pass
    
    @abstractmethod
    def get_instrument(self, instrument_name: str) -> Optional[Instrument]:
        """Get instrument definition"""
        pass
    
    @abstractmethod
    def get_all_instruments(self) -> List[Instrument]:
        """Get all instruments"""
        pass
    
    @abstractmethod
    def remove_instrument(self, instrument_name: str) -> bool:
        """Remove instrument. Returns success."""
        pass


class IMarketDataProvider(ABC):
    """Interface for market data operations"""
    
    @abstractmethod
    async def fetch_index_prices(self, symbols: List[str]) -> Dict[str, float]:
        """Fetch latest index prices for symbols"""
        pass
    
    @abstractmethod
    def get_cached_price(self, symbol: str) -> Optional[float]:
        """Get cached price for symbol"""
        pass


# ============================================================================
# RFQ Interfaces
# ============================================================================

class IRFQRepository(ABC):
    """Interface for RFQ storage and retrieval"""
    
    @abstractmethod
    def get_active_rfqs(self) -> List[RFQ]:
        """Get all active RFQs"""
        pass
    
    @abstractmethod
    def get_rfq(self, rfq_id: str) -> Optional[RFQ]:
        """Get specific RFQ by ID"""
        pass
    
    @abstractmethod
    def add_rfq(self, rfq: RFQ) -> bool:
        """Add or update RFQ. Returns success."""
        pass
    
    @abstractmethod
    def remove_rfq(self, rfq_id: str) -> bool:
        """Remove RFQ. Returns success."""
        pass
    
    @abstractmethod
    def get_rfqs_expiring_soon(self, seconds: int = 60) -> List[RFQ]:
        """Get RFQs expiring within specified seconds"""
        pass


class IRFQService(ABC):
    """Interface for RFQ business operations"""
    
    @abstractmethod
    async def process_websocket_message(self, message: dict) -> None:
        """Process RFQ update from websocket"""
        pass
    
    @abstractmethod
    async def sync_with_api(self) -> Dict[str, int]:
        """Sync RFQs with API. Returns sync stats."""
        pass


# ============================================================================
# Quote Interfaces
# ============================================================================

class IQuoteService(ABC):
    """Interface for quote operations"""
    
    @abstractmethod
    async def create_quote(self, rfq_id: str, legs: List[dict]) -> Optional[Quote]:
        """Create quote for RFQ"""
        pass
    
    @abstractmethod
    async def cancel_quote(self, quote_id: str) -> bool:
        """Cancel specific quote. Returns success."""
        pass
    
    @abstractmethod
    async def cancel_rfq_quotes(self, rfq_id: str) -> int:
        """Cancel all quotes for RFQ. Returns count cancelled."""
        pass
    
    @abstractmethod
    def get_quote(self, quote_id: str) -> Optional[Quote]:
        """Get specific quote"""
        pass
    
    @abstractmethod
    def get_rfq_quotes(self, rfq_id: str) -> List[Quote]:
        """Get all quotes for an RFQ"""
        pass


class IQuoteRepository(ABC):
    """Interface for quote storage"""
    
    @abstractmethod
    def add_quote(self, quote: Quote) -> bool:
        """Store quote. Returns success."""
        pass
    
    @abstractmethod
    def get_quote(self, quote_id: str) -> Optional[Quote]:
        """Retrieve quote by ID"""
        pass
    
    @abstractmethod
    def get_open_quotes(self) -> List[Quote]:
        """Get all open quotes"""
        pass
    
    @abstractmethod
    def update_quote_state(self, quote_id: str, state: str) -> bool:
        """Update quote state. Returns success."""
        pass


# ============================================================================
# Orchestration Interfaces
# ============================================================================

class IQuoteOrchestrator(ABC):
    """Interface for quote creation orchestration"""
    
    @abstractmethod
    async def should_quote_rfq(self, rfq: RFQ) -> bool:
        """Determine if RFQ should be quoted"""
        pass
    
    @abstractmethod
    async def create_quote_for_rfq(self, rfq: RFQ) -> Optional[Quote]:
        """Orchestrate quote creation for RFQ"""
        pass
    
    @abstractmethod
    async def update_quotes_for_rfq(self, rfq: RFQ) -> int:
        """Update existing quotes for RFQ. Returns count updated."""
        pass


class IRFQInstrumentTracker(ABC):
    """Interface for tracking RFQ to instrument relationships"""
    
    @abstractmethod
    def track_rfq(self, rfq: RFQ) -> None:
        """Track instruments for an RFQ"""
        pass
    
    @abstractmethod
    def untrack_rfq(self, rfq_id: str) -> None:
        """Stop tracking instruments for an RFQ"""
        pass
    
    @abstractmethod
    def get_rfq_instruments(self, rfq_id: str) -> Set[str]:
        """Get instruments for an RFQ"""
        pass
    
    @abstractmethod
    def get_instrument_rfqs(self, instrument: str) -> Set[str]:
        """Get RFQs using an instrument"""
        pass


# ============================================================================
# Event Bus Interface (for future event-driven architecture)
# ============================================================================

class IEventBus(ABC):
    """Interface for event publishing and subscription"""
    
    @abstractmethod
    def subscribe(self, event_type: str, handler) -> None:
        """Subscribe to event type"""
        pass
    
    @abstractmethod
    async def publish(self, event_type: str, data: any) -> None:
        """Publish event"""
        pass
    
    @abstractmethod
    def unsubscribe(self, event_type: str, handler) -> None:
        """Unsubscribe from event type"""
        pass


# ============================================================================
# Factory Interface
# ============================================================================

class IComponentFactory(ABC):
    """Abstract factory for creating components"""
    
    @abstractmethod
    def create_pricing_service(self) -> IPricingService:
        """Create pricing service instance"""
        pass
    
    @abstractmethod
    def create_rfq_repository(self) -> IRFQRepository:
        """Create RFQ repository instance"""
        pass
    
    @abstractmethod
    def create_quote_service(self) -> IQuoteService:
        """Create quote service instance"""
        pass