import asyncio
import logging
from datetime import datetime
from typing import Dict, Optional, List, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
import time

from api_client import RfqAPI, CcAPIException

logger = logging.getLogger(__name__)


class QuoteState(Enum):
    """Quote States"""
    OPEN = "OPEN"
    CANCELLED = "CANCELLED"
    FILLED = "FILLED"
    EXPIRED = "EXPIRED"


@dataclass
class QuoteLeg:
    """Represents a leg in a Quote"""
    instrumentName: str
    price: str
    side: Optional[str] = None  # From RFQ context
    quantity: Optional[str] = None  # From RFQ context


@dataclass
class Quote:
    """Represents a Quote with all its details"""
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
        """Check if quote is in terminal state (filled, cancelled, expired)"""
        return self.state in [QuoteState.FILLED, QuoteState.CANCELLED, QuoteState.EXPIRED]
    
    @property
    def age_seconds(self) -> float:
        """Get age of quote in seconds"""
        return (int(time.time() * 1000) - self.createTime) / 1000
    
    @classmethod
    def from_api_data(cls, data: dict) -> "Quote":
        """Create Quote from API response data"""
        legs = []
        for leg_data in data.get("legs", []):
            legs.append(QuoteLeg(
                instrumentName=leg_data.get("instrumentName", ""),
                price=str(leg_data.get("price", "")),
                side=leg_data.get("side"),
                quantity=str(leg_data.get("quantity", "")) if leg_data.get("quantity") else None
            ))
        
        return cls(
            quoteId=data["quoteId"],
            requestId=data.get("requestId", ""),
            state=QuoteState(data.get("state", "OPEN")),
            legs=legs,
            createTime=data.get("createTime", int(time.time() * 1000)),
            updateTime=data.get("updateTime"),
            expiryTime=data.get("expiryTime"),
            filledPrice=data.get("filledPrice"),
            filledQuantity=data.get("filledQuantity"),
            fillTime=data.get("fillTime"),
            blockTradeId=data.get("blockTradeId"),
            raw_data=data
        )
    
    @classmethod
    def from_ws_data(cls, data: dict) -> "Quote":
        """Create Quote from WebSocket message data"""
        # WebSocket data structure might be slightly different
        legs = []
        for leg_data in data.get("legs", []):
            legs.append(QuoteLeg(
                instrumentName=leg_data.get("instrumentName", ""),
                price=str(leg_data.get("price", "")),
                side=leg_data.get("side"),
                quantity=str(leg_data.get("quantity", "")) if leg_data.get("quantity") else None
            ))
        
        return cls(
            quoteId=data["quoteId"],
            requestId=data.get("requestId", ""),
            state=QuoteState(data.get("state", "OPEN")),
            legs=legs,
            createTime=data.get("createTime", int(time.time() * 1000)),
            updateTime=data.get("updateTime", int(time.time() * 1000)),
            expiryTime=data.get("expiryTime"),
            filledPrice=data.get("filledPrice"),
            filledQuantity=data.get("filledQuantity"),
            fillTime=data.get("fillTime"),
            blockTradeId=data.get("blockTradeId"),
            raw_data=data
        )


class QuoteManager:
    """
    Manages Quotes with API operations and WebSocket updates
    
    Features:
    - Create and cancel quotes via API
    - Track quote state from WebSocket updates
    - Cancel all quotes on initialization
    - Periodic sync with API for consistency
    - Track quote statistics and performance
    """
    
    def __init__(self, api_client: RfqAPI, sync_interval: int = 60):
        """
        Initialize Quote Manager
        
        Args:
            api_client: RfqAPI client for API calls
            sync_interval: Seconds between API syncs (default 60)
        """
        self.api_client = api_client
        self.sync_interval = sync_interval
        self.quotes: Dict[str, Quote] = {}
        self.quotes_by_rfq: Dict[str, Set[str]] = {}  # requestId -> set of quoteIds
        self.last_sync_time = 0
        self._sync_task: Optional[asyncio.Task] = None
        self._running = False
        
        # Statistics
        self.stats = {
            "total_created": 0,
            "total_cancelled": 0,
            "total_filled": 0,
            "total_expired": 0,
            "sync_count": 0,
            "sync_errors": 0,
            "api_errors": 0,
            "last_sync_time": None,
            "initialization_cancelled": 0
        }
    
    async def initialize(self) -> Tuple[int, int]:
        """
        Initialize Quote Manager by cancelling all existing quotes
        
        Returns:
            Tuple of (cancelled_count, existing_count)
        """
        logger.info("Initializing Quote Manager - cancelling all existing quotes")
        
        try:
            # First, cancel all existing quotes
            await self.api_client.cancel_all_quotes()
            # cancel_response = await self.api_client.cancel_all_quotes()
            # cancelled_count = cancel_response.get("data", {}).get("cancelledCount", 0)
            # self.stats["initialization_cancelled"] = cancelled_count
            
            # if cancelled_count > 0:
            #     logger.info(f"Cancelled {cancelled_count} existing quotes during initialization")
            
            # Wait a moment for cancellations to process
            await asyncio.sleep(1)
            
            # Now check if there are any remaining open quotes (edge case handling)
            response = await self.api_client.get_quote_list(state="OPEN")
            quotes_data = response.get("data", [])
            
            if quotes_data:
                # This shouldn't happen, but handle edge case
                logger.warning(f"Found {len(quotes_data)} quotes still open after cancel-all")
                
                # Try to cancel them individually
                for quote_data in quotes_data:
                    quote_id = quote_data.get("quoteId")
                    if quote_id:
                        try:
                            await self.api_client.cancel_quote(quote_id)
                            logger.info(f"Individually cancelled quote {quote_id}")
                            # cancelled_count += 1
                        except Exception as e:
                            logger.error(f"Failed to cancel quote {quote_id}: {e}")
            
            self.last_sync_time = time.time()
            self.stats["last_sync_time"] = self.last_sync_time
            
            logger.info(f"Quote Manager initialized")
            
        except CcAPIException as e:
            logger.error(f"Failed to initialize Quote Manager: {e}")
            self.stats["api_errors"] += 1
            raise
        except Exception as e:
            logger.error(f"Unexpected error during initialization: {e}")
            raise
    
    async def create_quote(self, request_id: str, legs: List[dict]) -> Optional[Quote]:
        """
        Create a new quote for an RFQ
        
        Args:
            request_id: RFQ ID to quote for
            legs: List of dicts with instrumentName and price
        
        Returns:
            Quote object if successful, None otherwise
        """
        try:
            logger.info(f"Creating quote for RFQ {request_id} with {len(legs)} legs")
            logger.debug(f"Legs: {legs}")
            
            # Create quote via API
            response = await self.api_client.create_quote(request_id, legs)
            logger.debug(f"Raw Create Quote response {response}")
            quote_data = response.get("data", {})
            
            if not quote_data or not quote_data.get("quoteId"):
                logger.error(f"Invalid quote response: {response}")
                return None
            
            # Create Quote object
            quote = Quote.from_api_data(quote_data)
            
            # Store quote
            self.quotes[quote.quoteId] = quote
            
            # Track by RFQ
            if request_id not in self.quotes_by_rfq:
                self.quotes_by_rfq[request_id] = set()
            self.quotes_by_rfq[request_id].add(quote.quoteId)
            
            self.stats["total_created"] += 1
            
            logger.info(f"Created quote {quote.quoteId} for RFQ {request_id}")
            return quote
            
        except CcAPIException as e:
            logger.error(f"API error creating quote: {e}")
            self.stats["api_errors"] += 1
            return None
        except Exception as e:
            logger.error(f"Error creating quote: {e}")
            return None
    
    async def cancel_quote(self, quote_id: str) -> bool:
        """
        Cancel a specific quote
        
        Args:
            quote_id: Quote ID to cancel
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Cancel via API
            response = await self.api_client.cancel_quote(quote_id)
            
            # Update local state
            if quote_id in self.quotes:
                self.quotes[quote_id].state = QuoteState.CANCELLED
                self.quotes[quote_id].updateTime = int(time.time() * 1000)
                self.stats["total_cancelled"] += 1
            
            logger.info(f"Cancelled quote {quote_id}")
            return True
            
        except CcAPIException as e:
            logger.error(f"API error cancelling quote {quote_id}: {e}")
            self.stats["api_errors"] += 1
            return False
        except Exception as e:
            logger.error(f"Error cancelling quote {quote_id}: {e}")
            return False
    
    async def cancel_all_quotes(self) -> int:
        """
        Cancel all active quotes
        
        Returns:
            Number of quotes cancelled
        """
        try:
            response = await self.api_client.cancel_all_quotes()
            logger.debug(f"Cancelled all quotes: {response}")
            # cancelled_count = response.get("data", {}).get("cancelledCount", 0)
            
            # Update local state for all open quotes
            cancelled_count = 0
            for quote in self.quotes.values():
                if quote.is_open:
                    quote.state = QuoteState.CANCELLED
                    quote.updateTime = int(time.time() * 1000)
                    self.stats["total_cancelled"] += 1
                    cancelled_count += 1
            
            logger.info(f"Cancelled all quotes ({cancelled_count} cancelled)")
            return cancelled_count
            
        except CcAPIException as e:
            logger.error(f"API error cancelling all quotes: {e}")
            self.stats["api_errors"] += 1
            return 0
        except Exception as e:
            logger.error(f"Error cancelling all quotes: {e}")
            return 0
    
    async def cancel_rfq_quotes(self, request_id: str) -> int:
        """
        Cancel all quotes for a specific RFQ
        
        Args:
            request_id: RFQ ID
        
        Returns:
            Number of quotes cancelled
        """
        quote_ids = self.quotes_by_rfq.get(request_id, set())
        cancelled_count = 0
        
        for quote_id in quote_ids:
            quote = self.quotes.get(quote_id)
            if quote and quote.is_open:
                if await self.cancel_quote(quote_id):
                    cancelled_count += 1
        
        if cancelled_count > 0:
            logger.info(f"Cancelled {cancelled_count} quotes for RFQ {request_id}")
        
        return cancelled_count
    
    def handle_websocket_message(self, message: dict) -> None:
        """
        Process WebSocket message for quote updates
        
        Args:
            message: WebSocket message dict
        """
        try:
            # Extract quote data from message
            quote_data = message.get("d", {})
            if not quote_data:
                return
            
            quote_id = quote_data.get("quoteId")
            state = quote_data.get("state")
            
            if not quote_id:
                return
            
            logger.debug(f"Processing WebSocket quote {quote_id} with state {state}")
            
            # Update existing quote or create new one
            if quote_id in self.quotes:
                # Update existing quote
                quote = self.quotes[quote_id]
                old_state = quote.state
                
                if state:
                    quote.state = QuoteState(state)
                    quote.updateTime = int(time.time() * 1000)
                    
                    # Update statistics based on state change
                    if old_state == QuoteState.OPEN:
                        if quote.state == QuoteState.FILLED:
                            self.stats["total_filled"] += 1
                            quote.fillTime = quote_data.get("fillTime", int(time.time() * 1000))
                            quote.filledPrice = quote_data.get("filledPrice")
                            quote.filledQuantity = quote_data.get("filledQuantity")
                            quote.blockTradeId = quote_data.get("blockTradeId")
                            logger.info(f"Quote {quote_id} FILLED")
                        elif quote.state == QuoteState.CANCELLED:
                            self.stats["total_cancelled"] += 1
                            logger.info(f"Quote {quote_id} CANCELLED")
                        elif quote.state == QuoteState.EXPIRED:
                            self.stats["total_expired"] += 1
                            logger.info(f"Quote {quote_id} EXPIRED")
            else:
                # New quote from WebSocket
                quote = Quote.from_ws_data(quote_data)
                self.quotes[quote_id] = quote
                
                # Track by RFQ
                request_id = quote.requestId
                if request_id:
                    if request_id not in self.quotes_by_rfq:
                        self.quotes_by_rfq[request_id] = set()
                    self.quotes_by_rfq[request_id].add(quote_id)
                
                logger.info(f"Added new quote {quote_id} from WebSocket (state: {quote.state})")
                
        except Exception as e:
            logger.error(f"Error handling WebSocket message: {e}")
    
    async def sync_with_api(self) -> Dict[str, int]:
        """
        Synchronize quotes with API
        
        Returns:
            Dict with sync statistics
        """
        logger.debug("Starting API sync for quotes")
        sync_stats = {
            "added": 0,
            "updated": 0,
            "removed": 0,
            "errors": 0
        }
        
        try:
            # Get open quotes from API
            response = await self.api_client.get_quote_list(state="OPEN")
            api_quotes = {}
            quotes_data = response.get("data", [])
            
            for quote_data in quotes_data:
                quote = Quote.from_api_data(quote_data)
                api_quotes[quote.quoteId] = quote
            
            # Find quotes to add or update
            for quote_id, api_quote in api_quotes.items():
                if quote_id not in self.quotes:
                    # Add new quote
                    self.quotes[quote_id] = api_quote
                    
                    # Track by RFQ
                    if api_quote.requestId:
                        if api_quote.requestId not in self.quotes_by_rfq:
                            self.quotes_by_rfq[api_quote.requestId] = set()
                        self.quotes_by_rfq[api_quote.requestId].add(quote_id)
                    
                    sync_stats["added"] += 1
                    logger.info(f"Sync: Added missing quote {quote_id}")
                else:
                    # Update existing quote if API has newer data
                    local_quote = self.quotes[quote_id]
                    if api_quote.updateTime and local_quote.updateTime:
                        if api_quote.updateTime > local_quote.updateTime:
                            self.quotes[quote_id] = api_quote
                            sync_stats["updated"] += 1
            
            # Find local open quotes not in API (likely filled/cancelled)
            for quote_id, quote in list(self.quotes.items()):
                if quote.is_open and quote_id not in api_quotes:
                    # Mark as expired/cancelled
                    quote.state = QuoteState.EXPIRED
                    quote.updateTime = int(time.time() * 1000)
                    sync_stats["removed"] += 1
                    logger.info(f"Sync: Marked quote {quote_id} as expired (not in API)")
            
            self.last_sync_time = time.time()
            self.stats["last_sync_time"] = self.last_sync_time
            self.stats["sync_count"] += 1
            
            logger.info(f"Quote sync completed: added={sync_stats['added']}, "
                       f"updated={sync_stats['updated']}, "
                       f"removed={sync_stats['removed']}, "
                       f"total_open={len(api_quotes)}")
            
        except CcAPIException as e:
            logger.error(f"API sync failed: {e}")
            sync_stats["errors"] += 1
            self.stats["sync_errors"] += 1
            self.stats["api_errors"] += 1
        except Exception as e:
            logger.error(f"Unexpected error during sync: {e}")
            sync_stats["errors"] += 1
            self.stats["sync_errors"] += 1
        
        return sync_stats
    
    async def _periodic_sync_task(self):
        """Background task for periodic synchronization"""
        while self._running:
            try:
                await asyncio.sleep(self.sync_interval)
                
                # Clean up old terminal quotes
                self._cleanup_old_quotes()
                
                # Sync with API
                await self.sync_with_api()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic sync: {e}")
                await asyncio.sleep(10)  # Wait before retry
    
    def _cleanup_old_quotes(self, max_age_seconds: int = 3600):
        """
        Remove old quotes in terminal states
        
        Args:
            max_age_seconds: Maximum age for terminal quotes (default 1 hour)
        """
        to_remove = []
        
        for quote_id, quote in self.quotes.items():
            if quote.is_terminal and quote.age_seconds > max_age_seconds:
                to_remove.append(quote_id)
        
        for quote_id in to_remove:
            quote = self.quotes[quote_id]
            
            # Remove from RFQ tracking
            if quote.requestId in self.quotes_by_rfq:
                self.quotes_by_rfq[quote.requestId].discard(quote_id)
                if not self.quotes_by_rfq[quote.requestId]:
                    del self.quotes_by_rfq[quote.requestId]
            
            del self.quotes[quote_id]
        
        if to_remove:
            logger.debug(f"Cleaned up {len(to_remove)} old quotes")
    
    async def start(self):
        """Start the Quote manager with periodic sync"""
        if self._running:
            logger.warning("Quote Manager already running")
            return
        
        self._running = True
        
        # Initialize by cancelling all quotes
        await self.initialize()
        
        # Start periodic sync task
        self._sync_task = asyncio.create_task(self._periodic_sync_task())
        logger.info("Quote Manager started")
    
    async def stop(self):
        """Stop the Quote manager"""
        self._running = False
        
        if self._sync_task:
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass
        
        logger.info("Quote Manager stopped")
    
    def get_open_quotes(self) -> List[Quote]:
        """Get list of open quotes"""
        return [q for q in self.quotes.values() if q.is_open]
    
    def get_filled_quotes(self) -> List[Quote]:
        """Get list of filled quotes"""
        return [q for q in self.quotes.values() if q.state == QuoteState.FILLED]
    
    def get_quote(self, quote_id: str) -> Optional[Quote]:
        """Get specific quote by ID"""
        return self.quotes.get(quote_id)
    
    def get_rfq_quotes(self, request_id: str) -> List[Quote]:
        """Get all quotes for a specific RFQ"""
        quote_ids = self.quotes_by_rfq.get(request_id, set())
        return [self.quotes[qid] for qid in quote_ids if qid in self.quotes]
    
    def get_stats(self) -> dict:
        """Get manager statistics"""
        open_quotes = self.get_open_quotes()
        filled_quotes = self.get_filled_quotes()
        
        return {
            **self.stats,
            "active_quotes": len(open_quotes),
            "filled_quotes": len(filled_quotes),
            "total_quotes": len(self.quotes),
            "rfqs_with_quotes": len(self.quotes_by_rfq),
            "time_since_sync": time.time() - self.last_sync_time if self.last_sync_time else None
        }
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.stop()