import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Set
from dataclasses import dataclass, field
from enum import Enum
import time

from api_client import RfqAPI, CoincallCredential, RFQLeg, CcAPIException
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pricing_engine import PricingEngine

logger = logging.getLogger(__name__)


class RFQState(Enum):
    """RFQ States"""
    ACTIVE = "ACTIVE"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"
    FILLED = "FILLED"
    TRADED_AWAY = "TRADED_AWAY"


@dataclass
class RFQ:
    """Represents an RFQ with all its details"""
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
    
    @property
    def is_active(self) -> bool:
        """Check if RFQ is active and not expired"""
        if self.state != RFQState.ACTIVE:
            return False
        # Check if expired based on current time
        return int(time.time() * 1000) < self.expiryTime
    
    @property
    def time_to_expiry(self) -> float:
        """Returns seconds until expiry"""
        return max(0, (self.expiryTime - int(time.time() * 1000)) / 1000)
    
    @classmethod
    def from_api_data(cls, data: dict) -> "RFQ":
        """Create RFQ from API response data"""
        legs = []
        for leg_data in data.get("legs", []):
            legs.append(RFQLeg(
                instrumentName=leg_data["instrumentName"],
                side=leg_data["side"],
                quantity=leg_data["quantity"]
            ))
        
        return cls(
            requestId=data["requestId"],
            state=RFQState(data["state"]),
            legs=legs,
            createTime=data.get("createTime", 0),
            expiryTime=data.get("expiryTime", 0),
            takerName=data.get("takerName"),
            counterparty=data.get("counterparty"),
            lastUpdateTime=data.get("updateTime"),
            raw_data=data
        )
    
    @classmethod
    def from_ws_data(cls, data: dict) -> "RFQ":
        """Create RFQ from WebSocket message data"""
        # WebSocket data structure might be slightly different
        legs = []
        for leg_data in data.get("legs", []):
            legs.append(RFQLeg(
                instrumentName=leg_data["instrumentName"],
                side=leg_data["side"],
                quantity=str(leg_data.get("quantity", ""))
            ))
        
        return cls(
            requestId=data["requestId"],
            state=RFQState(data["state"]),
            legs=legs,
            createTime=data.get("createTime", int(time.time() * 1000)),
            expiryTime=data.get("expiryTime", 0),
            takerName=data.get("takerName"),
            counterparty=data.get("counterparty"),
            lastUpdateTime=int(time.time() * 1000),
            raw_data=data
        )


class RFQManager:
    """
    Manages RFQs with API initialization, WebSocket updates, and periodic sync
    
    Features:
    - Initialize with active RFQs from API
    - Add/remove/update RFQs from WebSocket messages
    - Periodic synchronization with API
    - Track quoted RFQs
    - Monitor RFQ expiry
    """
    
    def __init__(self, api_client: RfqAPI, pricing_engine: Optional['PricingEngine'] = None, sync_interval: int = 60):
        """
        Initialize RFQ Manager
        
        Args:
            api_client: RfqAPI client for API calls
            pricing_engine: Optional PricingEngine for instrument management and quoting
            sync_interval: Seconds between API syncs (default 60)
        """
        self.api_client = api_client
        self.pricing_engine = pricing_engine
        self.sync_interval = sync_interval
        self.rfqs: Dict[str, RFQ] = {}
        self.last_sync_time = 0
        self._sync_task: Optional[asyncio.Task] = None
        self._running = False
        
        # Statistics
        self.stats = {
            "total_received": 0,
            "total_removed": 0,
            "total_expired": 0,
            "total_filled": 0,
            "sync_count": 0,
            "sync_errors": 0,
            "last_sync_time": None
        }
    
    async def initialize(self) -> int:
        """
        Initialize with all active RFQs from API
        
        Returns:
            Number of RFQs loaded
        """
        logger.info("Initializing RFQ Manager with API data")
        
        try:
            # Get active RFQs where user is maker
            response = await self.api_client.get_rfq_list(
                state="OPEN",
                # role="MAKER"
            )
            logger.debug(f"Raw response: {response}")
            
            rfq_list = response.get("data", {}).get("rfqList", [])
            
            for rfq_data in rfq_list:
                rfq = RFQ.from_api_data(rfq_data)
                if rfq.is_active:
                    self.rfqs[rfq.requestId] = rfq
                    # Update pricing engine with instruments
                    if self.pricing_engine:
                        self.pricing_engine.update_rfq_instruments(rfq)
                    logger.debug(f"Loaded RFQ {rfq.requestId} (expires in {rfq.time_to_expiry:.0f}s)")
            
            self.last_sync_time = time.time()
            self.stats["last_sync_time"] = self.last_sync_time
            
            logger.info(f"Initialized with {len(self.rfqs)} active RFQs")
            return len(self.rfqs)
            
        except CcAPIException as e:
            logger.error(f"Failed to initialize RFQs from API: {e}")
            self.stats["sync_errors"] += 1
            raise
        except Exception as e:
            logger.error(f"Unexpected error during initialization: {e}")
            self.stats["sync_errors"] += 1
            raise
    
    def add_or_update_rfq(self, rfq_data: dict, from_websocket: bool = True) -> bool:
        """
        Add or update an RFQ
        
        Args:
            rfq_data: RFQ data dict
            from_websocket: Whether data is from WebSocket or API
        
        Returns:
            True if RFQ was added/updated, False otherwise
        """
        try:
            if from_websocket:
                rfq = RFQ.from_ws_data(rfq_data)
            else:
                rfq = RFQ.from_api_data(rfq_data)
            
            request_id = rfq.requestId
            
            # Check if RFQ should be tracked
            if not rfq.is_active:
                logger.debug(f"Skipping inactive RFQ {request_id} (state: {rfq.state})")
                if request_id in self.rfqs:
                    del self.rfqs[request_id]
                    self.stats["total_removed"] += 1
                return False
            
            # Add or update
            is_new = request_id not in self.rfqs
            if is_new:
                logger.info(f"Added new RFQ {request_id} (expires in {rfq.time_to_expiry:.0f}s)")
                self.stats["total_received"] += 1
            else:
                logger.debug(f"Updated RFQ {request_id}")
            
            self.rfqs[request_id] = rfq
            
            # Update pricing engine with instruments
            if self.pricing_engine and is_new:
                self.pricing_engine.update_rfq_instruments(rfq)
                # Optionally create quotes immediately
                asyncio.create_task(self._try_create_quote(rfq))
            
            return True
            
        except Exception as e:
            logger.error(f"Error adding/updating RFQ: {e}")
            return False
    
    def remove_rfq(self, request_id: str, reason: str = "UNKNOWN") -> bool:
        """
        Remove an RFQ
        
        Args:
            request_id: RFQ ID to remove
            reason: Removal reason (CANCELLED, EXPIRED, FILLED, etc.)
        
        Returns:
            True if removed, False if not found
        """
        if request_id in self.rfqs:
            rfq = self.rfqs[request_id]
            
            # Remove instruments from pricing engine
            if self.pricing_engine:
                self.pricing_engine.remove_rfq_instruments(rfq)
            
            del self.rfqs[request_id]
            
            logger.info(f"Removed RFQ {request_id} (reason: {reason})")
            
            # Update stats
            self.stats["total_removed"] += 1
            if reason == "EXPIRED":
                self.stats["total_expired"] += 1
            elif reason == "FILLED":
                self.stats["total_filled"] += 1
            
            return True
        
        return False
    
    def handle_websocket_message(self, message: dict) -> None:
        """
        Process WebSocket message for RFQ updates
        
        Args:
            message: WebSocket message dict
        """
        try:
            # Extract RFQ data from message
            rfq_data = message.get("d", {})
            if not rfq_data:
                return
            
            request_id = rfq_data.get("requestId")
            state = rfq_data.get("state")
            
            if not request_id:
                return
            
            logger.debug(f"Processing WebSocket RFQ {request_id} with state {state}")
            
            # Handle based on state
            if state == "ACTIVE":
                self.add_or_update_rfq(rfq_data, from_websocket=True)
            elif state in ["CANCELLED", "EXPIRED", "FILLED", "TRADED_AWAY"]:
                self.remove_rfq(request_id, reason=state)
            else:
                logger.warning(f"Unknown RFQ state: {state}")
                
        except Exception as e:
            logger.error(f"Error handling WebSocket message: {e}")
    
    async def sync_with_api(self) -> Dict[str, int]:
        """
        Synchronize RFQs with API
        
        Returns:
            Dict with sync statistics
        """
        logger.debug("Starting API sync")
        sync_stats = {
            "added": 0,
            "removed": 0,
            "updated": 0,
            "errors": 0
        }
        
        try:
            # Get current active RFQs from API
            response = await self.api_client.get_rfq_list(
                state="OPEN",
                # role="MAKER"
            )
            logger.debug(f"Raw response: {response}")
            
            api_rfqs = {}
            rfq_list = response.get("data", {}).get("rfqList", [])
            
            for rfq_data in rfq_list:
                rfq = RFQ.from_api_data(rfq_data)
                if rfq.is_active:
                    api_rfqs[rfq.requestId] = rfq
            
            # Find RFQs to add (in API but not local)
            for request_id, rfq in api_rfqs.items():
                if request_id not in self.rfqs:
                    self.rfqs[request_id] = rfq
                    # Update pricing engine with instruments
                    if self.pricing_engine:
                        self.pricing_engine.update_rfq_instruments(rfq)
                    sync_stats["added"] += 1
                    logger.info(f"Sync: Added missing RFQ {request_id}")
                else:
                    # Update existing RFQ if API has newer data
                    local_rfq = self.rfqs[request_id]
                    if rfq.lastUpdateTime and local_rfq.lastUpdateTime:
                        if rfq.lastUpdateTime > local_rfq.lastUpdateTime:
                            self.rfqs[request_id] = rfq
                            sync_stats["updated"] += 1
            
            # Find RFQs to remove (local but not in API or expired)
            to_remove = []
            for request_id, rfq in self.rfqs.items():
                if request_id not in api_rfqs or not rfq.is_active:
                    to_remove.append(request_id)
            
            for request_id in to_remove:
                self.remove_rfq(request_id, reason="SYNC_CLEANUP")
                sync_stats["removed"] += 1
                logger.info(f"Sync: Removed stale RFQ {request_id}")
            
            self.last_sync_time = time.time()
            self.stats["last_sync_time"] = self.last_sync_time
            self.stats["sync_count"] += 1
            
            logger.info(f"Sync completed: added={sync_stats['added']}, "
                       f"removed={sync_stats['removed']}, "
                       f"updated={sync_stats['updated']}, "
                       f"total_active={len(self.rfqs)}")
            
        except CcAPIException as e:
            logger.error(f"API sync failed: {e}")
            sync_stats["errors"] += 1
            self.stats["sync_errors"] += 1
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
                
                # Also check for expired RFQs
                self._cleanup_expired_rfqs()
                
                # Sync with API
                await self.sync_with_api()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic sync: {e}")
                await asyncio.sleep(10)  # Wait before retry
    
    def _cleanup_expired_rfqs(self):
        """Remove expired RFQs based on expiry time"""
        current_time = int(time.time() * 1000)
        to_remove = []
        
        for request_id, rfq in self.rfqs.items():
            if current_time >= rfq.expiryTime:
                to_remove.append(request_id)
        
        for request_id in to_remove:
            self.remove_rfq(request_id, reason="EXPIRED")
    
    async def start(self):
        """Start the RFQ manager with periodic sync"""
        if self._running:
            logger.warning("RFQ Manager already running")
            return
        
        self._running = True
        
        # Initialize with API data
        await self.initialize()
        
        # Start periodic sync task
        self._sync_task = asyncio.create_task(self._periodic_sync_task())
        logger.info("RFQ Manager started")
    
    async def stop(self):
        """Stop the RFQ manager"""
        self._running = False
        
        if self._sync_task:
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass
        
        logger.info("RFQ Manager stopped")
    
    def get_active_rfqs(self) -> List[RFQ]:
        """Get list of active RFQs"""
        return [rfq for rfq in self.rfqs.values() if rfq.is_active]
    
    def get_rfq(self, request_id: str) -> Optional[RFQ]:
        """Get specific RFQ by ID"""
        return self.rfqs.get(request_id)
    
    def add_quote_to_rfq(self, request_id: str, quote_id: str) -> bool:
        """Track that we quoted an RFQ"""
        if request_id in self.rfqs:
            self.rfqs[request_id].quoteIds.add(quote_id)
            return True
        return False
    
    def get_stats(self) -> dict:
        """Get manager statistics"""
        return {
            **self.stats,
            "active_rfqs": len(self.rfqs),
            "active_rfq_ids": list(self.rfqs.keys()),
            "time_since_sync": time.time() - self.last_sync_time if self.last_sync_time else None
        }
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self.start()
        return self
    
    async def _try_create_quote(self, rfq: RFQ) -> None:
        """
        Try to create a quote for an RFQ if pricing engine is available
        
        Args:
            rfq: RFQ to quote for
        """
        if self.pricing_engine and rfq.is_active:
            try:
                success = await self.pricing_engine.create_quotes_for_rfq(rfq)
                if success:
                    logger.info(f"Created quote for RFQ {rfq.requestId}")
                else:
                    logger.debug(f"Could not create quote for RFQ {rfq.requestId}")
            except Exception as e:
                logger.error(f"Error creating quote for RFQ {rfq.requestId}: {e}")
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.stop()