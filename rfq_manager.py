"""
RFQ lifecycle management
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional, Set

from api_client import RfqAPI, CcAPIException
from interfaces import IRFQRepository, IRFQService
from models import RFQ, RFQState, RFQLeg

logger = logging.getLogger(__name__)


# ============================================================================
# RFQ Factory
# ============================================================================

class RFQFactory:
    """Factory for creating RFQ objects from different data sources"""
    
    @staticmethod
    def from_api_data(data: dict) -> RFQ:
        """Create RFQ from API response data"""
        legs = []
        for leg_data in data.get("legs", []):
            legs.append(RFQLeg(
                instrumentName=leg_data["instrumentName"],
                side=leg_data["side"],
                quantity=leg_data["quantity"]
            ))
        
        return RFQ(
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
    
    @staticmethod
    def from_websocket_data(data: dict) -> RFQ:
        """Create RFQ from WebSocket message data"""
        legs = []
        for leg_data in data.get("legs", []):
            legs.append(RFQLeg(
                instrumentName=leg_data["instrumentName"],
                side=leg_data["side"],
                quantity=str(leg_data.get("quantity", ""))
            ))
        
        return RFQ(
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


# ============================================================================
# RFQ Repository Implementation
# ============================================================================

class InMemoryRFQRepository(IRFQRepository):
    """In-memory RFQ storage implementation"""
    
    def __init__(self):
        self.rfqs: Dict[str, RFQ] = {}
        self.stats = {
            "total_added": 0,
            "total_removed": 0,
            "total_expired": 0
        }
    
    def get_active_rfqs(self) -> List[RFQ]:
        """Get all active RFQs"""
        current_time = int(time.time() * 1000)
        return [
            rfq for rfq in self.rfqs.values()
            if rfq.state == RFQState.ACTIVE and rfq.expiryTime > current_time
        ]
    
    def get_rfq(self, rfq_id: str) -> Optional[RFQ]:
        """Get specific RFQ by ID"""
        return self.rfqs.get(rfq_id)
    
    def add_rfq(self, rfq: RFQ) -> bool:
        """Add or update RFQ"""
        is_new = rfq.requestId not in self.rfqs
        self.rfqs[rfq.requestId] = rfq
        
        if is_new:
            self.stats["total_added"] += 1
            logger.info(f"Added RFQ {rfq.requestId}")
        else:
            logger.debug(f"Updated RFQ {rfq.requestId}")
        
        return True
    
    def remove_rfq(self, rfq_id: str) -> bool:
        """Remove RFQ"""
        if rfq_id in self.rfqs:
            del self.rfqs[rfq_id]
            self.stats["total_removed"] += 1
            logger.info(f"Removed RFQ {rfq_id}")
            return True
        return False
    
    def get_rfqs_expiring_soon(self, seconds: int = 60) -> List[RFQ]:
        """Get RFQs expiring within specified seconds"""
        current_time = int(time.time() * 1000)
        expiry_threshold = current_time + (seconds * 1000)
        
        return [
            rfq for rfq in self.get_active_rfqs()
            if rfq.expiryTime <= expiry_threshold
        ]
    
    def cleanup_expired(self) -> int:
        """Remove expired RFQs and return count"""
        current_time = int(time.time() * 1000)
        expired_ids = [
            rfq_id for rfq_id, rfq in self.rfqs.items()
            if rfq.expiryTime <= current_time
        ]
        
        for rfq_id in expired_ids:
            self.remove_rfq(rfq_id)
            self.stats["total_expired"] += 1
        
        if expired_ids:
            logger.info(f"Cleaned up {len(expired_ids)} expired RFQs")
        
        return len(expired_ids)
    
    def get_stats(self) -> dict:
        """Get repository statistics"""
        active_rfqs = self.get_active_rfqs()
        return {
            **self.stats,
            "total_rfqs": len(self.rfqs),
            "active_rfqs": len(active_rfqs)
        }


# ============================================================================
# RFQ Service Implementation
# ============================================================================

class SimpleRFQService(IRFQService):
    """
    Simple RFQ service focused on RFQ management.
    No pricing or quote creation dependencies.
    """
    
    def __init__(self, 
                 api_client: RfqAPI,
                 repository: Optional[InMemoryRFQRepository] = None,
                 sync_interval: int = 60):
        """Initialize RFQ service"""
        self.api_client = api_client
        self.repository = repository or InMemoryRFQRepository()
        self.factory = RFQFactory()
        self.sync_interval = sync_interval
        
        self._running = False
        self._sync_task: Optional[asyncio.Task] = None
        self.last_sync_time = 0
        
        self.sync_stats = {
            "sync_count": 0,
            "sync_errors": 0,
            "last_sync_time": None
        }
    
    async def initialize(self) -> int:
        """Initialize with RFQs from API"""
        logger.info("Initializing RFQ Service with API data")
        
        try:
            response = await self.api_client.get_rfq_list(state="OPEN")
            rfq_list = response.get("data", {}).get("rfqList", [])
            
            added_count = 0
            for rfq_data in rfq_list:
                rfq = self.factory.from_api_data(rfq_data)
                if rfq.state == RFQState.ACTIVE:
                    self.repository.add_rfq(rfq)
                    added_count += 1
            
            self.last_sync_time = time.time()
            self.sync_stats["last_sync_time"] = self.last_sync_time
            
            logger.info(f"Initialized with {added_count} active RFQs")
            return added_count
            
        except CcAPIException as e:
            logger.error(f"Failed to initialize RFQs from API: {e}")
            self.sync_stats["sync_errors"] += 1
            raise
    
    async def process_websocket_message(self, message: dict) -> None:
        """Process RFQ update from websocket"""
        try:
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
                rfq = self.factory.from_websocket_data(rfq_data)
                self.repository.add_rfq(rfq)
            elif state in ["CANCELLED", "EXPIRED", "FILLED", "TRADED_AWAY"]:
                self.repository.remove_rfq(request_id)
            else:
                logger.warning(f"Unknown RFQ state: {state}")
                
        except Exception as e:
            logger.error(f"Error processing WebSocket message: {e}")
    
    async def sync_with_api(self) -> Dict[str, int]:
        """Sync RFQs with API"""
        logger.debug("Starting API sync")
        sync_results = {
            "added": 0,
            "removed": 0,
            "updated": 0,
            "errors": 0
        }
        
        try:
            # Get current RFQs from API
            response = await self.api_client.get_rfq_list(state="OPEN")
            api_rfqs = {}
            
            rfq_list = response.get("data", {}).get("rfqList", [])
            for rfq_data in rfq_list:
                rfq = self.factory.from_api_data(rfq_data)
                if rfq.state == RFQState.ACTIVE:
                    api_rfqs[rfq.requestId] = rfq
            
            # Find RFQs to add
            for request_id, rfq in api_rfqs.items():
                existing = self.repository.get_rfq(request_id)
                if not existing:
                    self.repository.add_rfq(rfq)
                    sync_results["added"] += 1
                elif existing.lastUpdateTime and rfq.lastUpdateTime:
                    if rfq.lastUpdateTime > existing.lastUpdateTime:
                        self.repository.add_rfq(rfq)
                        sync_results["updated"] += 1
            
            # Find RFQs to remove
            current_rfqs = self.repository.rfqs.copy()
            for request_id in current_rfqs:
                if request_id not in api_rfqs:
                    self.repository.remove_rfq(request_id)
                    sync_results["removed"] += 1
            
            # Clean up expired
            self.repository.cleanup_expired()
            
            self.last_sync_time = time.time()
            self.sync_stats["last_sync_time"] = self.last_sync_time
            self.sync_stats["sync_count"] += 1
            
            logger.info(f"Sync completed: {sync_results}")
            
        except Exception as e:
            logger.error(f"Error during sync: {e}")
            sync_results["errors"] += 1
            self.sync_stats["sync_errors"] += 1
        
        return sync_results
    
    async def _periodic_sync_task(self):
        """Background task for periodic synchronization"""
        while self._running:
            try:
                await asyncio.sleep(self.sync_interval)
                
                # Clean up expired RFQs
                self.repository.cleanup_expired()
                
                # Sync with API
                await self.sync_with_api()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic sync: {e}")
                await asyncio.sleep(10)
    
    async def start(self):
        """Start the RFQ service"""
        if self._running:
            logger.warning("RFQ Service already running")
            return
        
        self._running = True
        
        # Initialize with API data
        await self.initialize()
        
        # Start periodic sync
        self._sync_task = asyncio.create_task(self._periodic_sync_task())
        logger.info("RFQ Service started")
    
    async def stop(self):
        """Stop the RFQ service"""
        self._running = False
        
        if self._sync_task:
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass
        
        logger.info("RFQ Service stopped")
    
    def get_active_rfqs(self) -> List[RFQ]:
        """Get all active RFQs"""
        return self.repository.get_active_rfqs()
    
    def get_rfq(self, rfq_id: str) -> Optional[RFQ]:
        """Get specific RFQ"""
        return self.repository.get_rfq(rfq_id)
    
    def get_stats(self) -> dict:
        """Get service statistics"""
        repo_stats = self.repository.get_stats()
        return {
            **repo_stats,
            **self.sync_stats,
            "time_since_sync": time.time() - self.last_sync_time if self.last_sync_time else None
        }
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.stop()