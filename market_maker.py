"""
Market Maker with orchestration logic.
"""

import asyncio
import logging
import os
from collections import defaultdict
from datetime import datetime
from dotenv import load_dotenv
from typing import Dict, List, Optional, Set

from api_client import RfqAPI, FuturesAPI, CoincallCredential
from interfaces import (
    IPricingService, IQuoteService,
    IQuoteOrchestrator, IRFQInstrumentTracker
)
from models import RFQ, Quote, RFQState
from pricing_engine import SimplePricingService
from quote_manager import QuoteManager
from rfq_manager import SimpleRFQService
from websocket_client import WebSocketConfig, WebSocketManager, MessageType

logger = logging.getLogger(__name__)


# ============================================================================
# Orchestration Components
# ============================================================================

class RFQInstrumentTracker(IRFQInstrumentTracker):
    """Tracks relationships between RFQs and instruments"""
    
    def __init__(self):
        self.rfq_instruments: Dict[str, Set[str]] = {}  # RFQ ID -> instruments
        self.instrument_rfqs: Dict[str, Set[str]] = {}  # Instrument -> RFQ IDs
    
    def track_rfq(self, rfq: RFQ) -> None:
        """Track instruments for an RFQ"""
        rfq_id = rfq.requestId
        
        if rfq_id not in self.rfq_instruments:
            self.rfq_instruments[rfq_id] = set()
        
        for leg in rfq.legs:
            instrument = leg.instrumentName
            
            # Track RFQ -> instruments
            self.rfq_instruments[rfq_id].add(instrument)
            
            # Track instrument -> RFQs
            if instrument not in self.instrument_rfqs:
                self.instrument_rfqs[instrument] = set()
            self.instrument_rfqs[instrument].add(rfq_id)
            
            logger.debug(f"Tracking {instrument} for RFQ {rfq_id}")
    
    def untrack_rfq(self, rfq_id: str) -> None:
        """Stop tracking instruments for an RFQ"""
        if rfq_id not in self.rfq_instruments:
            return
        
        # Remove from instrument -> RFQs mapping
        for instrument in self.rfq_instruments[rfq_id]:
            if instrument in self.instrument_rfqs:
                self.instrument_rfqs[instrument].discard(rfq_id)
                if not self.instrument_rfqs[instrument]:
                    del self.instrument_rfqs[instrument]
        
        # Remove RFQ tracking
        del self.rfq_instruments[rfq_id]
        logger.debug(f"Untracked RFQ {rfq_id}")
    
    def get_rfq_instruments(self, rfq_id: str) -> Set[str]:
        """Get instruments for an RFQ"""
        return self.rfq_instruments.get(rfq_id, set())
    
    def get_instrument_rfqs(self, instrument: str) -> Set[str]:
        """Get RFQs using an instrument"""
        return self.instrument_rfqs.get(instrument, set())
    
    def cleanup_orphaned_instruments(self) -> int:
        """Remove instruments not used by any RFQ"""
        orphaned = [
            inst for inst, rfqs in self.instrument_rfqs.items()
            if not rfqs
        ]
        
        for instrument in orphaned:
            del self.instrument_rfqs[instrument]
        
        return len(orphaned)


class QuoteOrchestrator(IQuoteOrchestrator):
    """Orchestrates quote creation workflow"""
    
    def __init__(self,
                 pricing_service: IPricingService,
                 quote_service: IQuoteService,
                 instrument_tracker: IRFQInstrumentTracker,
                 min_time_to_expiry: int = 5):
        """Initialize quote orchestrator"""
        self.pricing_service = pricing_service
        self.quote_service = quote_service
        self.instrument_tracker = instrument_tracker
        self.min_time_to_expiry = min_time_to_expiry
        
        self.stats = {
            "quotes_attempted": 0,
            "quotes_successful": 0,
            "quotes_failed": 0,
            "quotes_updated": 0
        }
    
    async def should_quote_rfq(self, rfq: RFQ) -> bool:
        """Determine if RFQ should be quoted"""
        # Check if RFQ is active
        if rfq.state != RFQState.ACTIVE:
            logger.debug(f"RFQ {rfq.requestId} not active")
            return False
        
        # Check expiry time
        time_to_expiry = (rfq.expiryTime - int(datetime.now().timestamp() * 1000)) / 1000
        if time_to_expiry < self.min_time_to_expiry:
            logger.debug(f"RFQ {rfq.requestId} expiring too soon ({time_to_expiry:.1f}s)")
            return False
        
        # Check if all instruments are available and priced
        for leg in rfq.legs:
            instrument = self.pricing_service.get_instrument(leg.instrumentName)
            if not instrument or not instrument.is_priced:
                logger.debug(f"Instrument {leg.instrumentName} not ready for quoting")
                return False
        
        return True
    
    async def create_quote_for_rfq(self, rfq: RFQ) -> Optional[Quote]:
        """Create quote for RFQ"""
        self.stats["quotes_attempted"] += 1
        
        # Check if we should quote
        if not await self.should_quote_rfq(rfq):
            return None
        
        # Prepare quote legs with prices
        quote_legs = []
        
        for leg in rfq.legs:
            price_quote = await self.pricing_service.calculate_price(leg.instrumentName)
            if not price_quote:
                logger.warning(f"Could not get price for {leg.instrumentName}")
                self.stats["quotes_failed"] += 1
                return None
            
            # Determine price based on side
            if leg.side == "BUY":
                # Customer buying, we quote ask
                price = price_quote.ask
            else:
                # Customer selling, we quote bid
                price = price_quote.bid
            
            quote_legs.append({
                "instrumentName": leg.instrumentName,
                "price": str(price)
            })
        
        # Create the quote
        try:
            quote = await self.quote_service.create_quote(rfq.requestId, quote_legs)
            if quote:
                self.stats["quotes_successful"] += 1
                logger.info(f"Created quote {quote.quoteId} for RFQ {rfq.requestId}")
                return quote
            else:
                self.stats["quotes_failed"] += 1
                return None
        except Exception as e:
            logger.error(f"Failed to create quote for RFQ {rfq.requestId}: {e}")
            self.stats["quotes_failed"] += 1
            return None
    
    async def update_quotes_for_rfq(self, rfq: RFQ) -> int:
        """Update existing quotes for RFQ"""
        # Cancel existing quotes
        try:
            cancelled = await self.quote_service.cancel_rfq_quotes(rfq.requestId)
            logger.info(f"Cancelled {cancelled} quotes for RFQ {rfq.requestId}")
        except Exception as e:
            logger.error(f"Failed to cancel quotes: {e}")
        
        # Create new quote
        quote = await self.create_quote_for_rfq(rfq)
        if quote:
            self.stats["quotes_updated"] += 1
            return 1
        return 0
    
    def get_stats(self) -> dict:
        """Get orchestrator statistics"""
        return self.stats.copy()


# ============================================================================
# Main Market Maker
# ============================================================================

class RFQMarketMaker:
    """
    RFQ Market Maker orchestration.
    """
    
    def __init__(self, api_key: str, api_secret: str,
                 enable_auto_quoting: bool = False,
                 pricing_update_interval: int = 5):
        """Initialize market maker"""
        
        # API Client
        credential = CoincallCredential(api_key=api_key, secret_key=api_secret)
        self.api_client = RfqAPI(credential=credential)
        self.futures_api = FuturesAPI(credential=credential)
        
        # Core Services (each with single responsibility)
        self.pricing_service = SimplePricingService(self.futures_api)
        self.rfq_service = SimpleRFQService(self.api_client)
        self.quote_service = QuoteManager(self.api_client)
        
        # Orchestration Components
        self.instrument_tracker = RFQInstrumentTracker()
        self.quote_orchestrator = QuoteOrchestrator(
            pricing_service=self.pricing_service,
            quote_service=self.quote_service,
            instrument_tracker=self.instrument_tracker
        )
        
        # Configuration
        self.enable_auto_quoting = enable_auto_quoting
        self.pricing_update_interval = pricing_update_interval
        
        # WebSocket Setup
        ws_config = WebSocketConfig(
            api_key=api_key,
            api_secret=api_secret,
            heartbeat_interval=5,
            reconnect_delay=5,
            max_reconnect_attempts=10
        )
        self.ws_manager = WebSocketManager(ws_config)
        
        # Register WebSocket callbacks
        self.ws_manager.register_callback(MessageType.RFQ_MAKER, self.handle_rfq)
        self.ws_manager.register_callback(MessageType.RFQ_QUOTE, self.handle_quote_update)
        self.ws_manager.register_callback(MessageType.BLOCK_TRADE_DETAIL, self.handle_trade)
        
        # Runtime state
        self._running = False
        self._pricing_task: Optional[asyncio.Task] = None
        self._quoting_task: Optional[asyncio.Task] = None
        
        # Statistics
        self.stats = {
            "rfqs_received": 0,
            "rfqs_processed": 0,
            "price_updates": 0,
            "trades_executed": 0
        }
    
    async def handle_rfq(self, data: dict):
        """Handle incoming RFQ from WebSocket"""
        self.stats["rfqs_received"] += 1
        
        # Process in RFQ service
        await self.rfq_service.process_websocket_message(data)
        
        # Get RFQ details
        rfq_data = data.get('d', {})
        request_id = rfq_data.get('requestId')
        state = rfq_data.get('state')
        
        logger.info(f"Received RFQ {request_id} with state {state}")
        
        # Only process ACTIVE RFQs
        if state == "ACTIVE":
            rfq = self.rfq_service.get_rfq(request_id)
            if rfq:
                # Check if this is a new RFQ we haven't seen before
                is_new_rfq = request_id not in self.instrument_tracker.rfq_instruments
                
                if is_new_rfq:
                    logger.info(f"Processing new active RFQ {request_id}")
                    # Always track and price new RFQs
                    await self.setup_rfq_tracking_and_pricing(rfq)
                    
                    # Only quote if auto-quoting is enabled
                    if self.enable_auto_quoting:
                        await self.create_quote_for_rfq_safe(rfq)
                else:
                    # For existing RFQs, only update quote if auto-quoting is enabled
                    if self.enable_auto_quoting:
                        logger.info(f"Updating quote for existing RFQ {request_id}")
                        await self.update_rfq_quote(rfq)
        elif state in ["EXPIRED", "CANCELLED", "FILLED"]:
            # Clean up tracking for non-active RFQs
            self.instrument_tracker.untrack_rfq(request_id)
            logger.info(f"Untracked RFQ {request_id} with state {state}")
    
    async def setup_rfq_tracking_and_pricing(self, rfq: RFQ):
        """Setup tracking and pricing for an RFQ (without quoting)"""
        try:
            self.stats["rfqs_processed"] += 1
            
            # 1. Track instruments for this RFQ
            self.instrument_tracker.track_rfq(rfq)
            
            # 2. Add all instruments to pricing service and collect them
            instruments_to_price = []
            for leg in rfq.legs:
                instrument_name = leg.instrumentName
                instruments_to_price.append(instrument_name)
                
                # Add instrument to pricing service if not already tracked
                if not self.pricing_service.get_instrument(instrument_name):
                    logger.info(f"Adding new instrument {instrument_name} to pricing service")
                    self.pricing_service.add_instrument(instrument_name)
            
            # 3. Update prices for all instruments
            logger.info(f"Updating prices for {len(instruments_to_price)} instruments from RFQ {rfq.requestId}")
            await self.pricing_service.update_market_data()
            
            # Log pricing status
            priced_count = 0
            for instrument_name in instruments_to_price:
                instrument = self.pricing_service.get_instrument(instrument_name)
                if instrument and instrument.is_priced:
                    priced_count += 1
                else:
                    logger.debug(f"Instrument {instrument_name} not yet priced")
            
            logger.info(f"RFQ {rfq.requestId}: {priced_count}/{len(instruments_to_price)} instruments priced")
            
        except Exception as e:
            logger.error(f"Error setting up tracking and pricing for RFQ {rfq.requestId}: {e}")
    
    async def create_quote_for_rfq_safe(self, rfq: RFQ):
        """Safely create a quote for an RFQ with error handling"""
        try:
            # Check if all instruments are priced before quoting
            all_priced = True
            for leg in rfq.legs:
                instrument = self.pricing_service.get_instrument(leg.instrumentName)
                if not instrument or not instrument.is_priced:
                    all_priced = False
                    break
            
            if all_priced:
                quote = await self.quote_orchestrator.create_quote_for_rfq(rfq)
                if quote:
                    logger.info(f"Successfully created quote for RFQ {rfq.requestId}")
                else:
                    logger.warning(f"Failed to create quote for RFQ {rfq.requestId}")
            else:
                logger.warning(f"Not all instruments priced for RFQ {rfq.requestId}, skipping quote")
                
        except Exception as e:
            logger.error(f"Error creating quote for RFQ {rfq.requestId}: {e}")
    
    async def update_rfq_quote(self, rfq: RFQ):
        """Update quote for an existing RFQ"""
        try:
            # Ensure prices are up to date
            await self.pricing_service.update_market_data()
            
            # Update the quote
            updated = await self.quote_orchestrator.update_quotes_for_rfq(rfq)
            if updated:
                logger.info(f"Successfully updated quote for RFQ {rfq.requestId}")
            else:
                logger.debug(f"No quote update needed for RFQ {rfq.requestId}")
                
        except Exception as e:
            logger.error(f"Error updating quote for RFQ {rfq.requestId}: {e}")
    
    async def handle_quote_update(self, data: dict):
        """Handle quote status updates"""
        # Process in quote service
        self.quote_service.handle_websocket_message(data)
        
        quote_data = data.get('d', {})
        quote_id = quote_data.get('quoteId')
        state = quote_data.get('state')
        
        logger.info(f"Quote {quote_id} updated to {state}")
    
    async def handle_trade(self, data: dict):
        """Handle executed trades"""
        self.stats["trades_executed"] += 1
        
        trade_data = data.get('d', {})
        trade_id = trade_data.get('blockTradeId')
        quote_id = trade_data.get('quoteId')
        
        logger.info(f"Trade executed: {trade_id} from quote {quote_id}")
        
        # Could add position tracking here in future
    
    async def _pricing_update_loop(self):
        """Periodic pricing update loop"""
        while self._running:
            try:
                # Update all prices
                await self.pricing_service.update_market_data()
                self.stats["price_updates"] += 1
                
                await asyncio.sleep(self.pricing_update_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in pricing loop: {e}")
                await asyncio.sleep(self.pricing_update_interval)
    
    async def _quote_update_loop(self):
        """Periodic quote update loop"""
        while self._running:
            try:
                if self.enable_auto_quoting:
                    # Get active RFQs
                    active_rfqs = self.rfq_service.get_active_rfqs()
                    
                    if active_rfqs:
                        logger.info(f"Updating quotes for {len(active_rfqs)} RFQs")
                        
                        for rfq in active_rfqs:
                            try:
                                await self.quote_orchestrator.update_quotes_for_rfq(rfq)
                            except Exception as e:
                                logger.error(f"Error updating quotes for RFQ {rfq.requestId}: {e}")
                
                await asyncio.sleep(self.pricing_update_interval * 2)  # Less frequent than pricing
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in quote update loop: {e}")
                await asyncio.sleep(self.pricing_update_interval * 2)
    
    async def initialize_market_maker(self):
        """Initialize market maker state on startup"""
        logger.info("Initializing market maker state...")
        
        try:
            # 1. Cancel all existing quotes
            logger.info("Cancelling all existing quotes...")
            cancelled_count = await self.quote_service.cancel_all_quotes()
            logger.info(f"Cancelled {cancelled_count} existing quotes")
            
            # 2. Get active RFQs
            logger.info("Getting active RFQs...")
            active_rfqs = self.rfq_service.get_active_rfqs()
            logger.info(f"Found {len(active_rfqs)} active RFQs")
            
            if active_rfqs:
                # 3. Track instruments from active RFQs
                instruments_to_price = set()
                for rfq in active_rfqs:
                    self.instrument_tracker.track_rfq(rfq)
                    for leg in rfq.legs:
                        instruments_to_price.add(leg.instrumentName)
                        self.pricing_service.add_instrument(leg.instrumentName)
                
                logger.info(f"Tracking {len(instruments_to_price)} unique instruments")
                
                # 4. Update prices for all tracked instruments
                logger.info("Updating prices for all instruments...")
                await self.pricing_service.update_market_data()
                
                # 5. Quote all active RFQs if auto-quoting is enabled
                if self.enable_auto_quoting:
                    logger.info("Creating initial quotes for active RFQs...")
                    quoted_count = 0
                    for rfq in active_rfqs:
                        quote = await self.quote_orchestrator.create_quote_for_rfq(rfq)
                        if quote:
                            quoted_count += 1
                    logger.info(f"Successfully quoted {quoted_count}/{len(active_rfqs)} active RFQs")
                
            logger.info("Market maker initialization complete")
            
        except Exception as e:
            logger.error(f"Error during market maker initialization: {e}")
            raise
    
    async def run(self):
        """Run the market maker"""
        logger.info("Starting RFQ Market Maker")
        
        # Start services
        async with self.api_client:
            async with self.futures_api:
                async with self.quote_service:
                    async with self.rfq_service:
                        async with self.ws_manager:
                            # Initialize market maker state
                            await self.initialize_market_maker()
                            
                            # Start background tasks
                            self._running = True
                            self._pricing_task = asyncio.create_task(self._pricing_update_loop())
                            self._quoting_task = asyncio.create_task(self._quote_update_loop())
                            
                            logger.info("=" * 50)
                            logger.info("Market Maker Started")
                            logger.info(f"Auto-quoting: {'ENABLED' if self.enable_auto_quoting else 'DISABLED'}")
                            logger.info(f"Pricing interval: {self.pricing_update_interval}s")
                            logger.info("=" * 50)
                            
                            # Main loop
                            while True:
                                await asyncio.sleep(30)
                                
                                # Log statistics
                                self._log_statistics()
        
        # Cleanup
        self._running = False
        if self._pricing_task:
            self._pricing_task.cancel()
        if self._quoting_task:
            self._quoting_task.cancel()
        
        logger.info("Market Maker stopped")
    
    def _log_statistics(self):
        """Log current statistics"""
        rfq_stats = self.rfq_service.get_stats()
        quote_stats = self.quote_service.get_stats()
        pricing_stats = self.pricing_service.get_summary()
        orchestrator_stats = self.quote_orchestrator.get_stats()
        
        logger.info("-" * 50)
        logger.info(f"RFQs - Active: {rfq_stats['active_rfqs']}, "
                   f"Total: {rfq_stats['total_rfqs']}")
        logger.info(f"Quotes - Active: {quote_stats['active_quotes']}, "
                   f"Filled: {quote_stats['filled_quotes']}")
        logger.info(f"Pricing - Instruments: {pricing_stats['total_instruments']}, "
                   f"Priced: {pricing_stats['priced_instruments']}")
        logger.info(f"Orchestrator - Attempted: {orchestrator_stats['quotes_attempted']}, "
                   f"Successful: {orchestrator_stats['quotes_successful']}")
        logger.info(f"Market Maker - RFQs Received: {self.stats['rfqs_received']}, "
                   f"Processed: {self.stats['rfqs_processed']}")
        logger.info("-" * 50)


# ============================================================================
# Main Entry Point
# ============================================================================

async def main():
    """Main entry point"""
    # Load environment variables
    load_dotenv()
    API_KEY = os.environ.get("API_KEY")
    API_SECRET = os.environ.get("API_SECRET")
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create and run market maker
    market_maker = RFQMarketMaker(
        API_KEY,
        API_SECRET,
        enable_auto_quoting=True,
        pricing_update_interval=5
    )
    
    await market_maker.run()


if __name__ == "__main__":
    asyncio.run(main())