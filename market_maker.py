import asyncio
from dotenv import load_dotenv
import logging
import os
import sys
import time

from websocket_client import WebSocketConfig, WebSocketManager, MessageType
from api_client import RfqAPI, CoincallCredential
from rfq_manager import RFQManager, RFQ
from pricing_engine import PricingEngine
from quote_manager import QuoteManager

# Setup logging BEFORE other imports
from logging_config import setup_logging
setup_logging()

# Get logger for this module
logger = logging.getLogger(__name__)

class RFQMarketMakerIntegration:
    """RFQ Market Maker Bot with integrated RFQ management"""
    
    def __init__(self, api_key: str, api_secret: str, 
                 enable_auto_quoting: bool = False,
                 pricing_update_interval: int = 5):
        # Create API client
        credential = CoincallCredential(api_key=api_key, secret_key=api_secret)
        self.api_client = RfqAPI(credential=credential)
        
        # Create Quote Manager
        self.quote_manager = QuoteManager(self.api_client, sync_interval=60)
        
        # Create Pricing Engine with Quote Manager
        self.pricing_engine = PricingEngine(
            quote_manager=self.quote_manager if enable_auto_quoting else None,
            update_interval_seconds=pricing_update_interval
        )
        
        # Create RFQ Manager with Pricing Engine
        self.rfq_manager = RFQManager(
            self.api_client, 
            pricing_engine=self.pricing_engine,
            sync_interval=60
        )
        
        self.enable_auto_quoting = enable_auto_quoting
        
        # Configure WebSocket
        ws_config = WebSocketConfig(
            api_key=api_key,
            api_secret=api_secret,
            heartbeat_interval=5,
            reconnect_delay=5,
            max_reconnect_attempts=10
        )
        
        self.ws_manager = WebSocketManager(ws_config)
        
        # Register callbacks for different message types
        self.ws_manager.register_callback(
            MessageType.RFQ_MAKER,
            self.handle_rfq
        )
        self.ws_manager.register_callback(
            MessageType.RFQ_QUOTE,
            self.handle_quote_update
        )
        self.ws_manager.register_callback(
            MessageType.BLOCK_TRADE_DETAIL,
            self.handle_trade
        )
        
        # Statistics
        self.stats = {
            "rfqs_processed": 0,
            "quotes_attempted": 0,
            "quotes_successful": 0,
            "price_updates": 0,
            "quote_updates": 0
        }
        
        # Pricing update task
        self._pricing_update_task: Optional[asyncio.Task] = None
        self._running = False
    
    async def handle_rfq(self, data: dict):
        """Handle incoming RFQ from WebSocket"""
        # Pass to RFQ Manager for processing
        self.rfq_manager.handle_websocket_message(data)
        
        # Get RFQ details
        rfq_data = data.get('d', {})
        request_id = rfq_data.get('requestId')
        state = rfq_data.get('state')
        
        logger.info(f"Received RFQ {request_id} with state {state}")
        
        # Process active RFQs for quoting if auto-quoting is enabled
        if state == "ACTIVE" and self.enable_auto_quoting:
            await self.process_rfq_for_quoting(request_id)
    
    async def process_rfq_for_quoting(self, request_id: str):
        """Process an RFQ and potentially create a quote"""
        rfq = self.rfq_manager.get_rfq(request_id)
        if not rfq:
            logger.warning(f"RFQ {request_id} not found in manager")
            return
        
        if not rfq.is_active:
            logger.info(f"Skipping inactive RFQ {request_id}")
            return
        
        logger.info(f"Processing RFQ {request_id} for quoting:")
        logger.info(f"  - Expires in: {rfq.time_to_expiry:.0f} seconds")
        logger.info(f"  - Legs: {len(rfq.legs)}")
        for i, leg in enumerate(rfq.legs):
            logger.info(f"    Leg {i+1}: {leg.side} {leg.quantity} {leg.instrumentName}")
        
        self.stats["rfqs_processed"] += 1
        self.stats["quotes_attempted"] += 1
        
        # Try to create quote through pricing engine
        try:
            success = await self.pricing_engine.create_quotes_for_rfq(rfq)
            if success:
                self.stats["quotes_successful"] += 1
                # Track the quote in RFQ manager
                quotes = self.quote_manager.get_rfq_quotes(request_id)
                for quote in quotes:
                    if quote.is_open:
                        self.rfq_manager.add_quote_to_rfq(request_id, quote.quoteId)
            else:
                logger.debug(f"Could not create quote for RFQ {request_id}")
        except Exception as e:
            logger.error(f"Error creating quote for RFQ {request_id}: {e}")
    
    async def handle_quote_update(self, data: dict):
        """Handle quote status updates"""
        # Pass to Quote Manager for processing
        self.quote_manager.handle_websocket_message(data)
        
        quote_data = data.get('d', {})
        quote_id = quote_data.get('quoteId')
        state = quote_data.get('state')
        request_id = quote_data.get('requestId')
        
        logger.info(f"Quote {quote_id} updated to {state} for RFQ {request_id}")
    
    async def handle_trade(self, data: dict):
        """Handle executed trades"""
        trade_data = data.get('d', {})
        trade_id = trade_data.get('blockTradeId')
        quote_id = trade_data.get('quoteId')
        
        logger.info(f"Trade executed: {trade_id} from quote {quote_id}")
        
        # TODO: Update positions and risk management
        # Your trade processing logic here
        
        # Update quote manager with trade information if needed
        if quote_id:
            quote = self.quote_manager.get_quote(quote_id)
            if quote:
                logger.info(f"Trade executed for quote on RFQ {quote.requestId}")
    
    async def _pricing_update_loop(self):
        """Internal method to run periodic pricing and quote updates"""
        try:
            while self._running:
                try:
                    # Update all prices
                    await self.pricing_engine.update_prices()
                    self.stats["price_updates"] += 1
                    
                    # Update quotes for all active RFQs if auto-quoting is enabled
                    if self.enable_auto_quoting:
                        active_rfqs = self.rfq_manager.get_active_rfqs()
                        
                        if active_rfqs:
                            logger.info(f"Updating quotes for {len(active_rfqs)} active RFQs")
                            
                            for rfq in active_rfqs:
                                try:
                                    success = await self.pricing_engine.update_quotes_for_rfq(rfq)
                                    if success:
                                        self.stats["quote_updates"] += 1
                                        # Track the updated quotes in RFQ manager
                                        quotes = self.quote_manager.get_rfq_quotes(rfq.requestId)
                                        for quote in quotes:
                                            if quote.is_open:
                                                self.rfq_manager.add_quote_to_rfq(rfq.requestId, quote.quoteId)
                                except Exception as e:
                                    logger.error(f"Error updating quotes for RFQ {rfq.requestId}: {e}")
                    
                    await asyncio.sleep(self.pricing_engine.update_interval)
                    
                except Exception as e:
                    logger.error(f"Error in pricing update loop: {e}")
                    await asyncio.sleep(self.pricing_engine.update_interval)
                    
        except asyncio.CancelledError:
            logger.info("Pricing update loop cancelled")
        except Exception as e:
            logger.error(f"Fatal error in pricing update loop: {e}")
            self._running = False
    
    async def run(self):
        """Run the market maker with RFQ management"""
        # Start API client session
        async with self.api_client:
            # Start Quote Manager
            async with self.quote_manager:
                # Start Pricing Engine (initialization only)
                await self.pricing_engine.start()
                
                # Start the pricing update loop
                self._running = True
                self._pricing_update_task = asyncio.create_task(self._pricing_update_loop())
                logger.info(f"Started pricing update loop with {self.pricing_engine.update_interval}s interval")
                
                # Start RFQ Manager
                async with self.rfq_manager:
                    # Start WebSocket Manager
                    async with self.ws_manager:
                        # Log initial stats
                        ws_stats = self.ws_manager.get_stats()
                        rfq_stats = self.rfq_manager.get_stats()
                        quote_stats = self.quote_manager.get_stats()
                        pricing_stats = self.pricing_engine.get_summary()
                        
                        logger.info("=" * 50)
                        logger.info("Market Maker Started")
                        logger.info(f"Auto-quoting: {'ENABLED' if self.enable_auto_quoting else 'DISABLED'}")
                        logger.info(f"Active RFQs: {rfq_stats['active_rfqs']}")
                        logger.info(f"Instruments tracked: {pricing_stats['total_instruments']}")
                        logger.info(f"WebSocket State: {ws_stats['state']}")
                        logger.info("=" * 50)
                    
                        # Main loop
                        while True:
                            await asyncio.sleep(30)
                            
                            # Log periodic stats
                            ws_stats = self.ws_manager.get_stats()
                            rfq_stats = self.rfq_manager.get_stats()
                            quote_stats = self.quote_manager.get_stats()
                            pricing_stats = self.pricing_engine.get_summary()
                            
                            logger.info("-" * 50)
                            logger.info(f"WebSocket - Uptime: {ws_stats['uptime_seconds']:.0f}s, "
                                      f"Messages: {ws_stats['messages_received']}, "
                                      f"Errors: {ws_stats['errors']}")
                            logger.info(f"RFQ Manager - Active: {rfq_stats['active_rfqs']}, "
                                      f"Total Received: {rfq_stats['total_received']}, "
                                      f"Removed: {rfq_stats['total_removed']}, "
                                      f"Syncs: {rfq_stats['sync_count']}")
                            logger.info(f"Pricing - Instruments: {pricing_stats['total_instruments']}, "
                                      f"Priced: {pricing_stats['priced_instruments']}, "
                                      f"RFQs tracked: {pricing_stats['rfqs_tracked']}")
                            logger.info(f"Quotes - Active: {quote_stats['active_quotes']}, "
                                      f"Filled: {quote_stats['filled_quotes']}, "
                                      f"Total: {quote_stats['total_quotes']}")
                            
                            if self.enable_auto_quoting:
                                logger.info(f"Auto-Quoting - Processed: {self.stats['rfqs_processed']}, "
                                          f"Attempted: {self.stats['quotes_attempted']}, "
                                          f"Successful: {self.stats['quotes_successful']}, "
                                          f"Price Updates: {self.stats['price_updates']}, "
                                          f"Quote Updates: {self.stats['quote_updates']}")
                        
                            # List active RFQs
                            active_rfqs = self.rfq_manager.get_active_rfqs()
                            if active_rfqs:
                                logger.info(f"Active RFQs ({len(active_rfqs)}):")
                                for rfq in active_rfqs[:5]:  # Show first 5
                                    logger.info(f"  - {rfq.requestId}: "
                                              f"expires in {rfq.time_to_expiry:.0f}s, "
                                              f"{len(rfq.legs)} legs")
                            
                            logger.info("-" * 50)
                
                # Stop the pricing update loop
                self._running = False
                if self._pricing_update_task:
                    self._pricing_update_task.cancel()
                    try:
                        await self._pricing_update_task
                    except asyncio.CancelledError:
                        pass
                    logger.info("Stopped pricing update loop")
                
                # Stop pricing engine when exiting
                await self.pricing_engine.stop()


async def main():
    """Example main function"""
    # Logging is already configured at module import
    
    # Your API credentials
    load_dotenv()
    API_KEY = os.environ.get("API_KEY")
    API_SECRET = os.environ.get("API_SECRET")
    
    # Create and run market maker
    # Set enable_auto_quoting=True to automatically create quotes for RFQs
    market_maker = RFQMarketMakerIntegration(
        API_KEY, 
        API_SECRET,
        enable_auto_quoting=True,  # Set to True to enable automatic quoting
        pricing_update_interval=5    # Update prices every 5 seconds
    )
    await market_maker.run()


if __name__ == "__main__":
    asyncio.run(main())