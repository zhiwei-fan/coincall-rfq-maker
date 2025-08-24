import asyncio
from dotenv import load_dotenv
import logging
import os
import sys
import time

from websocket_client import WebSocketConfig, WebSocketManager, MessageType
from api_client import RfqAPI, CoincallCredential
from rfq_manager import RFQManager, RFQ

# Setup logging BEFORE other imports
from logging_config import setup_logging
setup_logging()

# Get logger for this module
logger = logging.getLogger(__name__)

class RFQMarketMakerIntegration:
    """RFQ Market Maker Bot with integrated RFQ management"""
    
    def __init__(self, api_key: str, api_secret: str):
        # Create API client
        credential = CoincallCredential(api_key=api_key, secret_key=api_secret)
        self.api_client = RfqAPI(credential=credential)
        
        # Create RFQ Manager with 60 second sync interval
        self.rfq_manager = RFQManager(self.api_client, sync_interval=60)
        
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
        
        # Track our quotes
        self.active_quotes = {}  # quote_id -> quote_data
        self.filled_quotes = {}
    
    async def handle_rfq(self, data: dict):
        """Handle incoming RFQ from WebSocket"""
        # Pass to RFQ Manager for processing
        self.rfq_manager.handle_websocket_message(data)
        
        # Get RFQ details
        rfq_data = data.get('d', {})
        request_id = rfq_data.get('requestId')
        state = rfq_data.get('state')
        
        logger.info(f"Received RFQ {request_id} with state {state}")
        
        # Process active RFQs for quoting
        if state == "ACTIVE":
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
        
        # TODO: Implement your quoting logic here
        # Example: Calculate prices, check risk limits, create quote via API
        # quote_response = await self.api_client.create_quote(request_id, quote_legs)
        # if quote_response:
        #     quote_id = quote_response.get('data', {}).get('quoteId')
        #     self.rfq_manager.add_quote_to_rfq(request_id, quote_id)
    
    async def handle_quote_update(self, data: dict):
        """Handle quote status updates"""
        quote_data = data.get('d', {})
        quote_id = quote_data.get('quoteId')
        state = quote_data.get('state')
        request_id = quote_data.get('requestId')
        
        logger.info(f"Quote {quote_id} updated to {state} for RFQ {request_id}")
        
        # Update quote tracking
        if state == "OPEN":
            self.active_quotes[quote_id] = quote_data
        elif state == "FILLED":
            if quote_id in self.active_quotes:
                del self.active_quotes[quote_id]
            self.filled_quotes[quote_id] = quote_data
        elif state == "CANCELED":
            if quote_id in self.active_quotes:
                del self.active_quotes[quote_id]
    
    async def handle_trade(self, data: dict):
        """Handle executed trades"""
        trade_data = data.get('d', {})
        trade_id = trade_data.get('blockTradeId')
        quote_id = trade_data.get('quoteId')
        
        logger.info(f"Trade executed: {trade_id} from quote {quote_id}")
        
        # TODO: Update positions and risk management
        # Your trade processing logic here
    
    async def run(self):
        """Run the market maker with RFQ management"""
        # Start API client session
        async with self.api_client:
            # Start RFQ Manager
            async with self.rfq_manager:
                # Start WebSocket Manager
                async with self.ws_manager:
                    # Log initial stats
                    ws_stats = self.ws_manager.get_stats()
                    rfq_stats = self.rfq_manager.get_stats()
                    
                    logger.info("=" * 50)
                    logger.info("Market Maker Started")
                    logger.info(f"Active RFQs: {rfq_stats['active_rfqs']}")
                    logger.info(f"WebSocket State: {ws_stats['state']}")
                    logger.info("=" * 50)
                    
                    # Main loop
                    while True:
                        await asyncio.sleep(30)
                        
                        # Log periodic stats
                        ws_stats = self.ws_manager.get_stats()
                        rfq_stats = self.rfq_manager.get_stats()
                        
                        logger.info("-" * 50)
                        logger.info(f"WebSocket - Uptime: {ws_stats['uptime_seconds']:.0f}s, "
                                  f"Messages: {ws_stats['messages_received']}, "
                                  f"Errors: {ws_stats['errors']}")
                        logger.info(f"RFQ Manager - Active: {rfq_stats['active_rfqs']}, "
                                  f"Total Received: {rfq_stats['total_received']}, "
                                  f"Removed: {rfq_stats['total_removed']}, "
                                  f"Syncs: {rfq_stats['sync_count']}")
                        logger.info(f"Quotes - Active: {len(self.active_quotes)}, "
                                  f"Filled: {len(self.filled_quotes)}")
                        
                        # List active RFQs
                        active_rfqs = self.rfq_manager.get_active_rfqs()
                        if active_rfqs:
                            logger.info(f"Active RFQs ({len(active_rfqs)}):")
                            for rfq in active_rfqs[:5]:  # Show first 5
                                logger.info(f"  - {rfq.requestId}: "
                                          f"expires in {rfq.time_to_expiry:.0f}s, "
                                          f"{len(rfq.legs)} legs")
                        
                        logger.info("-" * 50)


async def main():
    """Example main function"""
    # Logging is already configured at module import
    
    # Your API credentials
    load_dotenv()
    API_KEY = os.environ.get("API_KEY")
    API_SECRET = os.environ.get("API_SECRET")
    
    # Create and run market maker
    market_maker = RFQMarketMakerIntegration(API_KEY, API_SECRET)
    await market_maker.run()


if __name__ == "__main__":
    asyncio.run(main())