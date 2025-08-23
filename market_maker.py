import asyncio
from dotenv import load_dotenv
import logging
import os
import sys

from websocket_client import WebSocketConfig, WebSocketManager, MessageType

# Setup logging BEFORE other imports
from logging_config import setup_logging
setup_logging()

# Get logger for this module
logger = logging.getLogger(__name__)

class RFQMarketMakerIntegration:
    """Example integration with RFQ Market Maker Bot"""
    
    def __init__(self, api_key: str, api_secret: str):
        # Configure WebSocket
        ws_config = WebSocketConfig(
            api_key=api_key,
            api_secret=api_secret,
            heartbeat_interval=30,
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
    
    async def handle_rfq(self, data: dict):
        """Handle incoming RFQ"""
        rfq_data = data.get('d', {})
        request_id = rfq_data.get('requestId')
        state = rfq_data.get('state')
        
        logger.info(f"Received RFQ {request_id} with state {state}")
        
        # Process RFQ for quoting
        if state == "ACTIVE":
            # Your RFQ processing logic here
            pass
    
    async def handle_quote_update(self, data: dict):
        """Handle quote status updates"""
        quote_data = data.get('d', {})
        quote_id = quote_data.get('quoteId')
        state = quote_data.get('state')
        
        logger.info(f"Quote {quote_id} updated to {state}")
        
        # Update internal quote tracking
        # Your quote management logic here
    
    async def handle_trade(self, data: dict):
        """Handle executed trades"""
        trade_data = data.get('d', {})
        trade_id = trade_data.get('blockTradeId')
        
        logger.info(f"Trade executed: {trade_id}")
        
        # Update positions and risk
        # Your trade processing logic here
    
    async def run(self):
        """Run the market maker"""
        async with self.ws_manager:
            # Get initial stats
            stats = self.ws_manager.get_stats()
            logger.info(f"WebSocket Stats: {stats}")
            
            # Keep running
            while True:
                await asyncio.sleep(60)
                
                # Periodically log stats
                stats = self.ws_manager.get_stats()
                logger.info(f"Uptime: {stats['uptime_seconds']:.0f}s, "
                          f"Messages: {stats['messages_received']}, "
                          f"Errors: {stats['errors']}")


async def main():
    """Example main function"""
    # Logging is already configured at module import
    
    # Your API credentials
    load_dotenv()
    API_KEY = os.environ.get("API_KEY")
    API_SECRET = os.environ.get("API_SECRET")
    print(API_KEY, API_SECRET)
    
    # Create and run market maker
    market_maker = RFQMarketMakerIntegration(API_KEY, API_SECRET)
    await market_maker.run()


if __name__ == "__main__":
    asyncio.run(main())