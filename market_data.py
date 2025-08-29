import asyncio
from datetime import datetime
from typing import Dict, List, Optional
import logging

from interfaces import IMarketDataProvider
from models import Instrument, PricedInstrument, PriceQuote, MarketData
from api_client import FuturesAPI, CoincallCredential, CcAPIException

logger = logging.getLogger(__name__)

# ============================================================================
# Market Data Provider
# ============================================================================

class CoincallMarketDataProvider(IMarketDataProvider):
    """Market data provider that uses Coincall API for real prices"""
    
    def __init__(self, futures_api: FuturesAPI):
        self.futures_api = futures_api
        self.cached_prices: Dict[str, float] = {}
        self.last_fetch: Optional[datetime] = None
    
    async def fetch_index_prices(self, symbols: List[str]) -> Dict[str, float]:
        """Fetch latest index prices from Coincall API"""
        new_prices = {}
        
        for symbol in symbols:
            try:
                # Get symbol info which includes indexPrice
                response = await self.futures_api.get_symbol_info(symbol)
                
                if response.get('code') == 0 and response.get('data'):
                    data = response['data']
                    # Use indexPrice if available, otherwise use markPrice
                    index_price = data.get('indexPrice')
                    if index_price:
                        new_prices[symbol] = float(index_price)
                        logger.info(f"Fetched {symbol} index price: {index_price}")
                    else:
                        # Fallback to markPrice if indexPrice not available
                        mark_price = data.get('markPrice')
                        if mark_price:
                            new_prices[symbol] = float(mark_price)
                            logger.warning(f"Using markPrice for {symbol}: {mark_price}")
                else:
                    logger.error(f"Failed to fetch price for {symbol}: {response.get('msg')}")
                    
            except CcAPIException as e:
                logger.error(f"API error fetching {symbol}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error fetching {symbol}: {e}")
        
        # Update cache
        if new_prices:
            self.cached_prices.update(new_prices)
            self.last_fetch = datetime.now()
            logger.info(f"Updated {len(new_prices)} prices at {self.last_fetch}")
        
        return new_prices
    
    def get_cached_price(self, symbol: str) -> Optional[float]:
        """Get cached price for symbol"""
        return self.cached_prices.get(symbol)
    
    def set_price(self, symbol: str, price: float) -> None:
        """Manually set a price"""
        self.cached_prices[symbol] = price
        logger.info(f"Set {symbol} price to {price}")