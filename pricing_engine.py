"""
Pricing calculations and instrument management.
"""

import asyncio
import logging
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional

import numpy as np
from scipy.stats import norm

from api_client import FuturesAPI
from interfaces import IPricingService, IInstrumentRepository
from models import Instrument, PricedInstrument, PriceQuote
from market_data import CoincallMarketDataProvider

logger = logging.getLogger(__name__)


# ============================================================================
# Core Pricing Components
# ============================================================================

class InstrumentParser:
    """Parses instrument strings into Instrument objects"""
    
    @staticmethod
    def parse(instrument_str: str) -> Instrument:
        """
        Parse instrument string (e.g., BTCUSD-21AUG25-120000-C)
        """
        parts = instrument_str.split('-')
        if len(parts) != 4:
            raise ValueError(f"Invalid instrument format: {instrument_str}")
        
        underlying = parts[0]
        expiry_str = parts[1]
        strike = float(parts[2])
        option_type = parts[3].upper()
        
        # Parse expiry date
        month_start = 0
        for i, char in enumerate(expiry_str):
            if char.isalpha():
                month_start = i
                break
        
        day = int(expiry_str[:month_start])
        month_str = expiry_str[month_start:month_start+3].upper()
        year = 2000 + int(expiry_str[month_start+3:])
        
        months = {
            'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
            'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
        }
        
        month = months.get(month_str)
        if month is None:
            raise ValueError(f"Invalid month: {month_str}")
        
        if option_type not in ['C', 'P']:
            raise ValueError(f"Invalid option type: {option_type}")
        
        expiry_date = datetime(year, month, day)
        
        return Instrument(
            instrument_str=instrument_str,
            underlying=underlying,
            expiry_date=expiry_date,
            strike=strike,
            option_type=option_type
        )


class BlackScholesPricer:
    """Calculates option prices using Black-Scholes model"""
    
    def __init__(self, bid_vol: float = 0.20, ask_vol: float = 2.00, 
                 risk_free_rate: float = 0.05):
        self.bid_vol = bid_vol
        self.ask_vol = ask_vol
        self.risk_free_rate = risk_free_rate
    
    def calculate_prices(self, instrument: Instrument, index_price: float) -> PricedInstrument:
        """Calculate bid/ask prices for an instrument"""
        time_to_expiry = self._calculate_tte(instrument.expiry_date)
        
        priced = PricedInstrument(instrument=instrument)
        priced.index_price = index_price
        
        if time_to_expiry <= 0:
            priced.bid_price = 0.0
            priced.ask_price = 0.0
        else:
            priced.bid_price = self._black_scholes(
                S=index_price,
                K=instrument.strike,
                T=time_to_expiry,
                r=self.risk_free_rate,
                sigma=self.bid_vol,
                option_type=instrument.option_type
            )
            priced.ask_price = self._black_scholes(
                S=index_price,
                K=instrument.strike,
                T=time_to_expiry,
                r=self.risk_free_rate,
                sigma=self.ask_vol,
                option_type=instrument.option_type
            )
        
        priced.last_priced = datetime.now()
        return priced
    
    @staticmethod
    def _calculate_tte(expiry_date: datetime) -> float:
        """Calculate time to expiry in years"""
        diff = expiry_date - datetime.now()
        days = diff.total_seconds() / (60 * 60 * 24)
        return max(days / 365, 0)
    
    @staticmethod
    def _black_scholes(S: float, K: float, T: float, r: float, 
                      sigma: float, option_type: str) -> float:
        """Black-Scholes formula implementation"""
        if T <= 0:
            return 0.0
        
        d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)
        
        if option_type == 'C':
            price = S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
        elif option_type == 'P':
            price = K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
        else:
            raise ValueError(f"Invalid option type: {option_type}")
        
        return max(price, 0.0)


# ============================================================================
# Repository Implementation
# ============================================================================

class InstrumentRepository(IInstrumentRepository):
    """In-memory instrument storage"""
    
    def __init__(self, parser: InstrumentParser):
        self.parser = parser
        self.instruments: Dict[str, Instrument] = {}
        self.priced_instruments: Dict[str, PricedInstrument] = {}
    
    def add_instrument(self, instrument_name: str) -> bool:
        """Add instrument by parsing its name"""
        try:
            if instrument_name not in self.instruments:
                instrument = self.parser.parse(instrument_name)
                self.instruments[instrument_name] = instrument
                logger.info(f"Added instrument: {instrument_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to add instrument {instrument_name}: {e}")
            return False
    
    def get_instrument(self, instrument_name: str) -> Optional[Instrument]:
        """Get instrument definition"""
        return self.instruments.get(instrument_name)
    
    def get_all_instruments(self) -> List[Instrument]:
        """Get all instruments"""
        return list(self.instruments.values())
    
    def remove_instrument(self, instrument_name: str) -> bool:
        """Remove instrument"""
        if instrument_name in self.instruments:
            del self.instruments[instrument_name]
            if instrument_name in self.priced_instruments:
                del self.priced_instruments[instrument_name]
            logger.info(f"Removed instrument: {instrument_name}")
            return True
        return False
    
    def store_priced(self, priced: PricedInstrument) -> None:
        """Store priced instrument"""
        self.priced_instruments[priced.instrument.instrument_str] = priced
    
    def get_priced(self, instrument_name: str) -> Optional[PricedInstrument]:
        """Get priced instrument"""
        return self.priced_instruments.get(instrument_name)

# ============================================================================
# Main Pricing Service
# ============================================================================

class SimplePricingService(IPricingService):
    """
    Simplified pricing service focused only on pricing operations.
    No quote creation or RFQ tracking.
    """
    
    def __init__(self, 
                 futures_api: FuturesAPI,
                 repository: Optional[InstrumentRepository] = None,
                 pricer: Optional[BlackScholesPricer] = None,
                 market_data: Optional[CoincallMarketDataProvider] = None):
        """Initialize with dependencies"""
        self.repository = repository or InstrumentRepository(InstrumentParser())
        self.pricer = pricer or BlackScholesPricer()
        self.market_data = market_data or CoincallMarketDataProvider(futures_api)
    
    async def calculate_price(self, instrument_name: str) -> Optional[PriceQuote]:
        """Calculate current price for an instrument"""
        instrument = self.repository.get_instrument(instrument_name)
        if not instrument:
            logger.warning(f"Instrument {instrument_name} not found")
            return None
        
        index_price = self.market_data.get_cached_price(instrument.underlying)
        if index_price is None:
            logger.warning(f"No index price for {instrument.underlying}")
            return None
        
        priced = self.pricer.calculate_prices(instrument, index_price)
        self.repository.store_priced(priced)
        
        return PriceQuote(
            instrument_name=instrument_name,
            bid=priced.bid_price,
            ask=priced.ask_price,
            mid=priced.mid_price,
            spread=priced.spread,
            timestamp=datetime.now()
        )
    
    async def update_market_data(self) -> int:
        """Update market data for all instruments"""
        instruments = self.repository.get_all_instruments()
        if not instruments:
            logger.debug("No instruments to price")
            return 0
        
        # Get unique underlyings
        underlyings = set(inst.underlying for inst in instruments)
        
        # Fetch latest prices
        await self.market_data.fetch_index_prices(list(underlyings))
        
        # Price all instruments
        priced_count = 0
        for instrument in instruments:
            price_quote = await self.calculate_price(instrument.instrument_str)
            if price_quote:
                priced_count += 1
        
        logger.info(f"Priced {priced_count}/{len(instruments)} instruments")
        return priced_count
    
    def get_instrument(self, instrument_name: str) -> Optional[PricedInstrument]:
        """Get instrument with current pricing"""
        return self.repository.get_priced(instrument_name)
    
    def add_instrument(self, instrument_name: str) -> bool:
        """Add instrument to be priced"""
        return self.repository.add_instrument(instrument_name)
    
    def remove_instrument(self, instrument_name: str) -> bool:
        """Remove instrument"""
        return self.repository.remove_instrument(instrument_name)
    
    def set_volatility(self, bid_vol: float, ask_vol: float) -> None:
        """Update volatility parameters"""
        self.pricer.bid_vol = bid_vol
        self.pricer.ask_vol = ask_vol
        logger.info(f"Updated volatilities: Bid={bid_vol:.1%}, Ask={ask_vol:.1%}")
    
    def set_risk_free_rate(self, rate: float) -> None:
        """Update risk-free rate"""
        self.pricer.risk_free_rate = rate
        logger.info(f"Updated risk-free rate to {rate:.1%}")
    
    def set_index_price(self, symbol: str, price: float) -> None:
        """Manually set index price"""
        self.market_data.set_price(symbol, price)
    
    def get_summary(self) -> dict:
        """Get summary statistics"""
        instruments = self.repository.get_all_instruments()
        priced = [self.repository.get_priced(i.instrument_str) 
                  for i in instruments]
        priced = [p for p in priced if p and p.is_priced]
        
        return {
            'total_instruments': len(instruments),
            'priced_instruments': len(priced),
            'unique_underlyings': len(set(i.underlying for i in instruments)),
            'bid_volatility': self.pricer.bid_vol,
            'ask_volatility': self.pricer.ask_vol,
            'risk_free_rate': self.pricer.risk_free_rate
        }