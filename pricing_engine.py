import aiohttp
import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import math
import numpy as np
import random
from scipy.stats import norm
from typing import Dict, List, Optional, Set

@dataclass
class Instrument:
    """
    Option instrument with integrated pricing capability.
    Combines static definition with dynamic pricing data.
    """
    # Static fields (instrument definition)
    instrument_str: str
    underlying: str
    expiry_date: datetime
    strike: float
    option_type: str  # 'C' or 'P'
    
    # Dynamic fields (market data and calculated prices)
    index_price: Optional[float] = None
    bid_price: Optional[float] = None
    ask_price: Optional[float] = None
    last_priced: Optional[datetime] = None
    
    # Internal field for caching time to expiry
    _cached_tte: Optional[float] = field(default=None, init=False, repr=False)
    _cached_tte_date: Optional[datetime] = field(default=None, init=False, repr=False)
    
    @property
    def time_to_expiry(self) -> float:
        """
        Calculate time to expiry in years.
        Cached for the current day to avoid recalculation.
        """
        today = datetime.now().date()
        if self._cached_tte_date != today:
            diff = self.expiry_date - datetime.now()
            days = diff.total_seconds() / (60 * 60 * 24)
            self._cached_tte = max(days / 365, 0)
            self._cached_tte_date = today
        return self._cached_tte
    
    @property
    def mid_price(self) -> Optional[float]:
        """Calculate mid price from bid and ask"""
        if self.bid_price is not None and self.ask_price is not None:
            return (self.bid_price + self.ask_price) / 2
        return None
    
    @property
    def spread(self) -> Optional[float]:
        """Calculate spread between bid and ask"""
        if self.bid_price is not None and self.ask_price is not None:
            return self.ask_price - self.bid_price
        return None
    
    @property
    def is_priced(self) -> bool:
        """Check if instrument has been priced"""
        return self.last_priced is not None
    
    @property
    def is_expired(self) -> bool:
        """Check if option has expired"""
        return self.time_to_expiry <= 0
    
    def update_prices(self, index_price: float, bid_vol: float, 
                     ask_vol: float, risk_free_rate: float) -> None:
        """
        Update prices for this instrument using Black-Scholes model.
        
        Args:
            index_price: Current price of underlying asset
            bid_vol: Bid volatility
            ask_vol: Ask volatility  
            risk_free_rate: Risk-free interest rate
        """
        self.index_price = index_price
        
        if self.is_expired:
            # Expired options have no value
            self.bid_price = 0.0
            self.ask_price = 0.0
        else:
            self.bid_price = self._black_scholes(
                index_price, self.strike, self.time_to_expiry,
                risk_free_rate, bid_vol, self.option_type
            )
            self.ask_price = self._black_scholes(
                index_price, self.strike, self.time_to_expiry,
                risk_free_rate, ask_vol, self.option_type
            )
        
        self.last_priced = datetime.now()
    
    @staticmethod
    def _black_scholes(S: float, K: float, T: float, r: float, 
                       sigma: float, option_type: str) -> float:
        """
        Calculate option price using Black-Scholes formula.
        
        Args:
            S: Spot price
            K: Strike price
            T: Time to expiry in years
            r: Risk-free rate
            sigma: Volatility
            option_type: 'C' for Call, 'P' for Put
            
        Returns:
            Option price
        """
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
        
        return max(price, 0.0)  # Option price cannot be negative
    
    def to_dict(self) -> Dict:
        """Convert instrument to dictionary for easy serialization"""
        return {
            'instrument': self.instrument_str,
            'underlying': self.underlying,
            'expiry_date': self.expiry_date.isoformat(),
            'strike': self.strike,
            'option_type': self.option_type,
            'time_to_expiry': self.time_to_expiry,
            'index_price': self.index_price,
            'bid_price': self.bid_price,
            'ask_price': self.ask_price,
            'mid_price': self.mid_price,
            'spread': self.spread,
            'last_priced': self.last_priced.isoformat() if self.last_priced else None,
            'is_expired': self.is_expired
        }


class PricingEngine:
    """
    Async cryptocurrency options pricing engine using Black-Scholes model.
    Single class design for instruments with integrated pricing.
    """
    
    def __init__(self, update_interval_seconds: int = 5):
        """
        Initialize the pricing engine.
        
        Args:
            update_interval_seconds: Interval between price updates in seconds
        """
        self.instruments: Dict[str, Instrument] = {}
        self.index_prices: Dict[str, float] = {}
        self.update_interval = update_interval_seconds
        self.bid_vol = 0.20  # 20% bid volatility
        self.ask_vol = 2.00  # 200% ask volatility
        self.risk_free_rate = 0.05  # 5% risk-free rate
        self._running = False
        self._update_task: Optional[asyncio.Task] = None
        self._session: Optional[aiohttp.ClientSession] = None
        
    async def __aenter__(self):
        """Async context manager entry"""
        self._session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.cleanup()
        
    async def cleanup(self):
        """Clean up resources"""
        if self._session:
            await self._session.close()
            self._session = None
        await self.stop()
        
    def parse_instrument_string(self, instrument_str: str) -> Instrument:
        """
        Parse instrument string and create Instrument object.
        Format: BTCUSD-21AUG25-120000-C
        
        Args:
            instrument_str: Instrument string to parse
            
        Returns:
            Instrument object
        """
        parts = instrument_str.split('-')
        if len(parts) != 4:
            raise ValueError(f"Invalid instrument format: {instrument_str}")
        
        underlying = parts[0]
        expiry_str = parts[1]
        strike = float(parts[2])
        option_type = parts[3].upper()
        
        # Parse expiry date (DDMMMYY format)
        day = int(expiry_str[:2])
        month_str = expiry_str[2:5].upper()
        year = 2000 + int(expiry_str[5:7])
        
        months = {
            'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
            'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
        }
        
        month = months.get(month_str)
        if month is None:
            raise ValueError(f"Invalid month: {month_str}")
        
        if option_type not in ['C', 'P']:
            raise ValueError(f"Invalid option type: {option_type}. Must be 'C' or 'P'")
            
        expiry_date = datetime(year, month, day)
        
        return Instrument(
            instrument_str=instrument_str,
            underlying=underlying,
            expiry_date=expiry_date,
            strike=strike,
            option_type=option_type
        )
    
    def add_instrument(self, instrument_str: str) -> bool:
        """
        Add instrument to the pricing engine.
        
        Args:
            instrument_str: Instrument string to add
            
        Returns:
            True if successful, False otherwise
        """
        try:
            instrument = self.parse_instrument_string(instrument_str)
            self.instruments[instrument_str] = instrument
            print(f"Added instrument: {instrument_str}")
            return True
        except Exception as e:
            print(f"Failed to add instrument: {e}")
            return False
    
    def remove_instrument(self, instrument_str: str) -> bool:
        """
        Remove instrument from the pricing engine.
        
        Args:
            instrument_str: Instrument string to remove
            
        Returns:
            True if successful, False otherwise
        """
        if instrument_str in self.instruments:
            del self.instruments[instrument_str]
            print(f"Removed instrument: {instrument_str}")
            return True
        print(f"Instrument not found: {instrument_str}")
        return False
    
    def get_instruments(self) -> List[str]:
        """Get list of all instrument identifiers"""
        return list(self.instruments.keys())
    
    def get_instrument(self, instrument_str: str) -> Optional[Instrument]:
        """Get specific instrument object"""
        return self.instruments.get(instrument_str)
    
    def get_priced_instruments(self) -> List[Instrument]:
        """Get all instruments that have been priced"""
        return [inst for inst in self.instruments.values() if inst.is_priced]
    
    def get_expired_instruments(self) -> List[Instrument]:
        """Get all expired instruments"""
        return [inst for inst in self.instruments.values() if inst.is_expired]
    
    def remove_expired_instruments(self) -> int:
        """Remove all expired instruments and return count removed"""
        expired = [inst.instrument_str for inst in self.instruments.values() 
                  if inst.is_expired]
        for instrument_str in expired:
            del self.instruments[instrument_str]
        
        if expired:
            print(f"Removed {len(expired)} expired instruments")
        return len(expired)

    async def price_all_instruments(self) -> int:
        """
        Price all instruments using current index prices.
        
        Returns:
            Number of instruments successfully priced
        """
        if not self.instruments:
            return 0
        
        # More Pythonic grouping
        instruments_by_underlying = defaultdict(list)
        for instrument in self.instruments.values():
            instruments_by_underlying[instrument.underlying].append(instrument)
        
        # Price all instruments for each underlying
        priced_count = 0
        pricing_tasks = []
        
        for underlying, instruments in instruments_by_underlying.items():
            index_price = self.index_prices.get(underlying)
            if index_price is None:
                print(f"Warning: No index price for {underlying}")
                continue
            
            # Create pricing tasks for concurrent execution
            for instrument in instruments:
                pricing_tasks.append(
                    self._price_instrument_async(instrument, index_price)
                )
        
        # Execute all pricing tasks concurrently
        results = await asyncio.gather(*pricing_tasks, return_exceptions=True)
        
        # Count successful pricings
        for result in results:
            if result is True:
                priced_count += 1
            elif isinstance(result, Exception):
                print(f"Pricing error: {result}")
        
        return priced_count
    
    async def _price_instrument_async(self, instrument: Instrument, 
                                     index_price: float) -> bool:
        """
        Price a single instrument asynchronously.
        
        Args:
            instrument: Instrument to price
            index_price: Current index price
            
        Returns:
            True if successful
        """
        try:
            # In a real system, this might involve async calculations or API calls
            instrument.update_prices(
                index_price=index_price,
                bid_vol=self.bid_vol,
                ask_vol=self.ask_vol,
                risk_free_rate=self.risk_free_rate
            )
            return True
        except Exception as e:
            print(f"Error pricing {instrument.instrument_str}: {e}")
            return False
    
    async def fetch_index_prices(self, symbols: List[str]) -> Dict[str, float]:
        """
        Fetch index prices for given symbols asynchronously.
        
        Args:
            symbols: List of underlying symbols
            
        Returns:
            Dictionary of symbol prices
        """
        async def fetch_single_price(symbol: str) -> tuple[str, float]:
            # Simulate network delay
            await asyncio.sleep(random.uniform(0.1, 0.3))
            
            # Simulate realistic prices
            base_prices = {
                'BTCUSD': 50000,
                'ETHUSD': 3000,
                'SOLUSD': 100,
                'ADAUSD': 0.5,
                'DOGEUSD': 0.1
            }
            
            base_price = base_prices.get(symbol, 1000)
            # Add market volatility (±5%)
            variation = 1 + (random.random() - 0.5) * 0.1
            return symbol, base_price * variation
        
        # Fetch all prices concurrently
        price_tasks = [fetch_single_price(symbol) for symbol in symbols]
        price_results = await asyncio.gather(*price_tasks)
        
        return dict(price_results)
    
    def set_index_price(self, symbol: str, price: float):
        """
        Manually set index price for a symbol.
        
        Args:
            symbol: Underlying symbol
            price: Price to set
        """
        self.index_prices[symbol] = price
        print(f"Updated {symbol} price to {price}")
    
    async def update_prices(self):
        """Update all prices asynchronously"""
        # Get unique underlying symbols
        symbols = set(inst.underlying for inst in self.instruments.values())
        
        if not symbols:
            print("No instruments to price")
            return
        
        # Fetch latest index prices
        new_prices = await self.fetch_index_prices(list(symbols))
        
        # Update stored index prices
        for symbol, price in new_prices.items():
            self.index_prices[symbol] = price
        
        # Price all instruments
        priced_count = await self.price_all_instruments()
        
        # Log summary
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n[{timestamp}] Priced {priced_count}/{len(self.instruments)} instruments")
        
        # Show sample prices (first 5)
        for i, instrument in enumerate(self.get_priced_instruments()[:5]):
            if instrument.bid_price is not None:
                print(f"  {instrument.instrument_str}: "
                      f"Bid=${instrument.bid_price:.2f}, "
                      f"Ask=${instrument.ask_price:.2f}, "
                      f"Mid=${instrument.mid_price:.2f}")
    
    async def _run_update_loop(self):
        """Internal method to run periodic updates asynchronously"""
        try:
            while self._running:
                await self.update_prices()
                await asyncio.sleep(self.update_interval)
        except asyncio.CancelledError:
            print("Update loop cancelled")
        except Exception as e:
            print(f"Error in update loop: {e}")
            self._running = False
    
    async def start(self):
        """Start automatic pricing updates"""
        if self._running:
            print("Pricing engine already running")
            return
        
        print(f"Starting pricing engine with {self.update_interval}s interval")
        self._running = True
        
        # Initial price update
        await self.update_prices()
        
        # Create and store the update task
        self._update_task = asyncio.create_task(self._run_update_loop())
    
    async def stop(self):
        """Stop automatic pricing updates"""
        self._running = False
        
        if self._update_task:
            self._update_task.cancel()
            try:
                await self._update_task
            except asyncio.CancelledError:
                pass
            self._update_task = None
            print("Pricing engine stopped")
    
    def set_volatility(self, bid_vol: float, ask_vol: float):
        """
        Set custom volatility parameters.
        
        Args:
            bid_vol: Bid volatility
            ask_vol: Ask volatility
        """
        self.bid_vol = bid_vol
        self.ask_vol = ask_vol
        print(f"Updated volatilities: Bid={bid_vol:.1%}, Ask={ask_vol:.1%}")
    
    def set_risk_free_rate(self, rate: float):
        """
        Set risk-free rate.
        
        Args:
            rate: Risk-free rate
        """
        self.risk_free_rate = rate
        print(f"Updated risk-free rate to {rate:.1%}")
    
    def get_summary(self) -> Dict:
        """Get summary statistics of the pricing engine"""
        priced = self.get_priced_instruments()
        expired = self.get_expired_instruments()
        
        return {
            'total_instruments': len(self.instruments),
            'priced_instruments': len(priced),
            'expired_instruments': len(expired),
            'unique_underlyings': len(set(inst.underlying for inst in self.instruments.values())),
            'bid_volatility': self.bid_vol,
            'ask_volatility': self.ask_vol,
            'risk_free_rate': self.risk_free_rate,
            'update_interval': self.update_interval
        }
    
    def export_prices(self) -> List[Dict]:
        """Export all priced instruments as list of dictionaries"""
        return [inst.to_dict() for inst in self.get_priced_instruments()]


# Example usage
async def main():
    """Example of using the simplified pricing engine"""
    
    # Create pricing engine
    engine = PricingEngine(update_interval_seconds=5)
    
    # Add various instruments
    instruments = [
        'BTCUSD-21AUG25-120000-C',  # BTC Call
        'BTCUSD-21AUG25-100000-P',  # BTC Put
        'ETHUSD-21AUG25-5000-C',    # ETH Call
        'ETHUSD-21SEP25-4000-P',    # ETH Put
        'SOLUSD-21AUG25-150-C',     # SOL Call
    ]
    
    for inst_str in instruments:
        engine.add_instrument(inst_str)
    
    # Set initial index prices
    engine.set_index_price('BTCUSD', 50000)
    engine.set_index_price('ETHUSD', 3000)
    engine.set_index_price('SOLUSD', 100)
    
    # Price once and display
    await engine.update_prices()
    
    print("\n" + "="*60)
    print("CURRENT PRICES")
    print("="*60)
    
    for instrument in engine.get_priced_instruments():
        print(f"\n{instrument.instrument_str}:")
        print(f"  Index: ${instrument.index_price:,.2f}")
        print(f"  Strike: ${instrument.strike:,.2f}")
        print(f"  Time to Expiry: {instrument.time_to_expiry:.3f} years")
        print(f"  Bid: ${instrument.bid_price:,.2f}")
        print(f"  Ask: ${instrument.ask_price:,.2f}")
        print(f"  Mid: ${instrument.mid_price:,.2f}")
        print(f"  Spread: ${instrument.spread:,.2f}")
    
    # Display summary
    print("\n" + "="*60)
    print("ENGINE SUMMARY")
    print("="*60)
    summary = engine.get_summary()
    for key, value in summary.items():
        print(f"{key}: {value}")
    
    # Start automatic updates
    await engine.start()
    
    # Run for 15 seconds
    await asyncio.sleep(15)
    
    # Stop the engine
    await engine.stop()
    
    # Export prices for external use
    prices_data = engine.export_prices()
    print(f"\nExported {len(prices_data)} priced instruments")


# Alternative: Using context manager
async def main_with_context():
    """Example using async context manager"""
    
    async with PricingEngine(update_interval_seconds=3) as engine:
        # Setup instruments
        engine.add_instrument('BTCUSD-21AUG25-120000-C')
        engine.add_instrument('ETHUSD-21AUG25-5000-C')
        
        # Start pricing
        await engine.start()
        
        # Run for 10 seconds
        await asyncio.sleep(10)
        
        # Automatic cleanup when exiting context


if __name__ == "__main__":
    # Run the main example
    asyncio.run(main())
    
    # Or use context manager version
    # asyncio.run(main_with_context())