import asyncio
from datetime import datetime, timedelta
import hmac
import json
import hashlib
import logging
import time
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException
from websockets.protocol import State

from typing import Callable, Optional, Any
from dataclasses import dataclass, field
from enum import Enum

# Setup logging BEFORE other imports
from logging_config import setup_logging
setup_logging()

# Get logger for this module
logger = logging.getLogger(__name__)

class ConnectionState(Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    AUTHENTICATED = "authenticated"
    SUBSCRIBED = "subscribed"
    RECONNECTING = "reconnecting"
    ERROR = "error"

class MessageType(Enum):
    RFQ_MAKER = 28
    RFQ_QUOTE = 20
    BLOCK_TRADE_DETAIL = 22
    BLOCK_TRADE_PUBLIC = 23
    HEARTBEAT = 1
    AUTH_SUCCESS = 2
    SUBSCRIPTION_SUCCESS = 3
    ERROR = 99

@dataclass
class WebSocketConfig:
    """Configuration for WebSocket connection"""
    api_key: str
    api_secret: str
    base_url: str = "wss://betaws.seizeyouralpha.com/options"
    heartbeat_interval: int = 30
    reconnect_delay: int = 5
    max_reconnect_attempts: int = 10
    connection_timeout: int = 30
    message_timeout: int = 60
    subscriptions: set[str] = field(default_factory=lambda: {
        "rfqMaker", "rfqQuote", "blockTradeDetail", "blockTradePublic"
    })

class WebSocketManager:
    """
    Enhanced WebSocket Manager for RFQ Market Maker Bot
    
    Features:
    - Automatic reconnection with exponential backoff
    - Connection health monitoring
    - Message routing and callback system
    - Subscription management
    - Error handling and recovery
    - Performance metrics
    """
    
    def __init__(self, config: WebSocketConfig):
        self.config = config
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.state = ConnectionState.DISCONNECTED
        self.reconnect_attempts = 0
        self.last_message_time = time.time()
        self.last_heartbeat_time = time.time()
        
        # Callback handlers
        self.callbacks: dict[MessageType, list[Callable]] = {
            msg_type: [] for msg_type in MessageType
        }
        
        # Message statistics
        self.stats = {
            'messages_received': 0,
            'messages_sent': 0,
            'errors': 0,
            'reconnections': 0,
            'uptime_start': None
        }
        
        # Active subscriptions
        self.active_subscriptions: set[str] = set()
        
        # Control flags
        self.running = False
        self.tasks: list[asyncio.Task] = []
        
    def get_signed_url(self) -> str:
        """Generate signed WebSocket URL with authentication"""
        ts = int(time.time() * 1000)
        verb = 'GET'
        uri = '/users/self/verify'
        auth = f"{verb}{uri}?uuid={self.config.api_key}&ts={ts}"
        
        signature = hmac.new(
            self.config.api_secret.encode('utf-8'),
            auth.encode('utf-8'),
            hashlib.sha256
        ).hexdigest().upper()
        
        params = f"?code=10&uuid={self.config.api_key}&ts={ts}&sign={signature}&apiKey={self.config.api_key}"
        return self.config.base_url + params
    
    def register_callback(self, message_type: MessageType, callback: Callable):
        """Register a callback for specific message type"""
        if message_type not in self.callbacks:
            self.callbacks[message_type] = []
        self.callbacks[message_type].append(callback)
        logger.info(f"Registered callback for {message_type.name}")
    
    def unregister_callback(self, message_type: MessageType, callback: Callable):
        """Unregister a callback"""
        if message_type in self.callbacks and callback in self.callbacks[message_type]:
            self.callbacks[message_type].remove(callback)
    
    async def connect(self) -> bool:
        """Establish WebSocket connection with retry logic"""
        if self.state == ConnectionState.CONNECTED:
            logger.warning("Already connected")
            return True
        
        self.state = ConnectionState.CONNECTING
        
        while self.reconnect_attempts < self.config.max_reconnect_attempts:
            try:
                url = self.get_signed_url()
                logger.info(f"Attempting connection (attempt {self.reconnect_attempts + 1})")
                
                # Connect with timeout
                self.ws = await asyncio.wait_for(
                    websockets.connect(
                        url,
                        ping_interval=20,
                        ping_timeout=10,
                        close_timeout=10
                    ),
                    timeout=self.config.connection_timeout
                )
                
                self.state = ConnectionState.CONNECTED
                self.reconnect_attempts = 0
                self.stats['uptime_start'] = time.time()
                
                logger.info("WebSocket connection established")
                
                # Subscribe to channels
                await self._subscribe_all()
                
                return True
                
            except asyncio.TimeoutError:
                logger.error(f"Connection timeout after {self.config.connection_timeout}s")
                self.reconnect_attempts += 1
                
            except Exception as e:
                logger.error(f"Connection failed: {e}")
                self.reconnect_attempts += 1
                
            # Exponential backoff
            delay = min(self.config.reconnect_delay * (2 ** self.reconnect_attempts), 60)
            logger.info(f"Retrying in {delay} seconds...")
            await asyncio.sleep(delay)
        
        self.state = ConnectionState.ERROR
        logger.error("Max reconnection attempts reached")
        return False
    
    async def _subscribe_all(self):
        """Subscribe to all configured channels"""
        for data_type in self.config.subscriptions:
            await self.subscribe(data_type)
        
        self.state = ConnectionState.SUBSCRIBED
        logger.info(f"Subscribed to {len(self.active_subscriptions)} channels")
    
    async def subscribe(self, data_type: str):
        """Subscribe to a specific data channel"""
        if not self.ws or self.ws.state != State.OPEN:
            logger.error("Cannot subscribe: WebSocket not connected")
            return False
        
        try:
            msg = {
                "action": "subscribe",
                "dataType": data_type
            }
            await self.ws.send(json.dumps(msg))
            self.active_subscriptions.add(data_type)
            self.stats['messages_sent'] += 1
            logger.info(f"Subscribed to {data_type}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to subscribe to {data_type}: {e}")
            return False
    
    async def unsubscribe(self, data_type: str):
        """Unsubscribe from a data channel"""
        if not self.ws or self.ws.state != State.OPEN:
            return False
        
        try:
            msg = {
                "action": "unsubscribe",
                "dataType": data_type
            }
            await self.ws.send(json.dumps(msg))
            self.active_subscriptions.discard(data_type)
            logger.info(f"Unsubscribed from {data_type}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to unsubscribe from {data_type}: {e}")
            return False
    
    async def send_message(self, message: dict[str, Any]):
        """Send a message through WebSocket"""
        if not self.ws or self.ws.state != State.OPEN:
            logger.error("Cannot send message: WebSocket not connected")
            return False
        
        try:
            await self.ws.send(json.dumps(message))
            self.stats['messages_sent'] += 1
            return True
            
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            self.stats['errors'] += 1
            return False
    
    async def _heartbeat_loop(self):
        """Send periodic heartbeat messages"""
        while self.running:
            try:
                if self.ws and self.ws.state == State.OPEN:
                    await self.send_message({"action": "heartbeat"})
                    self.last_heartbeat_time = time.time()
                    
                await asyncio.sleep(self.config.heartbeat_interval)
                
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
                await asyncio.sleep(5)
    
    async def _message_handler(self):
        """Main message handling loop"""
        while self.running:
            try:
                if not self.ws or self.ws.state != State.OPEN:
                    await asyncio.sleep(1)
                    continue
                
                # Wait for message with timeout
                try:
                    raw_message = await asyncio.wait_for(
                        self.ws.recv(),
                        timeout=self.config.message_timeout
                    )
                    
                    self.last_message_time = time.time()
                    self.stats['messages_received'] += 1
                    
                    # Parse and route message
                    await self._process_message(raw_message)
                    
                except asyncio.TimeoutError:
                    # Check if connection is still healthy
                    if time.time() - self.last_message_time > self.config.message_timeout * 2:
                        logger.warning("No messages received, connection may be stale")
                        await self._reconnect()
                        
            except ConnectionClosed as e:
                logger.warning(f"Connection closed: {e}")
                if self.running:
                    await self._reconnect()
                    
            except WebSocketException as e:
                logger.error(f"WebSocket error: {e}")
                self.stats['errors'] += 1
                if self.running:
                    await self._reconnect()
                    
            except Exception as e:
                logger.error(f"Unexpected error in message handler: {e}")
                self.stats['errors'] += 1
                await asyncio.sleep(1)
    
    async def _process_message(self, raw_message: str):
        """Process and route incoming messages"""
        try:
            data = json.loads(raw_message)
            logger.debug(f"Raw message: {raw_message}")
            
            # Determine message type
            msg_type = self._get_message_type(data)
            
            # Log message based on type
            if msg_type == MessageType.ERROR:
                logger.info(f"Received unknown message: {raw_message}")
            
            # Execute callbacks
            if msg_type in self.callbacks:
                for callback in self.callbacks[msg_type]:
                    try:
                        # Run callback in background to avoid blocking
                        asyncio.create_task(self._safe_callback(callback, data))
                    except Exception as e:
                        logger.error(f"Callback error for {msg_type.name}: {e}")
                        
        except json.JSONDecodeError:
            logger.warning(f"Received non-JSON message: {raw_message[:100]}")
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            self.stats['errors'] += 1
    
    async def _safe_callback(self, callback: Callable, data: dict):
        """Execute callback with error handling"""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(data)
            else:
                callback(data)
        except Exception as e:
            logger.error(f"Callback execution failed: {e}")
    
    def _get_message_type(self, data: dict) -> MessageType:
        """Map message to MessageType enum"""
        # Check for subscription success messages
        if data.get('action') == 'SUBSCRIBE' and data.get('result') == 'success':
            return MessageType.SUBSCRIPTION_SUCCESS
        
        # Check for heartbeat (c=11, rc=1)
        if data.get('c') == 11 and data.get('rc') == 1:
            return MessageType.HEARTBEAT
        
        # Check for data type field
        dt = data.get('dt')
        if dt is not None:
            mapping = {
                28: MessageType.RFQ_MAKER,
                20: MessageType.RFQ_QUOTE,
                22: MessageType.BLOCK_TRADE_DETAIL,
                23: MessageType.BLOCK_TRADE_PUBLIC,
            }
            return mapping.get(dt, MessageType.ERROR)
        
        return MessageType.ERROR
    
    async def _reconnect(self):
        """Handle reconnection logic"""
        if self.state == ConnectionState.RECONNECTING:
            return
        
        self.state = ConnectionState.RECONNECTING
        self.stats['reconnections'] += 1
        
        logger.info("Initiating reconnection...")
        
        # Close existing connection
        if self.ws:
            await self.ws.close()
            self.ws = None
        
        # Clear active subscriptions (will resubscribe on connect)
        self.active_subscriptions.clear()
        
        # Attempt reconnection
        success = await self.connect()
        
        if not success:
            logger.error("Reconnection failed")
            self.state = ConnectionState.ERROR
    
    async def _monitor_connection(self):
        """Monitor connection health and reconnect if needed"""
        while self.running:
            try:
                await asyncio.sleep(10)  # Check every 10 seconds
                
                if self.ws and self.ws.state == State.OPEN:
                    # Check if we're receiving heartbeats
                    time_since_heartbeat = time.time() - self.last_heartbeat_time
                    time_since_message = time.time() - self.last_message_time
                    
                    if time_since_heartbeat > self.config.heartbeat_interval * 3:
                        logger.warning("Heartbeat timeout detected")
                        await self._reconnect()
                        
                    elif time_since_message > self.config.message_timeout * 2:
                        logger.warning("Message timeout detected")
                        await self._reconnect()
                        
                elif self.state != ConnectionState.RECONNECTING:
                    logger.warning("Connection lost, attempting reconnect")
                    await self._reconnect()
                    
            except Exception as e:
                logger.error(f"Monitor error: {e}")
    
    async def start(self):
        """Start the WebSocket manager"""
        if self.running:
            logger.warning("WebSocket manager already running")
            return
        
        self.running = True
        logger.info("Starting WebSocket manager")
        
        # Connect
        success = await self.connect()
        if not success:
            raise ConnectionError("Failed to establish WebSocket connection")
        
        # Start background tasks
        self.tasks = [
            asyncio.create_task(self._heartbeat_loop()),
            asyncio.create_task(self._message_handler()),
            asyncio.create_task(self._monitor_connection())
        ]
        
        logger.info("WebSocket manager started successfully")
    
    async def stop(self):
        """Stop the WebSocket manager"""
        logger.info("Stopping WebSocket manager")
        self.running = False
        
        # Cancel all tasks
        for task in self.tasks:
            task.cancel()
        
        # Wait for tasks to complete
        await asyncio.gather(*self.tasks, return_exceptions=True)
        
        # Close WebSocket connection
        if self.ws:
            await self.ws.close()
            self.ws = None
        
        self.state = ConnectionState.DISCONNECTED
        logger.info("WebSocket manager stopped")
    
    def get_stats(self) -> dict:
        """Get connection statistics"""
        uptime = None
        if self.stats['uptime_start']:
            uptime = time.time() - self.stats['uptime_start']
        
        return {
            **self.stats,
            'state': self.state.value,
            'uptime_seconds': uptime,
            'active_subscriptions': list(self.active_subscriptions),
            'last_message_ago': time.time() - self.last_message_time,
            'last_heartbeat_ago': time.time() - self.last_heartbeat_time
        }
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.stop()


