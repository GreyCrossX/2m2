# app/services/trading/trading/orders.py
from __future__ import annotations

import time
import uuid
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from enum import Enum
import asyncio

from app.services.trading.main import app
from app.services.trading.config.trading_config import get_trading_config, get_symbol_config
from app.services.trading.logging.trade_logger import (
    log_order_created, log_order_filled, log_order_cancelled, log_error
)

# Import the actual Binance SDK
try:
    from binance.um_futures import UMFutures
except ImportError:
    # Fallback for development/testing
    UMFutures = None
    print("Warning: binance package not found. Using mock implementation.")

import logging

LOG = logging.getLogger("trading.orders")

class OrderSide(Enum):
    """Order side enumeration"""
    BUY = "BUY"
    SELL = "SELL"

class PositionSide(Enum):
    """Position side for hedge mode"""
    BOTH = "BOTH"  # One-way mode
    LONG = "LONG"  # Hedge mode
    SHORT = "SHORT"  # Hedge mode

class OrderType(Enum):
    """Binance order types"""
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"
    STOP_MARKET = "STOP_MARKET"
    TAKE_PROFIT = "TAKE_PROFIT"
    TAKE_PROFIT_MARKET = "TAKE_PROFIT_MARKET"
    TRAILING_STOP_MARKET = "TRAILING_STOP_MARKET"

class TimeInForce(Enum):
    """Time in force options"""
    GTC = "GTC"  # Good Till Cancel
    IOC = "IOC"  # Immediate or Cancel
    FOK = "FOK"  # Fill or Kill
    GTX = "GTX"  # Good Till Crossing
    GTD = "GTD"  # Good Till Date

class OrderStatus(Enum):
    """Order status enumeration"""
    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    PENDING_CANCEL = "PENDING_CANCEL"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"

@dataclass
class OrderRequest:
    """Order request structure matching Binance API"""
    symbol: str
    side: OrderSide
    type: OrderType
    quantity: Optional[float] = None
    price: Optional[float] = None
    stop_price: Optional[float] = None  # stopPrice in API
    time_in_force: TimeInForce = TimeInForce.GTC
    reduce_only: bool = False
    position_side: PositionSide = PositionSide.BOTH  # For hedge mode
    new_client_order_id: Optional[str] = None  # newClientOrderId in API
    close_position: bool = False  # For stop market close all
    working_type: str = "CONTRACT_PRICE"  # MARK_PRICE or CONTRACT_PRICE
    price_protect: bool = False
    new_order_resp_type: str = "ACK"  # ACK or RESULT
    
    def __post_init__(self):
        if self.new_client_order_id is None:
            self.new_client_order_id = f"trade_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"
    
    def to_api_params(self) -> Dict[str, Any]:
        """Convert to Binance API parameters"""
        params = {
            "symbol": self.symbol,
            "side": self.side.value,
            "type": self.type.value,
            "newClientOrderId": self.new_client_order_id,
            "newOrderRespType": self.new_order_resp_type,
        }
        
        # Add optional parameters
        if self.quantity is not None:
            params["quantity"] = str(self.quantity)
        if self.price is not None:
            params["price"] = str(self.price)
        if self.stop_price is not None:
            params["stopPrice"] = str(self.stop_price)
        if self.type in [OrderType.LIMIT, OrderType.STOP, OrderType.TAKE_PROFIT]:
            params["timeInForce"] = self.time_in_force.value
        if self.reduce_only:
            params["reduceOnly"] = "true"
        if self.position_side != PositionSide.BOTH:
            params["positionSide"] = self.position_side.value
        if self.close_position:
            params["closePosition"] = "true"
        if self.working_type != "CONTRACT_PRICE":
            params["workingType"] = self.working_type
        if self.price_protect:
            params["priceProtect"] = "TRUE"
            
        return params

@dataclass
class OrderResponse:
    """Order response structure matching Binance API"""
    order_id: str  # orderId
    client_order_id: str  # clientOrderId
    symbol: str
    side: str
    position_side: str  # positionSide
    type: str
    orig_type: str  # origType
    quantity: float  # origQty
    price: Optional[float]
    stop_price: Optional[float]  # stopPrice
    status: OrderStatus
    time_in_force: str  # timeInForce
    reduce_only: bool  # reduceOnly
    close_position: bool  # closePosition
    executed_qty: float  # executedQty
    cum_qty: float  # cumQty
    cum_quote: float  # cumQuote
    avg_price: float  # avgPrice
    update_time: int  # updateTime
    working_type: str  # workingType
    price_protect: bool  # priceProtect
    
    @classmethod
    def from_binance_response(cls, response: Dict[str, Any]) -> 'OrderResponse':
        """Create OrderResponse from Binance API response"""
        return cls(
            order_id=str(response['orderId']),
            client_order_id=response['clientOrderId'],
            symbol=response['symbol'],
            side=response['side'],
            position_side=response.get('positionSide', 'BOTH'),
            type=response['type'],
            orig_type=response.get('origType', response['type']),
            quantity=float(response.get('origQty', response.get('quantity', '0'))),
            price=float(response['price']) if response.get('price') and response['price'] != '0' else None,
            stop_price=float(response['stopPrice']) if response.get('stopPrice') and response['stopPrice'] != '0' else None,
            status=OrderStatus(response['status']),
            time_in_force=response.get('timeInForce', 'GTC'),
            reduce_only=response.get('reduceOnly', False),
            close_position=response.get('closePosition', False),
            executed_qty=float(response.get('executedQty', '0')),
            cum_qty=float(response.get('cumQty', '0')),
            cum_quote=float(response.get('cumQuote', '0')),
            avg_price=float(response.get('avgPrice', '0')),
            update_time=int(response.get('updateTime', 0)),
            working_type=response.get('workingType', 'CONTRACT_PRICE'),
            price_protect=response.get('priceProtect', False)
        )


class BinanceFuturesOrderManager:
    """Handles Binance futures order operations"""
    
    def __init__(self):
        self.config = get_trading_config()
        self._pending_orders: Dict[str, OrderRequest] = {}
        self._client = None  # Will be initialized when needed
    
    def _get_client(self) -> UMFutures:
        """Get or create Binance futures client"""
        if self._client is None:
            if UMFutures is None:
                raise ImportError("Binance SDK not available. Install with: pip install binance-futures-connector")
            
            # Use testnet if configured
            if self.config.use_testnet:
                base_url = "https://testnet.binancefuture.com"
            else:
                base_url = "https://fapi.binance.com"
            
            self._client = UMFutures(
                key=self.config.api_key,
                secret=self.config.api_secret,
                base_url=base_url
            )
            
            LOG.info(f"Initialized Binance client (testnet={self.config.use_testnet})")
            
        return self._client
    
    def _side_from_trade_side(self, trade_side: str) -> OrderSide:
        """Convert trade side (long/short) to order side (buy/sell)"""
        if trade_side.lower() == "long":
            return OrderSide.BUY
        elif trade_side.lower() == "short":
            return OrderSide.SELL
        else:
            raise ValueError(f"Invalid trade side: {trade_side}")
    
    def _format_quantity(self, symbol: str, quantity: float) -> float:
        """Format quantity according to symbol precision"""
        symbol_config = get_symbol_config(symbol)
        if symbol_config:
            # Round to the appropriate precision
            precision = symbol_config.quantity_precision
            return round(quantity, precision)
        return quantity
    
    def _format_price(self, symbol: str, price: float) -> float:
        """Format price according to symbol tick size"""
        symbol_config = get_symbol_config(symbol)
        if symbol_config:
            tick_size = symbol_config.price_tick
            return round(price / tick_size) * tick_size
        return price
    
    async def create_market_order(self, symbol: str, side: str, quantity: float) -> Optional[OrderResponse]:
        """
        Create a market order for immediate execution
        
        Args:
            symbol: Trading symbol (e.g., "BTCUSDT")
            side: Trade side ("long" or "short")
            quantity: Position size
            
        Returns:
            OrderResponse if successful, None otherwise
        """
        try:
            order_side = self._side_from_trade_side(side)
            formatted_qty = self._format_quantity(symbol, quantity)
            
            order_request = OrderRequest(
                symbol=symbol,
                side=order_side,
                type=OrderType.MARKET,
                quantity=formatted_qty,
                new_order_resp_type="RESULT"  # Get full response with fills
            )
            
            # Store as pending
            self._pending_orders[order_request.new_client_order_id] = order_request
            
            LOG.info(f"Creating market order: {side} {formatted_qty} {symbol}")
            
            # Make API call
            client = self._get_client()
            response = client.new_order(**order_request.to_api_params())
            
            order_response = OrderResponse.from_binance_response(response)
            
            # Log order creation
            log_order_created(
                symbol=symbol,
                side=side,
                order_id=order_response.order_id,
                client_order_id=order_response.client_order_id,
                quantity=formatted_qty,
                order_type="MARKET"
            )
            
            # If filled immediately, log the fill
            if order_response.status == OrderStatus.FILLED:
                log_order_filled(
                    symbol=symbol,
                    side=side,
                    order_id=order_response.order_id,
                    fill_price=order_response.avg_price,
                    quantity=order_response.executed_qty
                )
            
            # Remove from pending if completed
            if order_response.status in [OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED]:
                self._pending_orders.pop(order_request.new_client_order_id, None)
            
            return order_response
            
        except Exception as e:
            LOG.error(f"Error creating market order for {symbol}: {e}")
            log_error(symbol, "ORDER_CREATION", str(e))
            
            # Clean up pending order
            if 'order_request' in locals():
                self._pending_orders.pop(order_request.new_client_order_id, None)
            
            return None
    
    async def create_stop_loss_order(self, symbol: str, side: str, quantity: float, 
                                   stop_price: float) -> Optional[OrderResponse]:
        """
        Create a stop-loss order
        
        Args:
            symbol: Trading symbol
            side: Trade side (opposite of position - "long" position needs "short" stop)
            quantity: Position size to close
            stop_price: Stop loss trigger price
        """
        try:
            # Stop loss side is opposite to position side
            stop_side = "short" if side == "long" else "long"
            order_side = self._side_from_trade_side(stop_side)
            
            formatted_qty = self._format_quantity(symbol, quantity)
            formatted_stop = self._format_price(symbol, stop_price)
            
            order_request = OrderRequest(
                symbol=symbol,
                side=order_side,
                type=OrderType.STOP_MARKET,
                quantity=formatted_qty,
                stop_price=formatted_stop,
                reduce_only=True,  # Close position only
                working_type="CONTRACT_PRICE"
            )
            
            self._pending_orders[order_request.new_client_order_id] = order_request
            
            LOG.info(f"Creating stop loss: {stop_side} {formatted_qty} {symbol} @ {formatted_stop}")
            
            # Make API call
            client = self._get_client()
            response = client.new_order(**order_request.to_api_params())
            
            order_response = OrderResponse.from_binance_response(response)
            
            log_order_created(
                symbol=symbol,
                side=stop_side,
                order_id=order_response.order_id,
                client_order_id=order_response.client_order_id,
                quantity=formatted_qty,
                price=formatted_stop,
                order_type="STOP_MARKET"
            )
            
            return order_response
            
        except Exception as e:
            LOG.error(f"Error creating stop loss order for {symbol}: {e}")
            log_error(symbol, "STOP_ORDER_CREATION", str(e))
            return None
    
    async def cancel_order(self, symbol: str, order_id: str = None, 
                          client_order_id: str = None) -> bool:
        """
        Cancel an order by order ID or client order ID
        
        Returns:
            True if successfully cancelled, False otherwise
        """
        try:
            if not order_id and not client_order_id:
                raise ValueError("Either order_id or client_order_id must be provided")
            
            LOG.info(f"Cancelling order: {order_id or client_order_id} for {symbol}")
            
            client = self._get_client()
            params = {"symbol": symbol}
            
            if order_id:
                params["orderId"] = order_id
            else:
                params["origClientOrderId"] = client_order_id
            
            response = client.cancel_order(**params)
            
            log_order_cancelled(symbol, order_id or client_order_id, "Manual cancellation")
            
            # Clean up from pending orders
            if client_order_id and client_order_id in self._pending_orders:
                del self._pending_orders[client_order_id]
            
            return True
            
        except Exception as e:
            LOG.error(f"Error cancelling order {order_id or client_order_id}: {e}")
            log_error(symbol, "ORDER_CANCELLATION", str(e))
            return False
    
    async def cancel_all_orders(self, symbol: str) -> int:
        """
        Cancel all open orders for a symbol
        
        Returns:
            Number of orders cancelled
        """
        try:
            LOG.info(f"Cancelling all orders for {symbol}")
            
            client = self._get_client()
            response = client.cancel_open_orders(symbol=symbol)
            
            # Response contains count of cancelled orders
            cancelled_count = len(response) if isinstance(response, list) else 1
            
            # Clean up pending orders for this symbol
            pending_to_remove = [
                client_order_id for client_order_id, order_req in self._pending_orders.items()
                if order_req.symbol == symbol
            ]
            
            for client_order_id in pending_to_remove:
                log_order_cancelled(symbol, client_order_id, "Bulk cancellation")
                del self._pending_orders[client_order_id]
            
            LOG.info(f"Cancelled {cancelled_count} orders for {symbol}")
            return cancelled_count
            
        except Exception as e:
            LOG.error(f"Error cancelling all orders for {symbol}: {e}")
            log_error(symbol, "BULK_ORDER_CANCELLATION", str(e))
            return 0
    
    async def get_order_status(self, symbol: str, order_id: str = None, 
                              client_order_id: str = None) -> Optional[OrderResponse]:
        """
        Get order status by order ID or client order ID
        """
        try:
            if not order_id and not client_order_id:
                raise ValueError("Either order_id or client_order_id must be provided")
            
            client = self._get_client()
            params = {"symbol": symbol}
            
            if order_id:
                params["orderId"] = order_id
            else:
                params["origClientOrderId"] = client_order_id
            
            response = client.query_order(**params)
            
            return OrderResponse.from_binance_response(response)
            
        except Exception as e:
            LOG.error(f"Error getting order status {order_id or client_order_id}: {e}")
            log_error(symbol, "ORDER_STATUS_CHECK", str(e))
            return None
    
    async def change_leverage(self, symbol: str, leverage: int) -> bool:
        """Change leverage for a symbol"""
        try:
            client = self._get_client()
            response = client.change_leverage(symbol=symbol, leverage=leverage)
            LOG.info(f"Changed leverage for {symbol} to {leverage}x: {response}")
            return True
        except Exception as e:
            LOG.error(f"Error changing leverage for {symbol}: {e}")
            return False
    
    async def change_margin_type(self, symbol: str, margin_type: str) -> bool:
        """Change margin type for a symbol"""
        try:
            client = self._get_client()
            response = client.change_margin_type(symbol=symbol, marginType=margin_type)
            LOG.info(f"Changed margin type for {symbol} to {margin_type}: {response}")
            return True
        except Exception as e:
            LOG.error(f"Error changing margin type for {symbol}: {e}")
            return False
    
    def get_pending_orders(self) -> Dict[str, OrderRequest]:
        """Get all pending orders"""
        return self._pending_orders.copy()


# Global order manager instance
order_manager = BinanceFuturesOrderManager()fills': []
            }
            
            order_response = OrderResponse.from_binance_response(mock_response)
            
            log_order_created(
                symbol=symbol,
                side=stop_side,
                order_id=order_response.order_id,
                client_order_id=order_response.client_order_id,
                quantity=formatted_qty,
                price=formatted_stop,
                order_type="STOP_MARKET"
            )
            
            return order_response
            
        except Exception as e:
            LOG.error(f"Error creating stop loss order for {symbol}: {e}")
            log_error(symbol, "STOP_ORDER_CREATION", str(e))
            return None
    
    async def cancel_order(self, symbol: str, order_id: str = None, 
                          client_order_id: str = None) -> bool:
        """
        Cancel an order by order ID or client order ID
        
        Returns:
            True if successfully cancelled, False otherwise
        """
        try:
            if not order_id and not client_order_id:
                raise ValueError("Either order_id or client_order_id must be provided")
            
            LOG.info(f"Cancelling order: {order_id or client_order_id} for {symbol}")
            
            # TODO: Implement actual Binance API call
            # client = self._get_client()
            # if order_id:
            #     response = client.cancel_order(symbol=symbol, orderId=order_id)
            # else:
            #     response = client.cancel_order(symbol=symbol, origClientOrderId=client_order_id)
            
            # Mock success response
            log_order_cancelled(symbol, order_id or client_order_id, "Manual cancellation")
            
            # Clean up from pending orders
            if client_order_id and client_order_id in self._pending_orders:
                del self._pending_orders[client_order_id]
            
            return True
            
        except Exception as e:
            LOG.error(f"Error cancelling order {order_id or client_order_id}: {e}")
            log_error(symbol, "ORDER_CANCELLATION", str(e))
            return False
    
    async def cancel_all_orders(self, symbol: str) -> int:
        """
        Cancel all open orders for a symbol
        
        Returns:
            Number of orders cancelled
        """
        try:
            LOG.info(f"Cancelling all orders for {symbol}")
            
            # TODO: Implement actual Binance API call
            # client = self._get_client()
            # response = client.cancel_all_open_orders(symbol=symbol)
            
            # Mock cancellation of pending orders
            cancelled_count = 0
            pending_to_remove = []
            
            for client_order_id, order_req in self._pending_orders.items():
                if order_req.symbol == symbol:
                    log_order_cancelled(symbol, client_order_id, "Bulk cancellation")
                    pending_to_remove.append(client_order_id)
                    cancelled_count += 1
            
            # Clean up pending orders
            for client_order_id in pending_to_remove:
                del self._pending_orders[client_order_id]
            
            LOG.info(f"Cancelled {cancelled_count} orders for {symbol}")
            return cancelled_count
            
        except Exception as e:
            LOG.error(f"Error cancelling all orders for {symbol}: {e}")
            log_error(symbol, "BULK_ORDER_CANCELLATION", str(e))
            return 0
    
    async def get_order_status(self, symbol: str, order_id: str = None, 
                              client_order_id: str = None) -> Optional[OrderResponse]:
        """
        Get order status by order ID or client order ID
        """
        try:
            if not order_id and not client_order_id:
                raise ValueError("Either order_id or client_order_id must be provided")
            
            # TODO: Implement actual Binance API call
            # client = self._get_client()
            # if order_id:
            #     response = client.get_order(symbol=symbol, orderId=order_id)
            # else:
            #     response = client.get_order(symbol=symbol, origClientOrderId=client_order_id)
            
            # Mock response
            mock_response = {
                'orderId': order_id or '12345',
                'clientOrderId': client_order_id or 'mock_client_id',
                'symbol': symbol,
                'side': 'BUY',
                'type': 'MARKET',
                'origQty': '0.1',
                'status': 'FILLED',
                'fills': []
            }
            
            return OrderResponse.from_binance_response(mock_response)
            
        except Exception as e:
            LOG.error(f"Error getting order status {order_id or client_order_id}: {e}")
            log_error(symbol, "ORDER_STATUS_CHECK", str(e))
            return None
    
    def get_pending_orders(self) -> Dict[str, OrderRequest]:
        """Get all pending orders"""
        return self._pending_orders.copy()


# Global order manager instance
order_manager = BinanceFuturesOrderManager()


@app.task(name="cancel_pending_orders")
def cancel_pending_orders(symbol: str, reason: str = "DISARM signal"):
    """
    Celery task to cancel pending orders for a symbol
    
    Args:
        symbol: Trading symbol
        reason: Reason for cancellation
    """
    LOG.info(f"[{symbol}] Cancelling pending orders: {reason}")
    
    try:
        # Run async cancellation
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            cancelled_count = loop.run_until_complete(
                order_manager.cancel_all_orders(symbol)
            )
            LOG.info(f"[{symbol}] Cancelled {cancelled_count} pending orders: {reason}")
            return cancelled_count
        finally:
            loop.close()
            
    except Exception as e:
        LOG.error(f"[{symbol}] Error cancelling pending orders: {e}")
        from app.services.trading.logging.trade_logger import log_error
        log_error(symbol, "ORDER_CANCELLATION", str(e))
        raise


@app.task(name="set_leverage_and_margin")
def set_leverage_and_margin(symbol: str, leverage: int = None, margin_mode: str = None):
    """
    Set leverage and margin mode for a symbol
    
    Args:
        symbol: Trading symbol
        leverage: Leverage multiplier (default from config)
        margin_mode: "ISOLATED" or "CROSS" (default from config)
    """
    config = get_trading_config()
    leverage = leverage or config.leverage
    margin_mode = margin_mode or config.margin_mode.value
    
    LOG.info(f"[{symbol}] Setting leverage to {leverage}x and margin mode to {margin_mode}")
    
    try:
        # TODO: Implement actual Binance API calls
        # client = order_manager._get_client()
        
        # Set leverage
        # leverage_response = client.change_leverage(
        #     symbol=symbol,
        #     leverage=leverage
        # )
        
        # Set margin mode
        # margin_response = client.change_margin_type(
        #     symbol=symbol,
        #     marginType=margin_mode
        # )
        
        LOG.info(f"[{symbol}] Successfully set {leverage}x leverage and {margin_mode} margin")
        return {
            "symbol": symbol,
            "leverage": leverage,
            "margin_mode": margin_mode,
            "status": "success"
        }
        
    except Exception as e:
        LOG.error(f"[{symbol}] Error setting leverage/margin: {e}")
        from app.services.trading.logging.trade_logger import log_error
        log_error(symbol, "LEVERAGE_MARGIN_SETUP", str(e))
        raise


# Convenience functions for external use
async def create_market_order(symbol: str, side: str, quantity: float):
    """Create a market order"""
    return await order_manager.create_market_order(symbol, side, quantity)

async def create_stop_loss_order(symbol: str, side: str, quantity: float, stop_price: float):
    """Create a stop loss order"""
    return await order_manager.create_stop_loss_order(symbol, side, quantity, stop_price)

async def cancel_order(symbol: str, order_id: str = None, client_order_id: str = None):
    """Cancel a specific order"""
    return await order_manager.cancel_order(symbol, order_id, client_order_id)

async def get_order_status(symbol: str, order_id: str = None, client_order_id: str = None):
    """Get order status"""
    return await order_manager.get_order_status(symbol, order_id, client_order_id)


if __name__ == "__main__":
    # Test order manager
    import asyncio
    import logging
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    
    async def test_orders():
        """Test order management functions"""
        symbol = "BTCUSDT"
        
        # Test market order
        print("Testing market order creation...")
        market_order = await create_market_order(symbol, "long", 0.001)
        if market_order:
            print(f"Market order created: {market_order.order_id}")
        
        # Test stop loss order
        print("Testing stop loss order creation...")
        stop_order = await create_stop_loss_order(symbol, "long", 0.001, 44500.0)
        if stop_order:
            print(f"Stop loss order created: {stop_order.order_id}")
        
        # Test order status
        if market_order:
            print("Testing order status check...")
            status = await get_order_status(symbol, market_order.order_id)
            if status:
                print(f"Order status: {status.status.value}")
    
    # Run tests
    asyncio.run(test_orders())