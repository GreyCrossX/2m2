from __future__ import annotations

from decimal import Decimal
from typing import Dict

from ...domain.enums import Side
from .client import BinanceClient


class BinanceTrading:
    """
    Trading adapter used by OrderExecutor.
    Now auto-quantizes qty & price before placing the order.
    """

    def __init__(self, client: BinanceClient):
        self._client = client

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        await self._client.change_leverage(symbol, int(leverage))

    async def create_limit_order(
        self,
        symbol: str,
        side: Side,
        quantity: Decimal,
        price: Decimal,
        reduce_only: bool = False,
    ) -> Dict:
        # Quantize to exchange constraints
        q_qty, q_price = await self._client.quantize(symbol, quantity, price)

        side_str = "BUY" if side == Side.LONG else "SELL"
        # Some SDKs accept bool, others strings — try bool first
        return await self._client.new_order(
            symbol=symbol,
            side=side_str,
            order_type="LIMIT",
            quantity=float(q_qty),
            price=float(q_price),
            timeInForce="GTC",
            reduceOnly=bool(reduce_only),
        )

    async def create_stop_market_order(
        self,
        symbol: str,
        side: Side,
        quantity: Decimal,
        stop_price: Decimal,
    ) -> Dict:
        # Quantize qty; stopPrice uses tick filter too, but many exchanges accept more decimals — still safe to snap
        q_qty, q_stop = await self._client.quantize(symbol, quantity, stop_price)
        # Protective side is opposite
        side_str = "SELL" if side == Side.LONG else "BUY"
        return await self._client.new_order(
            symbol=symbol,
            side=side_str,
            order_type="STOP_MARKET",
            quantity=float(q_qty),
            stopPrice=float(q_stop),
        )

    async def cancel_order(self, symbol: str, order_id: int) -> None:
        await self._client.cancel_order(symbol=symbol, orderId=int(order_id))

    async def get_order_status(self, symbol: str, order_id: int) -> str:
        q = await self._client.query_order(symbol=symbol, orderId=int(order_id))
        return str(q.get("status", ""))
