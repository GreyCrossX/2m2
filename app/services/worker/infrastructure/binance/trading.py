from __future__ import annotations

from decimal import Decimal
from typing import Dict, Tuple

from ...domain.enums import Side
from ...domain.exceptions import DomainBadRequest, DomainExchangeError
from .client import BinanceClient


class BinanceTrading:
    """Trading adapter used by the application order manager."""

    def __init__(self, client: BinanceClient):
        self._client = client

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        await self._client.change_leverage(symbol, int(leverage))

    async def quantize_limit_order(
        self,
        symbol: str,
        quantity: Decimal,
        price: Decimal,
    ) -> Tuple[Decimal, Decimal | None]:
        return await self._client.quantize_order(symbol, quantity, price)

    async def create_limit_order(
        self,
        symbol: str,
        side: Side,
        quantity: Decimal,
        price: Decimal,
        reduce_only: bool = False,
        time_in_force: str = "GTC",
        new_client_order_id: str | None = None,
    ) -> Dict:
        filters = await self._client.get_symbol_filters(symbol)
        q_qty, q_price = await self.quantize_limit_order(symbol, quantity, price)
        if q_qty <= 0 or q_price is None or q_price <= 0:
            raise DomainBadRequest("Quantized quantity/price invalid for limit order")

        min_notional_filter = filters.get("MIN_NOTIONAL") or {}
        min_notional_raw = (
            min_notional_filter.get("notional")
            or min_notional_filter.get("minNotional")
            or "0"
        )
        min_notional = Decimal(str(min_notional_raw)) if min_notional_raw is not None else Decimal("0")
        if min_notional > 0 and (q_qty * q_price) < min_notional:
            raise DomainBadRequest(
                f"Notional {q_qty * q_price} below minimum {min_notional}"
            )

        side_str = "BUY" if side == Side.LONG else "SELL"
        payload = {
            "symbol": symbol.upper(),
            "side": side_str,
            "type": "LIMIT",
            "quantity": q_qty,
            "price": q_price,
            "timeInForce": time_in_force,
            "reduceOnly": bool(reduce_only),
        }
        if new_client_order_id:
            payload["newClientOrderId"] = new_client_order_id

        try:
            return await self._client.new_order(**payload)
        except DomainExchangeError as exc:
            message = (
                f"{exc} | symbol={symbol} qty={q_qty} price={q_price} "
                f"filters={filters}"
            )
            raise type(exc)(message) from exc

    async def create_stop_market_order(
        self,
        symbol: str,
        side: Side,
        quantity: Decimal,
        stop_price: Decimal,
    ) -> Dict:
        q_qty, q_stop = await self._client.quantize_order(symbol, quantity, stop_price)
        if q_qty <= 0 or q_stop is None or q_stop <= 0:
            raise DomainBadRequest("Quantized quantity/stop invalid for stop-market order")
        side_str = "SELL" if side == Side.LONG else "BUY"
        payload = {
            "symbol": symbol.upper(),
            "side": side_str,
            "type": "STOP_MARKET",
            "quantity": q_qty,
            "stopPrice": q_stop,
        }
        return await self._client.new_order(**payload)

    async def cancel_order(self, symbol: str, order_id: int) -> None:
        await self._client.cancel_order(symbol=symbol.upper(), orderId=int(order_id))

    async def get_order_status(self, symbol: str, order_id: int) -> str:
        q = await self._client.query_order(symbol=symbol.upper(), orderId=int(order_id))
        return str(q.get("status", ""))
