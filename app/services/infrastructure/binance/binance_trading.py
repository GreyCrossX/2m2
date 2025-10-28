"""High-level trading helpers built on top of :class:`BinanceClient`."""
from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, Iterable

from app.services.domain.exceptions import DomainBadRequest, DomainExchangeError
from app.services.worker.domain.enums import OrderSide  # <<< updated

from .binance_client import BinanceClient

_VALID_SIDES = {"BUY", "SELL"}
_VALID_ORDER_TYPES = {
    "LIMIT",
    "MARKET",
    "STOP_MARKET",
    "TAKE_PROFIT_MARKET",
}
_VALID_TIME_IN_FORCE = {"GTC", "IOC", "FOK", "GTX"}
_VALID_POSITION_SIDES = {"BOTH", "LONG", "SHORT"}


def _normalize_side(side: OrderSide | str) -> str:
    """Map domain side to exchange side string."""
    if isinstance(side, OrderSide):
        return "BUY" if side == OrderSide.LONG else "SELL"
    normalized = str(side).upper()
    if normalized not in _VALID_SIDES:
        raise DomainBadRequest(f"Invalid side '{side}'")
    return normalized


def _validate_choice(value: str, valid: Iterable[str], field: str) -> str:
    normalized = str(value).upper()
    if normalized not in {v.upper() for v in valid}:
        raise DomainBadRequest(f"Invalid {field} '{value}'")
    return normalized


class BinanceTrading:
    """Trading facade that performs validation and quantization before submitting orders."""

    def __init__(self, client: BinanceClient) -> None:
        self._client = client

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        """Set leverage for a symbol."""
        await self._client.change_leverage(symbol, int(leverage))

    async def quantize_limit_order(
        self,
        symbol: str,
        quantity: Decimal,
        price: Decimal,
    ) -> tuple[Decimal, Decimal | None]:
        """Return quantized quantity and price for diagnostics/tests."""
        return await self._client.quantize_order(symbol, quantity, price)

    async def create_limit_order(
        self,
        symbol: str,
        side: OrderSide | str,
        quantity: Decimal,
        price: Decimal,
        *,
        reduce_only: bool = False,
        time_in_force: str = "GTC",
        new_client_order_id: str | None = None,
    ) -> Dict[str, Any]:
        """Create a validated LIMIT order."""

        side_str = _normalize_side(side)
        tif = _validate_choice(time_in_force, _VALID_TIME_IN_FORCE, "time_in_force")

        filters = await self._client.get_symbol_filters(symbol)
        q_qty, q_price = await self.quantize_limit_order(symbol, quantity, price)
        if q_qty <= 0 or q_price is None or q_price <= 0:
            raise DomainBadRequest("Quantized quantity/price invalid for limit order")

        min_notional_filter = filters.get("MIN_NOTIONAL") or filters.get("NOTIONAL") or {}
        min_notional_raw = (
            min_notional_filter.get("notional")
            or min_notional_filter.get("minNotional")
            or "0"
        )
        # Defensive: ensure safe Decimal construction
        min_notional = Decimal(str(min_notional_raw)) if min_notional_raw not in (None, "") else Decimal("0")
        notional = q_qty * q_price
        if min_notional > 0 and notional < min_notional:
            raise DomainBadRequest(
                f"Notional {notional} below minimum {min_notional} for {symbol.upper()}"
            )

        payload: Dict[str, Any] = {
            "symbol": symbol.upper(),
            "side": side_str,
            "type": "LIMIT",
            "quantity": q_qty,
            "price": q_price,
            "timeInForce": tif,
            "reduceOnly": bool(reduce_only),
        }
        if new_client_order_id:
            payload["newClientOrderId"] = new_client_order_id

        try:
            return await self._client.new_order(**payload)
        except DomainExchangeError as exc:
            message = (
                f"{exc} | symbol={symbol} qty={q_qty} price={q_price} "
                f"reduce_only={reduce_only} tif={tif}"
            )
            raise type(exc)(message) from exc

    async def create_stop_market_order(
        self,
        symbol: str,
        side: OrderSide | str,
        quantity: Decimal,
        stop_price: Decimal,
        *,
        working_type: str | None = None,
        position_side: str = "BOTH",
        order_type: str = "STOP_MARKET",
    ) -> Dict[str, Any]:
        """Create a validated STOP_MARKET or TAKE_PROFIT_MARKET order."""

        normalized_type = _validate_choice(order_type, _VALID_ORDER_TYPES, "type")
        if normalized_type not in {"STOP_MARKET", "TAKE_PROFIT_MARKET"}:
            raise DomainBadRequest("Stop order must be STOP_MARKET or TAKE_PROFIT_MARKET")

        side_str = _normalize_side(side)
        position = _validate_choice(position_side, _VALID_POSITION_SIDES, "position_side")

        q_qty, q_stop = await self._client.quantize_order(symbol, quantity, stop_price)
        if q_qty <= 0 or q_stop is None or q_stop <= 0:
            raise DomainBadRequest("Quantized quantity/stop invalid for stop-market order")

        payload: Dict[str, Any] = {
            "symbol": symbol.upper(),
            "side": side_str,
            "type": normalized_type,
            "quantity": q_qty,
            "stopPrice": q_stop,
            "positionSide": position,
        }
        if working_type:
            payload["workingType"] = working_type

        return await self._client.new_order(**payload)

    async def cancel_order(self, symbol: str, order_id: int) -> Dict[str, Any]:
        """Cancel an order by id."""
        return await self._client.cancel_order(symbol=symbol.upper(), orderId=int(order_id))

    async def get_order_status(self, symbol: str, order_id: int) -> str:
        """Return status string for the specified order."""
        result = await self._client.query_order(symbol=symbol.upper(), orderId=int(order_id))
        return str(result.get("status", ""))

    async def close_position_market(
        self,
        symbol: str,
        side: OrderSide | str,
        quantity: Decimal,
        *,
        position_side: str = "BOTH",
    ) -> Dict[str, Any]:
        """Close an open position using a reduce-only market order."""
        side_str = _normalize_side(side)
        position = _validate_choice(position_side, _VALID_POSITION_SIDES, "position_side")
        return await self._client.close_position_market(
            symbol,
            side_str,
            quantity,
            position_side=position,
        )
