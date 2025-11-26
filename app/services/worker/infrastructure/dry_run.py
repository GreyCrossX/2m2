"""Dry-run trading adapter used when live order placement is disabled."""

from __future__ import annotations

import logging
from decimal import Decimal
from itertools import count
from typing import TYPE_CHECKING, Any, Dict

if TYPE_CHECKING:  # pragma: no cover - import only for static analysis
    from app.services.worker.domain.enums import OrderSide
else:
    OrderSide = Any  # type: ignore


log = logging.getLogger("worker.dry_run.trading")


class DryRunTradingAdapter:
    """No-op trading adapter that logs payloads instead of calling Binance."""

    _DEFAULT_FILTERS: Dict[str, Dict[str, Any]] = {
        "LOT_SIZE": {"stepSize": "0.001", "minQty": "0", "maxQty": "0"},
        "PRICE_FILTER": {"tickSize": None, "minPrice": None, "maxPrice": None},
        "NOTIONAL": {"notional": "20"},
    }

    def __init__(self) -> None:
        self._id_counter = count(1)
        self._orders: Dict[int, Dict[str, Any]] = {}

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        log.info(
            "[dry-run] set_leverage noop | symbol=%s leverage=%s", symbol, leverage
        )

    async def quantize_limit_order(
        self, symbol: str, quantity: Decimal, price: Decimal
    ) -> tuple[Decimal, Decimal | None]:
        log.info(
            "[dry-run] quantize_limit_order | symbol=%s quantity=%s price=%s",
            symbol,
            str(quantity),
            str(price),
        )
        return quantity, price

    async def create_limit_order(
        self,
        *,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        price: Decimal,
        reduce_only: bool = False,
        time_in_force: str = "GTC",
        new_client_order_id: str | None = None,
    ) -> Dict[str, Any]:
        payload = {
            "symbol": symbol,
            "side": getattr(side, "value", str(side)),
            "quantity": str(quantity),
            "price": str(price),
            "reduceOnly": reduce_only,
            "timeInForce": time_in_force,
            "newClientOrderId": new_client_order_id,
        }
        order_id = int(next(self._id_counter))
        log.info(
            "[dry-run] create_limit_order | order_id=%s payload=%s", order_id, payload
        )
        record = {
            "orderId": order_id,
            "dryRun": True,
            "payload": payload,
            "status": "NEW",
            "executedQty": "0",
            "price": payload.get("price"),
        }
        self._orders[order_id] = record
        return record

    async def create_take_profit_limit(
        self,
        *,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        price: Decimal,
        stop_price: Decimal,
        reduce_only: bool = True,
        time_in_force: str = "GTC",
        new_client_order_id: str | None = None,
    ) -> Dict[str, Any]:
        payload = {
            "symbol": symbol,
            "side": getattr(side, "value", str(side)),
            "quantity": str(quantity),
            "price": str(price),
            "stopPrice": str(stop_price),
            "reduceOnly": reduce_only,
            "timeInForce": time_in_force,
            "newClientOrderId": new_client_order_id,
        }
        order_id = int(next(self._id_counter))
        log.info(
            "[dry-run] create_take_profit_limit | order_id=%s payload=%s",
            order_id,
            payload,
        )
        record = {
            "orderId": order_id,
            "dryRun": True,
            "payload": payload,
            "status": "NEW",
            "executedQty": "0",
            "price": payload.get("price"),
            "stopPrice": payload.get("stopPrice"),
        }
        self._orders[order_id] = record
        return record

    async def get_symbol_filters(self, symbol: str) -> Dict[str, Dict[str, Any]]:
        log.info("[dry-run] get_symbol_filters | symbol=%s", symbol)
        return self._DEFAULT_FILTERS

    async def cancel_order(self, *, symbol: str, order_id: int) -> None:
        log.info("[dry-run] cancel_order | symbol=%s order_id=%s", symbol, order_id)
        order = self._orders.get(int(order_id))
        if order:
            order["status"] = "CANCELED"

    async def get_order(self, symbol: str, order_id: int) -> Dict[str, Any]:
        return self._orders.get(
            int(order_id), {"orderId": order_id, "status": "NEW", "executedQty": "0"}
        )

    async def list_open_orders(self, symbol: str | None = None) -> list[dict]:
        return [
            o
            for o in self._orders.values()
            if o.get("status") not in {"CANCELED", "FILLED"}
        ]
