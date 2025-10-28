from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any, Callable, Dict, Tuple

from app.core.exchange.filters import (
    build_symbol_filters,
    quantize_price,
    quantize_qty,
)
from app.infrastructure.exchange.binance_usds import BinanceUSDS, BinanceUSDSConfig
from app.services.worker.domain.exceptions import DomainBadRequest

class BinanceClient:
    """Async wrapper around :class:`BinanceUSDS` with symbol filter caching."""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        *,
        testnet: bool = False,
        timeout_ms: int = 5000,
        gateway: BinanceUSDS | None = None,
    ) -> None:
        if not api_key or not api_secret:
            raise ValueError("API key and secret are required")

        self._config = BinanceUSDSConfig(
            api_key=api_key,
            api_secret=api_secret,
            testnet=testnet,
            timeout_ms=timeout_ms,
        )
        self._gateway = gateway or BinanceUSDS(self._config)

        self._filters_lock = asyncio.Lock()
        self._filters: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._exchange_info: Dict[str, Any] | None = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    async def _call(self, func: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Any:
        return await asyncio.to_thread(func, *args, **kwargs)

    async def exchange_info(self) -> dict:
        info: dict = await self._call(self._gateway.exchange_info)
        self._exchange_info = info
        return info

    async def _get_symbol_filters(self, symbol: str) -> Dict[str, Dict[str, Any]]:
        sym = symbol.upper()
        async with self._filters_lock:
            if sym in self._filters:
                return self._filters[sym]

            if self._exchange_info is None:
                info = await self.exchange_info()
            else:
                info = self._exchange_info

            filters_map = build_symbol_filters(info)
            self._filters.update(filters_map)
            symbol_filters = self._filters.get(sym)
            if symbol_filters is None:
                # refresh once more in case exchange info changed
                info = await self.exchange_info()
                self._filters = build_symbol_filters(info)
                symbol_filters = self._filters.get(sym)
            if symbol_filters is None:
                raise DomainBadRequest(f"Symbol {sym} not present in exchange info")
            return symbol_filters

    async def get_symbol_filters(self, symbol: str) -> Dict[str, Dict[str, Any]]:
        """Expose filters for diagnostics/testing."""
        return await self._get_symbol_filters(symbol)

    # ------------------------------------------------------------------
    # Account + positions
    # ------------------------------------------------------------------
    async def account(self) -> dict:
        return await self._call(self._gateway.account_information)

    async def balance(self) -> list[dict]:
        return await self._call(self._gateway.account_balance)

    async def position_information(self, symbol: str | None = None) -> list[dict]:
        return await self._call(self._gateway.position_information, symbol)

    async def get_position_mode(self) -> dict:
        return await self._call(self._gateway.get_position_mode)

    async def set_position_mode(self, dual_side: bool) -> dict:
        return await self._call(self._gateway.set_position_mode, dual_side)

    # ------------------------------------------------------------------
    # Orders & leverage
    # ------------------------------------------------------------------
    async def change_leverage(self, symbol: str, leverage: int) -> dict:
        return await self._call(self._gateway.change_leverage, symbol, leverage)

    async def quantize_order(
        self,
        symbol: str,
        quantity: Decimal,
        price: Decimal | None,
    ) -> Tuple[Decimal, Decimal | None]:
        filters = await self._get_symbol_filters(symbol)
        qty = quantize_qty(filters, quantity)
        px = quantize_price(filters, price) if price is not None else None
        return qty, px

    async def new_order(self, **params: Any) -> dict:
        payload = self._prepare_payload(params)
        return await self._call(self._gateway.new_order, **payload)

    async def query_order(self, **params: Any) -> dict:
        payload = self._prepare_payload(params)
        return await self._call(self._gateway.query_order, **payload)

    async def cancel_order(self, **params: Any) -> dict:
        payload = self._prepare_payload(params)
        return await self._call(self._gateway.cancel_order, **payload)

    async def close_position_market(self, symbol: str, side: str, quantity: Decimal) -> dict:
        qty, _ = await self.quantize_order(symbol, quantity, None)
        if qty <= 0:
            raise DomainBadRequest("Quantity rounded to zero when closing position")
        params = {
            "symbol": symbol.upper(),
            "side": side,
            "type": "MARKET",
            "quantity": str(qty),
            "reduceOnly": True,
        }
        return await self.new_order(**params)

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------
    @staticmethod
    def _prepare_payload(params: Dict[str, Any]) -> Dict[str, Any]:
        if not params:
            return {}
        payload: Dict[str, Any] = {}
        for key, value in params.items():
            if key == "symbol" and isinstance(value, str):
                payload[key] = value.upper()
            elif isinstance(value, Decimal):
                payload[key] = str(value)
            else:
                payload[key] = value
        return payload

    @property
    def base_path(self) -> str:
        return self._gateway.base_path

    @property
    def timeout_ms(self) -> int:
        return self._gateway.timeout_ms
