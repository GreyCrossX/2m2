"""Async wrapper around the synchronous :class:`BinanceUSDS` adapter."""
from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from functools import partial
from typing import Any, Dict, Mapping, Tuple

from app.services.domain.exceptions import DomainBadRequest
from .binance_usds import BinanceUSDS, BinanceUSDSConfig
from .utils.filters import build_symbol_filters, quantize_price, quantize_qty

logger = logging.getLogger("services.infrastructure.binance.binance_client")

_REDACT_KEYS = {"timestamp", "signature", "recvWindow"}


class BinanceClient:
    """Async wrapper that provides convenience helpers around :class:`BinanceUSDS`."""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        *,
        testnet: bool = False,
        timeout_ms: int = 5000,
        gateway: BinanceUSDS | None = None,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        call_timeout: float = 10.0,
    ) -> None:
        if not api_key or not api_secret:
            raise ValueError("API key and secret are required")

        self._config = BinanceUSDSConfig(
            api_key=api_key,
            api_secret=api_secret,
            testnet=testnet,
            timeout_ms=timeout_ms,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
        )
        self._gateway = gateway or BinanceUSDS(self._config)
        self._call_timeout = float(call_timeout)

        # Cached exchange info + per-symbol filters
        self._filters_lock = asyncio.Lock()
        self._filters: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._exchange_info: Dict[str, Any] | None = None

    # ---------------------------
    # Low-level async call helper
    # ---------------------------
    async def _call(self, func, /, *args, **kwargs):
        call = partial(func, *args, **kwargs)
        return await asyncio.wait_for(asyncio.to_thread(call), timeout=self._call_timeout)

    # ---------------------------
    # Exchange info / filters
    # ---------------------------
    async def exchange_info(self) -> dict:
        """Fetch exchange information and cache it for later filter lookups."""
        info: dict = await self._call(self._gateway.exchange_information)
        self._exchange_info = info
        return info

    async def _get_symbol_filters(self, symbol: str) -> Dict[str, Dict[str, Any]]:
        """Return symbol filters (LOT_SIZE, PRICE_FILTER, NOTIONAL/MIN_NOTIONAL, ...), cached."""
        sym = symbol.upper()
        async with self._filters_lock:
            # Fast path: already cached
            if sym in self._filters:
                return self._filters[sym]

            # Ensure we have fresh exchange info
            info = self._exchange_info or await self.exchange_info()

            # Build (or rebuild) the full map and cache it
            filters_map = build_symbol_filters(info)
            self._filters.update(filters_map)

            symbol_filters = self._filters.get(sym)
            if symbol_filters is None:
                # One more refresh attempt in case cache was stale
                info = await self.exchange_info()
                filters_map = build_symbol_filters(info)
                self._filters = filters_map
                symbol_filters = self._filters.get(sym)

            if symbol_filters is None:
                raise DomainBadRequest(f"Symbol {sym} not present in exchange info")

            return symbol_filters

    async def get_symbol_filters(self, symbol: str) -> Dict[str, Dict[str, Any]]:
        """Expose filters for diagnostics/testing."""
        return await self._get_symbol_filters(symbol)

    # ---------------------------
    # Account / positions
    # ---------------------------
    async def account(self) -> dict:
        """Return account information."""
        return await self._call(self._gateway.account_information)

    async def balance(self) -> list[dict]:
        """Return account balances."""
        return await self._call(self._gateway.account_balance)

    async def position_information(self, symbol: str | None = None) -> list[dict]:
        """Return current position information."""
        return await self._call(self._gateway.position_information, symbol)

    async def get_position_mode(self) -> dict:
        """Return the configured position mode."""
        return await self._call(self._gateway.get_position_mode)

    async def set_position_mode(self, dual_side: bool) -> dict:
        """Set hedge mode (dual-side) for the account."""
        return await self._call(self._gateway.set_position_mode, dual_side)

    async def change_leverage(self, symbol: str, leverage: int) -> dict:
        """Update leverage for a symbol."""
        return await self._call(self._gateway.change_leverage, symbol.upper(), int(leverage))

    # ---------------------------
    # Quantization helpers
    # ---------------------------
    async def quantize_order(
        self,
        symbol: str,
        quantity: Decimal,
        price: Decimal | None,
    ) -> Tuple[Decimal, Decimal | None]:
        """
        Quantize quantity/price according to symbol filters.
        MUST floor to Binance LOT_SIZE / PRICE_FILTER (ROUND_DOWN) to avoid precision errors.
        """
        filters = await self._get_symbol_filters(symbol)
        q_qty = quantize_qty(filters, quantity)               # expected to floor to stepSize
        q_price = quantize_price(filters, price) if price is not None else None  # floor to tickSize
        return q_qty, q_price

    # ---------------------------
    # Payload normalization
    # ---------------------------
    @staticmethod
    def _prepare_payload(params: Mapping[str, Any]) -> Dict[str, Any]:
        """Uppercase symbol; stringify Decimals; leave other types as-is."""
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

    @staticmethod
    def _redact_payload(payload: Mapping[str, Any]) -> Dict[str, Any]:
        redacted: Dict[str, Any] = {}
        for key, value in payload.items():
            redacted[key] = "***" if key in _REDACT_KEYS else value
        return redacted

    # ---------------------------
    # Order endpoints
    # ---------------------------
    async def new_order(self, **params: Any) -> dict:
        """Submit a new order with payload normalization/logging."""
        payload = self._prepare_payload(params)
        logger.debug("binance_new_order", extra={"payload": self._redact_payload(payload)})
        return await self._call(self._gateway.new_order, **payload)

    async def query_order(self, **params: Any) -> dict:
        """Query order status from Binance."""
        payload = self._prepare_payload(params)
        logger.debug("binance_query_order", extra={"payload": self._redact_payload(payload)})
        return await self._call(self._gateway.query_order, **payload)

    async def cancel_order(self, **params: Any) -> dict:
        """Cancel an existing order."""
        payload = self._prepare_payload(params)
        logger.debug("binance_cancel_order", extra={"payload": self._redact_payload(payload)})
        return await self._call(self._gateway.cancel_order, **payload)

    async def open_orders(self, symbol: str | None = None) -> list[dict]:
        params: Dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol.upper()
        result = await self._call(self._gateway.open_orders, **params)
        if isinstance(result, list):
            return result
        return [] if result is None else [result]

    async def close_position_market(
        self,
        symbol: str,
        side: str,
        quantity: Decimal,
        *,
        position_side: str = "BOTH",
    ) -> dict:
        """Issue a reduce-only market order to close a position."""
        q_qty, _ = await self.quantize_order(symbol, quantity, None)
        if q_qty <= 0:
            raise DomainBadRequest("Quantity rounded to zero when closing position")
        payload = {
            "symbol": symbol.upper(),
            "side": side.upper(),
            "type": "MARKET",
            "quantity": str(q_qty),
            "reduceOnly": True,
            "positionSide": position_side.upper(),
        }
        return await self.new_order(**payload)

    # ---------------------------
    # Misc
    # ---------------------------
    @property
    def base_path(self) -> str:
        """Return Binance REST base path."""
        return self._gateway.base_path

    @property
    def timeout_ms(self) -> int:
        """Return configured HTTP timeout in milliseconds."""
        return self._gateway.timeout_ms
