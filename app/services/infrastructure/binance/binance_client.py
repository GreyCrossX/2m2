"""Async wrapper around the synchronous :class:`BinanceUSDS` adapter."""

from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal, InvalidOperation
from functools import partial
from typing import Any, Callable, Dict, Mapping, NoReturn, Tuple

from app.services.domain.exceptions import DomainBadRequest
from .request_validators import (
    validate_new_order_payload,
    validate_query_or_cancel_payload,
)
from .binance_usds import BinanceUSDS, BinanceUSDSConfig
from .utils.filters import build_symbol_filters, quantize_price, quantize_qty

logger = logging.getLogger("services.infrastructure.binance.binance_client")

_REDACT_KEYS = {"timestamp", "signature", "recvWindow"}
_BOOL_FIELDS = {"reduceOnly", "closePosition", "priceProtect"}
_UPPER_FIELDS = {"symbol", "side", "type", "timeInForce", "positionSide", "workingType"}

DEFAULT_RECV_WINDOW_MS = 5_000
DEFAULT_MAX_CLOCK_SKEW_MS = 1_000


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
        recv_window_ms: int = DEFAULT_RECV_WINDOW_MS,
        max_clock_skew_ms: int = DEFAULT_MAX_CLOCK_SKEW_MS,
        timestamp_provider: Callable[[], int] | None = None,
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
        self._recv_window_ms = min(max(1, int(recv_window_ms)), 60_000)
        self._max_clock_skew_ms = max(0, int(max_clock_skew_ms))
        self._timestamp_provider = timestamp_provider or (
            lambda: int(time.time() * 1000)
        )

        # Cached exchange info + per-symbol filters
        self._filters_lock = asyncio.Lock()
        self._filters: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._exchange_info: Dict[str, Any] | None = None
        self._exchange_info_fetched_at: float | None = None
        self._exchange_info_ttl = 300.0

    # ---------------------------
    # Low-level async call helper
    # ---------------------------
    async def _call(self, func, /, *args, **kwargs):
        call = partial(func, *args, **kwargs)
        return await asyncio.wait_for(
            asyncio.to_thread(call), timeout=self._call_timeout
        )

    # ---------------------------
    # Exchange info / filters
    # ---------------------------
    async def exchange_info(self) -> dict:
        """Fetch exchange information and cache it for later filter lookups."""
        info: dict = await self._call(self._gateway.exchange_information)
        self._exchange_info = info
        self._exchange_info_fetched_at = time.time()
        return info

    async def _get_symbol_filters(self, symbol: str) -> Dict[str, Dict[str, Any]]:
        """Return symbol filters (LOT_SIZE, PRICE_FILTER, NOTIONAL/MIN_NOTIONAL, ...), cached."""
        sym = symbol.upper()
        async with self._filters_lock:
            now = time.time()
            # Fast path: already cached and not stale
            if (
                sym in self._filters
                and self._exchange_info_fetched_at
                and now - self._exchange_info_fetched_at < self._exchange_info_ttl
            ):
                return self._filters[sym]

            # Ensure we have fresh exchange info (refresh if stale)
            info = self._exchange_info
            if (
                info is None
                or not self._exchange_info_fetched_at
                or (now - self._exchange_info_fetched_at >= self._exchange_info_ttl)
            ):
                info = await self.exchange_info()

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

            pf = symbol_filters.get("PRICE_FILTER", {})
            lot = symbol_filters.get("LOT_SIZE", {})
            # Defensive fallbacks for futures tick/step when SDK omits them
            fallback_ticks = {"BTCUSDT": "0.1", "ETHUSDT": "0.01"}
            if sym in fallback_ticks:
                pf["tickSize"] = fallback_ticks[sym]
            if not lot.get("stepSize"):
                lot["stepSize"] = "0.001"

            if not pf.get("tickSize") or not lot.get("stepSize"):
                logger.warning(
                    "Missing tickSize/stepSize for %s; falling back to precision",
                    sym,
                )

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
        return await self._call(
            self._gateway.change_leverage, symbol.upper(), int(leverage)
        )

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
        q_qty = quantize_qty(filters, quantity)  # expected to floor to stepSize
        q_price = (
            quantize_price(filters, price) if price is not None else None
        )  # floor to tickSize
        return q_qty, q_price

    # ---------------------------
    # Payload normalization
    # ---------------------------
    @staticmethod
    def _prepare_payload(params: Mapping[str, Any]) -> Dict[str, Any]:
        """
        Uppercase enum-ish fields; stringify Decimals; coerce bools to Binance-friendly strings; drop None.
        """
        if not params:
            return {}
        payload: Dict[str, Any] = {}
        for key, value in params.items():
            if value is None:
                continue
            if key in _UPPER_FIELDS and isinstance(value, str):
                payload[key] = value.upper()
            elif isinstance(value, Decimal):
                payload[key] = str(value)
            elif isinstance(value, bool) and key in _BOOL_FIELDS:
                payload[key] = "true" if value else "false"
            else:
                payload[key] = value
        return payload

    @staticmethod
    def _redact_payload(payload: Mapping[str, Any]) -> Dict[str, Any]:
        redacted: Dict[str, Any] = {}
        for key, value in payload.items():
            redacted[key] = "***" if key in _REDACT_KEYS else value
        return redacted

    def _attach_timing(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ensure recvWindow is present; if caller provided timestamp, guard against skew.
        """
        merged = dict(payload)
        merged.setdefault("recvWindow", self._recv_window_ms)
        if "timestamp" in merged:
            ts = merged.get("timestamp")
            now_ms = self._timestamp_provider()
            try:
                ts_int = int(ts) if ts is not None else now_ms
            except Exception as exc:  # noqa: BLE001
                raise DomainBadRequest(f"Invalid timestamp '{ts}'") from exc

            skew = abs(now_ms - ts_int)
            if self._max_clock_skew_ms and skew > (
                self._recv_window_ms + self._max_clock_skew_ms
            ):
                raise DomainBadRequest(
                    f"Local clock skew too large ({skew}ms) for recvWindow {self._recv_window_ms}ms"
                )
        return merged

    async def _enforce_min_notional(self, payload: Mapping[str, Any]) -> None:
        """
        Pre-flight check: for priced orders with quantity present, ensure qty*price meets minNotional.
        Skip when price is absent (e.g., MARKET/STOP_MARKET).
        """
        sym = payload.get("symbol")
        price = payload.get("price")
        qty = payload.get("quantity")
        if sym is None or price is None or qty is None:
            return
        try:
            q_price = Decimal(str(price))
            q_qty = Decimal(str(qty))
        except (InvalidOperation, TypeError, ValueError):
            return
        if q_price <= 0 or q_qty <= 0:
            return
        filters = await self._get_symbol_filters(str(sym))
        mn = filters.get("MIN_NOTIONAL") or filters.get("NOTIONAL") or {}
        mn_val = (
            mn.get("notional") or mn.get("minNotional") or mn.get("minNotionalValue")
        )
        try:
            min_notional = (
                Decimal(str(mn_val)) if mn_val not in (None, "", "0") else Decimal("0")
            )
        except Exception:
            min_notional = Decimal("0")
        notional = q_qty * q_price
        if min_notional > 0 and notional < min_notional:
            raise DomainBadRequest(
                f"Position size too small: notional {notional} < min {min_notional} for {sym}"
            )

    def _log_and_raise(
        self, action: str, payload: Mapping[str, Any], exc: Exception
    ) -> NoReturn:
        logger.warning(
            "binance_%s_failed base=%s payload=%s err=%s",
            action,
            self._gateway.base_path,
            self._redact_payload(payload),
            exc,
        )
        raise exc

    async def _retry_with_requantize(
        self, payload: Dict[str, Any], exc: Exception
    ) -> dict | None:
        """Best-effort retry when Binance rejects price/tick precision."""
        reason = str(exc).lower()
        if not any(key in reason for key in ("tick size", "price filter", "precision")):
            return None

        sym = payload.get("symbol")
        price = payload.get("price")
        qty = payload.get("quantity")
        if not sym or price is None or qty is None:
            return None

        try:
            # Force refresh so we don't reuse stale tick/step caches
            self._filters.pop(str(sym).upper(), None)
            self._exchange_info_fetched_at = None
            filters = await self._get_symbol_filters(str(sym))
            q_qty = quantize_qty(filters, Decimal(str(qty)))
            q_price = quantize_price(filters, Decimal(str(price)))
            if q_qty <= 0 or q_price is None or q_price <= 0:
                return None
            new_payload = dict(payload)
            new_payload["quantity"] = str(q_qty)
            new_payload["price"] = str(q_price)
        except Exception as retry_exc:  # noqa: BLE001 - best-effort path
            logger.debug(
                "Requantize retry skipped | sym=%s err=%s",
                sym,
                retry_exc,
                exc_info=True,
            )
            return None

        logger.warning(
            "Retrying new_order with refreshed quantization | sym=%s old_qty=%s new_qty=%s old_price=%s new_price=%s reason=%s",
            sym,
            payload.get("quantity"),
            new_payload["quantity"],
            payload.get("price"),
            new_payload["price"],
            exc,
        )
        try:
            return await self._call(self._gateway.new_order, **new_payload)
        except Exception as retry_exc:  # noqa: BLE001 - only warn, fall through
            logger.warning(
                "Retry after requantize failed | sym=%s err=%s", sym, retry_exc
            )
            return None

    # ---------------------------
    # Order endpoints
    # ---------------------------
    async def new_order(self, **params: Any) -> dict:
        """Submit a new order with payload normalization/logging."""
        # Inject recvWindow if caller did not provide
        merged = self._attach_timing(dict(params))
        validated = validate_new_order_payload(merged)
        await self._enforce_min_notional(validated)
        payload = self._prepare_payload(validated)
        logger.debug(
            "binance_new_order", extra={"payload": self._redact_payload(payload)}
        )
        try:
            return await self._call(self._gateway.new_order, **payload)
        except Exception as exc:
            retry = await self._retry_with_requantize(dict(payload), exc)
            if retry is not None:
                return retry
            self._log_and_raise("new_order", payload, exc)

    async def query_order(self, **params: Any) -> dict:
        """Query order status from Binance."""
        validated = validate_query_or_cancel_payload(self._attach_timing(dict(params)))
        payload = self._prepare_payload(validated)
        logger.debug(
            "binance_query_order", extra={"payload": self._redact_payload(payload)}
        )
        try:
            return await self._call(self._gateway.query_order, **payload)
        except Exception as exc:
            self._log_and_raise("query_order", payload, exc)

    async def cancel_order(self, **params: Any) -> dict:
        """Cancel an existing order."""
        validated = validate_query_or_cancel_payload(self._attach_timing(dict(params)))
        payload = self._prepare_payload(validated)
        logger.debug(
            "binance_cancel_order", extra={"payload": self._redact_payload(payload)}
        )
        try:
            return await self._call(self._gateway.cancel_order, **payload)
        except Exception as exc:
            self._log_and_raise("cancel_order", payload, exc)

    async def open_orders(self, symbol: str | None = None) -> list[dict]:
        params: Dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol.upper()
        validated = validate_query_or_cancel_payload(
            self._attach_timing({"symbol": params.get("symbol"), "type": "open_orders"})
        )
        result = await self._call(self._gateway.open_orders, **validated)
        if isinstance(result, list):
            return result
        return [] if result is None else [result]

    # ---------------------------
    # Test order (no execution)
    # ---------------------------
    async def test_order(self, **params: Any) -> dict:
        """
        Call Binance test order endpoint to validate payload without execution.
        Relies on the same validation/normalization as new_order.
        """
        merged = self._attach_timing(dict(params))
        validated = validate_new_order_payload(merged)
        await self._enforce_min_notional(validated)
        payload = self._prepare_payload(validated)
        logger.debug(
            "binance_test_order", extra={"payload": self._redact_payload(payload)}
        )
        # If the SDK doesn't expose test_order, fall back to new_order on gateway if available.
        test_fn = getattr(self._gateway, "test_order", None) or getattr(
            self._gateway, "new_order", None
        )
        try:
            return await self._call(test_fn, **payload)
        except Exception as exc:
            self._log_and_raise("test_order", payload, exc)

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
