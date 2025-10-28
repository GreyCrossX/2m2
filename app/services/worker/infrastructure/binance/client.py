from __future__ import annotations

import asyncio
import time
import hmac
import hashlib
import logging
from urllib.parse import urlencode
from decimal import Decimal, ROUND_DOWN
from typing import Dict, List, Optional, Tuple, Any, Callable

import requests

from binance_sdk_derivatives_trading_usds_futures.derivatives_trading_usds_futures import (
    DerivativesTradingUsdsFutures,
    ConfigurationRestAPI,
    DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL,
)

# Some SDK versions export a TESTNET constant; provide a safe fallback if missing.
try:
    from binance_sdk_derivatives_trading_usds_futures.derivatives_trading_usds_futures import (
        DERIVATIVES_TRADING_USDS_FUTURES_REST_API_TESTNET_URL,
    )
except Exception:  # pragma: no cover
    DERIVATIVES_TRADING_USDS_FUTURES_REST_API_TESTNET_URL = "https://testnet.binancefuture.com"

log = logging.getLogger("services.worker.infrastructure.binance.client")


def _unwrap(resp: Any) -> Any:
    """
    Normalize SDK responses into plain dict/list.
    Handles wrapper objects with .data() or .data property.
    """
    try:
        return resp.data()  # method
    except Exception:
        pass
    try:
        d = getattr(resp, "data")  # property or bound method
        return d() if callable(d) else d
    except Exception:
        pass
    return resp  # already plain


def _summarize_payload(obj: Any) -> str:
    try:
        if isinstance(obj, dict):
            return f"dict(keys={list(obj.keys())[:8]})"
        if isinstance(obj, list):
            return f"list(len={len(obj)})"
        return type(obj).__name__
    except Exception:
        return "<unrepr>"


def S(symbol: Optional[str]) -> Optional[str]:
    """Normalize symbol to upper-case (None-safe)."""
    if symbol is None:
        return None
    return str(symbol).upper()


# --- account() snake->camel aliasing for top-level totals ---
_CAMEL_MAP = {
    "total_initial_margin": "totalInitialMargin",
    "total_maint_margin": "totalMaintMargin",
    "total_wallet_balance": "totalWalletBalance",
    "total_unrealized_profit": "totalUnrealizedProfit",
    "total_margin_balance": "totalMarginBalance",
    "total_position_initial_margin": "totalPositionInitialMargin",
    "total_open_order_initial_margin": "totalOpenOrderInitialMargin",
    "total_cross_wallet_balance": "totalCrossWalletBalance",
}


def _add_camel_aliases(d: dict) -> dict:
    """
    Given an account_information_v3 dict (snake_case),
    return a shallow-copied dict with camelCase aliases for known keys.
    Safe no-op if keys are already camelCase.
    """
    if not isinstance(d, dict):
        return d
    out = dict(d)
    for snake, camel in _CAMEL_MAP.items():
        if snake in d and camel not in d:
            out[camel] = d[snake]
    return out


class BinanceClient:
    """
    Async facade over Binance USDⓈ-M Futures modular SDK.
    - Uses SDK where possible (v3 first), then gracefully falls back to REST.
    - All SDK calls are wrapped with `_with_timeout` so they cannot wedge.
    - SDK timeout is configured in **milliseconds**; constructor accepts seconds.
    """

    def __init__(self, api_key: str, api_secret: str, testnet: bool = False, timeout: int = 30):
        """
        :param timeout: human-friendly seconds (e.g., 30). Internally converted to ms for the SDK.
        """
        if not api_key or not api_secret:
            raise ValueError("API key and secret are required")

        base_path = (
            DERIVATIVES_TRADING_USDS_FUTURES_REST_API_TESTNET_URL
            if testnet
            else DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL
        )

        # Keep both units around for clarity
        self._timeout_s = int(timeout)
        self._timeout_ms = int(timeout * 1000)

        # SDK configuration (expects timeout in milliseconds)
        self._conf = ConfigurationRestAPI(
            api_key=api_key,
            api_secret=api_secret,
            base_path=base_path,
            timeout=self._timeout_ms,
        )
        self._rest = DerivativesTradingUsdsFutures(config_rest_api=self._conf)

        # For REST fallbacks
        self._api_key = api_key
        self._api_secret = api_secret
        self._base_url = base_path.rstrip("/")
        self._testnet = testnet

        # Exchange filter cache for quantization
        self._symbol_filters: Dict[str, Dict[str, Decimal]] = {}
        self._filters_lock = asyncio.Lock()

        # Detect capabilities up-front
        rest_api = self._rest.rest_api
        self._has_account_v3 = hasattr(rest_api, "account_information_v3")
        self._has_account_v2 = hasattr(rest_api, "account_information_v2")
        self._has_bal_v3 = hasattr(rest_api, "futures_account_balance_v3")
        self._has_bal_v2 = hasattr(rest_api, "futures_account_balance_v2")
        # Position information (correct names in SDK)
        self._has_posinfo_v3 = hasattr(rest_api, "position_information_v3")
        self._has_posinfo_v2 = hasattr(rest_api, "position_information_v2")

        log.info(
            "BinanceClient init | base=%s testnet=%s timeout_ms=%s | caps: acc_v3=%s acc_v2=%s "
            "bal_v3=%s bal_v2=%s posinfo_v3=%s posinfo_v2=%s",
            self._base_url,
            self._testnet,
            self._timeout_ms,
            self._has_account_v3,
            self._has_account_v2,
            self._has_bal_v3,
            self._has_bal_v2,
            self._has_posinfo_v3,
            self._has_posinfo_v2,
        )

    # ---------- Utilities ----------

    async def _with_timeout(self, func: Callable, *args, **kwargs):
        """
        Bound a blocking SDK call:
        - offload to a thread
        - add asyncio.wait_for hard cap (timeout_s + 5s buffer)
        Also logs elapsed time to help spot slow calls.
        """
        start = time.perf_counter()
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(func, *args, **kwargs),
                timeout=self._timeout_s + 5,
            )
        finally:
            elapsed = (time.perf_counter() - start) * 1000
            log.info("_with_timeout(%s): %.1f ms", getattr(func, "__name__", str(func)), elapsed)

    def _sign(self, params: dict) -> str:
        qs = urlencode(params, doseq=True)
        return hmac.new(self._api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()

    def _auth_headers(self) -> dict:
        return {"X-MBX-APIKEY": self._api_key}

    async def _rest_get(
        self,
        path: str,
        params: dict | None = None,
        *,
        signed: bool = False,
        timeout: Optional[int] = None,
    ) -> Any:
        url = f"{self._base_url}{path}"
        p = dict(params or {})
        if signed:
            p.setdefault("recvWindow", 5000)
            p["timestamp"] = int(time.time() * 1000)
            p["signature"] = self._sign(p)

        use_timeout = timeout if timeout is not None else max(self._timeout_s, 10)

        def _do():
            return requests.get(
                url,
                params=p,
                headers=self._auth_headers() if signed else None,
                timeout=use_timeout,
            )

        safe = {k: v for k, v in p.items() if k != "signature"}
        log.info("REST GET %s | signed=%s params=%s timeout_s=%s", path, signed, safe, use_timeout)

        r = await asyncio.to_thread(_do)
        try:
            body = r.json()
        except Exception:
            body = {"raw": r.text}

        log.info("REST GET %s -> status=%s | %s", path, r.status_code, _summarize_payload(body))
        if r.status_code != 200 and isinstance(body, dict):
            body.setdefault("status", r.status_code)
        return body

    async def _server_time_ms(self) -> Optional[int]:
        try:
            resp = await asyncio.to_thread(
                lambda: requests.get(f"{self._base_url}/fapi/v1/time", timeout=max(self._timeout_s // 2, 5))
            )
            return int(resp.json()["serverTime"])
        except Exception:
            log.exception("server time fetch failed")
            return None

    # ---------- Account / Balance / Positions ----------

    async def account(self) -> Dict:
        """
        Prefer SDK v3, then v2. If both fail/absent, fallback to REST /fapi/v2/account (signed).
        Adds camelCase aliases for snake_case v3 totals.
        """
        log.info("account(): try SDK v3->v2, then REST fallback")

        if self._has_account_v3:
            try:
                log.info("account(): attempting account_information_v3")
                resp = await self._with_timeout(self._rest.rest_api.account_information_v3)
                data = _unwrap(resp)
                if isinstance(data, dict):
                    data = _add_camel_aliases(data)
                log.info("account(): v3 OK | %s", _summarize_payload(data))
                return data
            except Exception:
                log.exception("account(): SDK v3 failed")

        if self._has_account_v2:
            try:
                log.info("account(): attempting account_information_v2")
                resp = await self._with_timeout(self._rest.rest_api.account_information_v2)
                data = _unwrap(resp)
                log.info("account(): v2 OK | %s", _summarize_payload(data))
                return data
            except Exception:
                log.exception("account(): SDK v2 failed")

        log.info("account(): SDK failed/unavailable -> REST /fapi/v2/account (signed)")
        data = await self._rest_get("/fapi/v2/account", {}, signed=True, timeout=max(self._timeout_s, 20))
        if isinstance(data, dict):
            data = _add_camel_aliases(data)
        log.info("account(): REST result | %s", _summarize_payload(data))
        return data

    async def balance(self) -> List[Dict]:
        """
        Try SDK v3, then v2, then REST fallback (/fapi/v2/balance, signed).
        All SDK calls bounded by _with_timeout.
        """
        # v3
        if self._has_bal_v3:
            log.info("balance(): attempting futures_account_balance_v3")
            try:
                resp = await self._with_timeout(self._rest.rest_api.futures_account_balance_v3)
                data = _unwrap(resp)
                log.info("balance(): v3 OK | %s", _summarize_payload(data))
                # Some SDK builds return empty list if no USDT; that's fine.
                return data if isinstance(data, list) else [data]
            except Exception:
                log.exception("balance(): v3 failed")

        # v2
        if self._has_bal_v2:
            log.info("balance(): attempting futures_account_balance_v2")
            try:
                resp = await self._with_timeout(self._rest.rest_api.futures_account_balance_v2)
                data = _unwrap(resp)
                log.info("balance(): v2 OK | %s", _summarize_payload(data))
                return data if isinstance(data, list) else [data]
            except Exception:
                log.exception("balance(): v2 failed")

        # REST fallback
        log.info("balance(): SDK failed -> REST /fapi/v2/balance (signed)")
        data = await self._rest_get("/fapi/v2/balance", {}, signed=True, timeout=max(self._timeout_s, 20))
        log.info("balance(): REST result | %s", _summarize_payload(data))
        return data if isinstance(data, list) else [data]

    async def balance_free_usdt(self) -> Decimal:
        """Convenience: return available/free USDT as Decimal."""
        bals = await self.balance()
        for b in bals or []:
            if str(b.get("asset")) == "USDT":
                amt = b.get("availableBalance") or b.get("balance") or "0"
                try:
                    v = Decimal(str(amt))
                except Exception:
                    v = Decimal("0")
                log.info("balance_free_usdt(): %s", v)
                return v
        log.info("balance_free_usdt(): USDT not found -> 0")
        return Decimal("0")

    async def position_information(self, symbol: Optional[str] = None) -> List[Dict]:
        """
        Unified accessor:
        - Try SDK v3, then v2 (both USER_DATA).
        - If SDK method(s) missing/failed, fallback to REST (signed):
            1) /fapi/v3/positionInfo
            2) /fapi/v2/positionRisk (older naming kept for compatibility)
        Returns a list[dict] in all cases.
        """
        params = {}
        if symbol:
            params["symbol"] = S(symbol)

        # SDK v3
        if self._has_posinfo_v3:
            log.info("position_information(): attempting position_information_v3")
            try:
                resp = await self._with_timeout(self._rest.rest_api.position_information_v3, **params)
                data = _unwrap(resp)
                log.info("position_information(): v3 OK | %s", _summarize_payload(data))
                return data if isinstance(data, list) else [data]
            except Exception:
                log.exception("position_information(): v3 failed")

        # SDK v2
        if self._has_posinfo_v2:
            log.info("position_information(): attempting position_information_v2")
            try:
                resp = await self._with_timeout(self._rest.rest_api.position_information_v2, **params)
                data = _unwrap(resp)
                log.info("position_information(): v2 OK | %s", _summarize_payload(data))
                return data if isinstance(data, list) else [data]
            except Exception:
                log.exception("position_information(): v2 failed")

        # REST fallback 1: /fapi/v3/positionInfo
        log.info("position_information(): SDK unavailable -> REST /fapi/v3/positionInfo (signed)")
        data = await self._rest_get("/fapi/v3/positionInfo", params, signed=True, timeout=max(self._timeout_s, 20))
        if isinstance(data, list):
            log.info("position_information(): REST v3 OK | list(len=%s)", len(data))
            return data

        # REST fallback 2: /fapi/v2/positionRisk
        log.info("position_information(): trying REST /fapi/v2/positionRisk (signed)")
        data2 = await self._rest_get("/fapi/v2/positionRisk", params, signed=True, timeout=max(self._timeout_s, 20))
        if isinstance(data2, list):
            log.info("position_information(): REST v2 OK | list(len=%s)", len(data2))
            return data2

        log.info("position_information(): REST non-list -> wrapping")
        return [data2 if isinstance(data2, dict) else {"raw": data2}]

    async def position_risk(self, symbol: Optional[str] = None) -> List[Dict]:
        """
        Back-compat wrapper so callers don't have to change.
        Binance SDK names this 'position_information_*'; we normalize here.
        """
        log.info("position_risk(): delegating to position_information() | sym=%s", symbol)
        return await self.position_information(symbol=symbol)

    # ---------- Trading ----------

    async def change_leverage(self, symbol: str, leverage: int) -> Dict:
        log.info("change_leverage(): %s -> %sx", symbol, leverage)
        resp = await self._with_timeout(self._rest.rest_api.change_initial_leverage, symbol=symbol, leverage=leverage)
        data = _unwrap(resp)
        log.info("change_leverage(): OK | %s", _summarize_payload(data))
        return data

    async def new_order(
        self,
        symbol: str,
        side: str,        # "BUY" | "SELL"
        order_type: str,  # "LIMIT" | "MARKET" | "STOP_MARKET" | ...
        quantity: float,
        price: Optional[float] = None,
        stopPrice: Optional[float] = None,
        timeInForce: str | None = "GTC",
        **kwargs,
    ) -> Dict:
        params = dict(symbol=S(symbol), side=side, type=order_type, quantity=quantity)
        if price is not None:
            params["price"] = price
        if stopPrice is not None:
            params["stopPrice"] = stopPrice
        if timeInForce and order_type == "LIMIT":
            params["timeInForce"] = timeInForce
        params.update({k: v for k, v in kwargs.items() if v is not None})

        log.info("new_order(): params=%s", {**params, "signature": "<hidden>"})
        resp = await self._with_timeout(self._rest.rest_api.new_order, **params)
        data = _unwrap(resp)
        log.info("new_order(): OK | %s", _summarize_payload(data))
        return data

    async def new_order_test(
        self,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float,
        price: Optional[float] = None,
        stopPrice: Optional[float] = None,
        timeInForce: str | None = "GTC",
        **kwargs,
    ) -> Dict:
        """
        POST /fapi/v1/order/test (signed). Validates order without placing.
        Retries once on -1021 using server time.
        """
        params = dict(symbol=S(symbol), side=side, type=order_type, quantity=quantity)
        if price is not None:
            params["price"] = price
        if stopPrice is not None:
            params["stopPrice"] = stopPrice
        if timeInForce and order_type == "LIMIT":
            params["timeInForce"] = timeInForce
        params.update({k: v for k, v in kwargs.items() if v is not None})

        params.setdefault("recvWindow", 5000)
        params["timestamp"] = int(time.time() * 1000)
        url = f"{self._base_url}/fapi/v1/order/test"
        params["signature"] = self._sign(params)

        def _post(p):
            return requests.post(url, params=p, headers=self._auth_headers(), timeout=max(self._timeout_s, 20))

        safe = {**params}
        safe.pop("signature", None)
        log.info("new_order_test(): POST %s | params=%s", url, safe)

        r = await asyncio.to_thread(_post, params)
        try:
            body = r.json()
        except Exception:
            body = {"raw": r.text, "status": r.status_code}

        log.info("new_order_test(): first attempt -> status=%s | %s", r.status_code, _summarize_payload(body))

        # Retry once on -1021 (timestamp)
        if r.status_code != 200 and (("-1021" in str(body)) or (isinstance(body, dict) and body.get("code") == -1021)):
            server_ms = await self._server_time_ms()
            if server_ms:
                params["timestamp"] = int(server_ms)
                params["signature"] = self._sign(params)
                log.info("new_order_test(): retrying with serverTime=%s", server_ms)
                r2 = await asyncio.to_thread(_post, params)
                try:
                    body = r2.json()
                except Exception:
                    body = {"raw": r2.text, "status": r2.status_code}
                log.info("new_order_test(): retry -> status=%s | %s", r2.status_code, _summarize_payload(body))
                if r2.status_code != 200 and isinstance(body, dict):
                    body.setdefault("status", r2.status_code)
                return body

        if r.status_code != 200 and isinstance(body, dict):
            body.setdefault("status", r.status_code)
        return body

    async def cancel_order(self, symbol: str, orderId: int) -> Dict:
        log.info("cancel_order(): %s #%s", symbol, orderId)
        resp = await self._with_timeout(self._rest.rest_api.cancel_order, symbol=S(symbol), orderId=orderId)
        data = _unwrap(resp)
        log.info("cancel_order(): OK | %s", _summarize_payload(data))
        return data

    async def query_order(self, symbol: str, orderId: int) -> Dict:
        log.info("query_order(): %s #%s", symbol, orderId)
        resp = await self._with_timeout(self._rest.rest_api.query_order, symbol=S(symbol), orderId=orderId)
        data = _unwrap(resp)
        log.info("query_order(): OK | %s", _summarize_payload(data))
        return data

    async def get_orders(self, symbol: Optional[str] = None) -> List[Dict]:
        log.info("get_orders(): sym=%s", symbol)
        resp = await self._with_timeout(
            self._rest.rest_api.current_open_orders, symbol=S(symbol) if symbol else None
        )
        data = _unwrap(resp)
        log.info("get_orders(): OK | %s", _summarize_payload(data))
        return data

    # ---------- Market data ----------

    async def exchange_info(self) -> Dict:
        log.info("exchange_info()")
        resp = await self._with_timeout(self._rest.rest_api.exchange_information)
        data = _unwrap(resp)
        log.info("exchange_info(): OK | %s", _summarize_payload(data))
        return data

    async def ticker_price(self, symbol: str) -> Dict:
        log.info("ticker_price(): %s", symbol)
        resp = await self._with_timeout(self._rest.rest_api.symbol_price_ticker, symbol=S(symbol))
        data = _unwrap(resp)
        log.info("ticker_price(): OK | %s", _summarize_payload(data))
        return data

    # ---------- Exchange filters / quantization ----------

    async def _get_symbol_filters(self, symbol: str) -> Dict[str, Decimal]:
        sym = S(symbol)
        cached = self._symbol_filters.get(sym)
        if cached:
            log.info("_get_symbol_filters(%s): cache hit", sym)
            return cached

        async with self._filters_lock:
            cached = self._symbol_filters.get(sym)
            if cached:
                log.info("_get_symbol_filters(%s): cache hit (after lock)", sym)
                return cached

            log.info("_get_symbol_filters(%s): fetching exchangeInfo", sym)
            info = await self.exchange_info()
            symbols = info.get("symbols", []) if isinstance(info, dict) else []
            filters_map: Dict[str, Dict[str, str]] = {}
            for entry in symbols:
                if str(entry.get("symbol", "")).upper() != sym:
                    continue
                raw_filters = entry.get("filters") or []
                filters_map = {str(f.get("filterType")): f for f in raw_filters if isinstance(f, dict)}
                break

            if not filters_map:
                log.error("_get_symbol_filters(%s): NOT FOUND in exchangeInfo", sym)
                raise ValueError(f"Exchange info does not contain symbol {sym}")

            lot_filter = filters_map.get("LOT_SIZE") or filters_map.get("MARKET_LOT_SIZE") or {}
            price_filter = filters_map.get("PRICE_FILTER") or {}
            min_notional_filter = filters_map.get("MIN_NOTIONAL") or {}

            def _to_decimal(value: str | float | int | None) -> Decimal:
                if value in (None, ""):
                    return Decimal("0")
                return Decimal(str(value))

            filters = {
                "step_size": _to_decimal(lot_filter.get("stepSize")),
                "min_qty": _to_decimal(lot_filter.get("minQty")),
                "tick_size": _to_decimal(price_filter.get("tickSize")),
                "min_notional": _to_decimal(
                    min_notional_filter.get("notional") or min_notional_filter.get("minNotional")
                ),
            }

            self._symbol_filters[sym] = filters
            log.info("_get_symbol_filters(%s): %s", sym, filters)
            return filters

    @staticmethod
    def _round_step(value: Decimal, step: Decimal) -> Decimal:
        if step <= 0:
            return value
        units = (value / step).to_integral_value(rounding=ROUND_DOWN)
        return (units * step).normalize()

    async def quantize(
        self,
        symbol: str,
        quantity: Decimal,
        price: Optional[Decimal] = None,
    ) -> Tuple[Decimal, Optional[Decimal]]:
        log.info("quantize(): sym=%s qty=%s price=%s", symbol, quantity, price)
        filters = await self._get_symbol_filters(symbol)
        qty = quantity if isinstance(quantity, Decimal) else Decimal(str(quantity))
        q_price = price if (price is None or isinstance(price, Decimal)) else Decimal(str(price))

        step_size = filters.get("step_size", Decimal("0"))
        min_qty = filters.get("min_qty", Decimal("0"))
        tick_size = filters.get("tick_size", Decimal("0"))
        min_notional = filters.get("min_notional", Decimal("0"))

        if qty < 0:
            qty = Decimal("0")
        qty = self._round_step(qty, step_size) if step_size > 0 else qty
        if min_qty > 0 and qty < min_qty:
            qty = Decimal("0")

        if q_price is not None and tick_size > 0:
            q_price = self._round_step(q_price, tick_size)

        if min_notional > 0 and q_price is not None and qty > 0:
            notional = qty * q_price
            if notional < min_notional:
                qty = Decimal("0")

        log.info("quantize(): -> qty=%s price=%s", qty, q_price)
        return qty, q_price

    # ---------- User Data Stream (listen key) ----------

    async def new_listen_key(self) -> str:
        log.info("new_listen_key()")
        resp = await self._with_timeout(self._rest.rest_api.start_user_data_stream)
        data = _unwrap(resp)
        lk = data.get("listenKey", "") if isinstance(data, dict) else ""
        log.info("new_listen_key(): %s", "obtained" if lk else "empty")
        return lk

    async def keepalive_listen_key(self, listen_key: str) -> None:
        log.info("keepalive_listen_key(%s)", listen_key[:8] + "...")
        await self._with_timeout(self._rest.rest_api.keepalive_user_data_stream, listenKey=listen_key)
        log.info("keepalive_listen_key(): OK")

    async def close_listen_key(self, listen_key: str) -> None:
        log.info("close_listen_key(%s)", listen_key[:8] + "...")
        await self._with_timeout(self._rest.rest_api.close_user_data_stream, listenKey=listen_key)
        log.info("close_listen_key(): OK")

    # ---------- Properties ----------

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def is_testnet(self) -> bool:
        return self._testnet
