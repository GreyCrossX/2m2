from __future__ import annotations

import hashlib
import hmac
import logging
import os
import threading
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable, Dict, Iterable, Mapping, MutableMapping, Optional, Tuple
from urllib.parse import urlencode

import requests

try:  # pragma: no cover - exercised via tests with a stub
    from binance_sdk_derivatives_trading_usds_futures.derivatives_trading_usds_futures import (
        ConfigurationRestAPI,
        DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL,
        DerivativesTradingUsdsFutures,
    )
except Exception as exc:  # pragma: no cover - defensive guard for missing dependency
    raise RuntimeError(
        "Binance USDS futures SDK is required (pip install binance-futures-connector)."
    ) from exc

# Some SDK builds export a TESTNET URL constant; provide a safe fallback if absent.
try:  # pragma: no cover - attribute availability depends on SDK version
    from binance_sdk_derivatives_trading_usds_futures.derivatives_trading_usds_futures import (
        DERIVATIVES_TRADING_USDS_FUTURES_REST_API_TESTNET_URL,
    )
except Exception:  # pragma: no cover - best effort fallback
    DERIVATIVES_TRADING_USDS_FUTURES_REST_API_TESTNET_URL = "https://testnet.binancefuture.com"


log = logging.getLogger("app.services.tasks.exchange")

_DEFAULT_TIMEOUT_SECONDS = int(os.getenv("BINANCE_USDSF_TIMEOUT", "30"))
_HTTP_TIMEOUT = max(_DEFAULT_TIMEOUT_SECONDS, 10)

_ClientKey = Tuple[str, str, str]
_CLIENT_CACHE: Dict[_ClientKey, DerivativesTradingUsdsFutures] = {}
_CLIENT_LOCK = threading.RLock()
_CREDS_LOOKUP: Optional[Callable[[str], Tuple[str, str, Optional[str]]]] = None


@dataclass(frozen=True)
class _Creds:
    api_key: str
    api_secret: str
    base_url: str


def set_creds_lookup(func: Optional[Callable[[str], Tuple[str, str, Optional[str]]]]) -> None:
    """Install a user->(api_key, api_secret, base_url) resolver.

    Passing ``None`` clears the lookup and falls back to environment variables.
    The client cache is cleared so subsequent calls pick up new credentials.
    """

    global _CREDS_LOOKUP
    if func is not None and not callable(func):  # pragma: no cover - defensive
        raise TypeError("set_creds_lookup expects a callable or None")
    _CREDS_LOOKUP = func
    clear_client_cache()
    log.info("set_creds_lookup installed -> cache cleared")


def clear_client_cache() -> None:
    with _CLIENT_LOCK:
        _CLIENT_CACHE.clear()


def _resolve_creds(user_id: str) -> _Creds:
    """Resolve credentials for *user_id* via lookup or environment defaults."""

    api_key = os.getenv("BINANCE_USDSF_API_KEY", "")
    api_secret = os.getenv("BINANCE_USDSF_API_SECRET", "")
    base_url = os.getenv("BINANCE_USDSF_BASE_URL", "").strip()

    if _CREDS_LOOKUP is not None:
        try:
            resolved = _CREDS_LOOKUP(user_id)
        except Exception as exc:  # pragma: no cover - propagation tested via callers
            log.exception("creds lookup failed | user_id=%s", user_id)
            raise RuntimeError(f"credential lookup failed for {user_id}: {exc}") from exc
        if not isinstance(resolved, Iterable):  # pragma: no cover - defensive
            raise RuntimeError("credential lookup must return an iterable of length 2 or 3")
        try:
            api_key, api_secret, *rest = resolved  # type: ignore[misc]
        except ValueError as exc:  # pragma: no cover - defensive
            raise RuntimeError("credential lookup must return 2 or 3 elements") from exc
        if rest:
            base_url = (rest[0] or "").strip()

    base_url = base_url or DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL
    return _Creds(api_key=str(api_key), api_secret=str(api_secret), base_url=base_url)


def _client_for_user(
    user_id: str,
    api_key: Optional[str] = None,
    api_secret: Optional[str] = None,
    base_url: Optional[str] = None,
) -> DerivativesTradingUsdsFutures:
    """Return (and cache) a configured SDK client for ``user_id``."""

    if api_key is None or api_secret is None or base_url is None:
        creds = _resolve_creds(user_id)
    else:
        creds = _Creds(api_key, api_secret, base_url or DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL)

    cache_key: _ClientKey = (user_id, creds.base_url, creds.api_key)
    with _CLIENT_LOCK:
        client = _CLIENT_CACHE.get(cache_key)
        if client is not None:
            return client

        try:
            conf = ConfigurationRestAPI(
                api_key=creds.api_key,
                api_secret=creds.api_secret,
                base_path=creds.base_url,
                timeout=_DEFAULT_TIMEOUT_SECONDS * 1000,
            )
        except TypeError:  # pragma: no cover - exercised by contract tests with a stub
            conf = ConfigurationRestAPI(
                api_key=creds.api_key,
                api_secret=creds.api_secret,
                base_path=creds.base_url,
            )
        client = DerivativesTradingUsdsFutures(config_rest_api=conf)
        _CLIENT_CACHE[cache_key] = client
        log.info("created new Binance SDK client | user_id=%s base=%s", user_id, creds.base_url)
        return client


def _unwrap(resp: Any) -> Any:
    """Normalise SDK responses that expose ``data``/``data()`` helpers."""

    if resp is None:
        return None
    for attr in ("data", "data"):
        try:
            value = getattr(resp, attr)
        except Exception:
            continue
        if callable(value):
            try:
                return value()
            except Exception:
                continue
        return value
    return resp


def _numeric(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    return value


def _build_order_payload(kwargs: Mapping[str, Any]) -> Dict[str, Any]:
    symbol = kwargs.get("symbol")
    side = kwargs.get("side")
    order_type = str(kwargs.get("order_type", "")).upper()
    payload: Dict[str, Any] = {
        "symbol": str(symbol).upper() if symbol else symbol,
        "side": str(side).upper() if side else side,
        "type": order_type,
    }

    qty = kwargs.get("quantity")
    if qty is not None:
        payload["quantity"] = _numeric(qty)

    price = kwargs.get("price")
    time_in_force = kwargs.get("timeInForce")
    stop_price = kwargs.get("stop_price")

    # STOP/TAKE_PROFIT (non-market) behave like stop-limit orders.
    if order_type in {"STOP", "TAKE_PROFIT"}:
        if stop_price is not None:
            payload["stopPrice"] = _numeric(stop_price)
        if price is not None:
            payload["price"] = _numeric(price)
        if time_in_force is not None:
            payload["timeInForce"] = time_in_force
    elif order_type in {"STOP_MARKET", "TAKE_PROFIT_MARKET", "MARKET"}:
        if stop_price is not None:
            payload["stopPrice"] = _numeric(stop_price)
        # Explicitly drop price/timeInForce for market variants.
    else:  # e.g. LIMIT
        if price is not None:
            payload["price"] = _numeric(price)
        if time_in_force is not None:
            payload["timeInForce"] = time_in_force
        # LIMIT should not send stopPrice

    mapping = {
        "reduce_only": ("reduceOnly", bool if kwargs.get("reduce_only") is not None else None),
        "working_type": "workingType",
        "close_position": ("closePosition", bool if kwargs.get("close_position") is not None else None),
        "client_order_id": "newClientOrderId",
        "positionSide": "positionSide",
        "priceProtect": ("priceProtect", bool if kwargs.get("priceProtect") is not None else None),
    }

    for src, dest in mapping.items():
        value = kwargs.get(src)
        if value is None:
            continue
        if isinstance(dest, tuple):
            dest_name, coercer = dest
            payload[dest_name] = coercer(value) if coercer else value
        else:
            payload[dest] = value if src != "positionSide" else str(value).upper()

    # working_type maps from kw "working_type"
    if "working_type" in kwargs and kwargs["working_type"] is not None:
        payload["workingType"] = kwargs["working_type"]

    # Remove any keys with value None to satisfy contract tests.
    return {k: v for k, v in payload.items() if v is not None}


def new_order(**kwargs: Any) -> Dict[str, Any]:
    """Place a new order synchronously via the SDK REST client."""

    user_id = kwargs.pop("user_id")
    creds = _resolve_creds(user_id)
    client = _client_for_user(user_id, creds.api_key, creds.api_secret, creds.base_url)

    payload = _build_order_payload(kwargs)
    try:
        resp = client.rest_api.new_order(**payload)
        data = _unwrap(resp) or {}
        return {
            "ok": True,
            "order_id": data.get("orderId"),
            "client_order_id": data.get("clientOrderId") or data.get("origClientOrderId"),
            "raw": data,
        }
    except Exception as exc:  # pragma: no cover - exercised via tests
        log.exception("new_order failed | user_id=%s", user_id)
        return {"ok": False, "error": str(exc)}


def cancel_order(user_id: str, symbol: str, order_id: Optional[int] = None, orig_client_order_id: Optional[str] = None) -> Dict[str, Any]:
    creds = _resolve_creds(user_id)
    client = _client_for_user(user_id, creds.api_key, creds.api_secret, creds.base_url)
    params: Dict[str, Any] = {"symbol": str(symbol).upper()}
    if order_id is not None:
        params["orderId"] = int(order_id)
    if orig_client_order_id is not None:
        params["origClientOrderId"] = orig_client_order_id
    try:
        resp = client.rest_api.cancel_order(**params)
        data = _unwrap(resp) or {}
        return {"ok": True, "raw": data}
    except Exception as exc:  # pragma: no cover - defensive
        log.exception("cancel_order failed | user_id=%s", user_id)
        return {"ok": False, "error": str(exc)}


def get_open_orders(user_id: str, symbol: Optional[str] = None) -> Dict[str, Any]:
    creds = _resolve_creds(user_id)
    client = _client_for_user(user_id, creds.api_key, creds.api_secret, creds.base_url)
    params = {"symbol": str(symbol).upper()} if symbol else {}
    try:
        resp = client.rest_api.current_all_open_orders(**params)
        data = _unwrap(resp) or []
        return {"ok": True, "orders": data}
    except Exception as exc:  # pragma: no cover - defensive
        log.exception("get_open_orders failed | user_id=%s", user_id)
        return {"ok": False, "error": str(exc)}


def get_positions(user_id: str, symbol: Optional[str] = None) -> Dict[str, Any]:
    creds = _resolve_creds(user_id)
    client = _client_for_user(user_id, creds.api_key, creds.api_secret, creds.base_url)
    params = {"symbol": str(symbol).upper()} if symbol else {}
    try:
        resp = client.rest_api.position_information(**params)
        data = _unwrap(resp) or []
        return {"ok": True, "positions": data}
    except Exception as exc:  # pragma: no cover
        log.exception("get_positions failed | user_id=%s", user_id)
        return {"ok": False, "error": str(exc)}


def set_leverage(user_id: str, symbol: str, leverage: int) -> Dict[str, Any]:
    creds = _resolve_creds(user_id)
    client = _client_for_user(user_id, creds.api_key, creds.api_secret, creds.base_url)
    try:
        resp = client.rest_api.change_initial_leverage(symbol=str(symbol).upper(), leverage=int(leverage))
        data = _unwrap(resp) or {}
        return {"ok": True, "raw": data}
    except Exception as exc:  # pragma: no cover
        log.exception("set_leverage failed | user_id=%s", user_id)
        return {"ok": False, "error": str(exc)}


def set_margin_type(user_id: str, symbol: str, margin_type: str) -> Dict[str, Any]:
    creds = _resolve_creds(user_id)
    client = _client_for_user(user_id, creds.api_key, creds.api_secret, creds.base_url)
    try:
        resp = client.rest_api.change_margin_type(symbol=str(symbol).upper(), marginType=str(margin_type).upper())
        data = _unwrap(resp) or {}
        return {"ok": True, "raw": data}
    except Exception as exc:  # pragma: no cover
        log.exception("set_margin_type failed | user_id=%s", user_id)
        return {"ok": False, "error": str(exc)}


def _signed_get(base_url: str, path: str, api_key: str, api_secret: str, params: Optional[MutableMapping[str, Any]] = None) -> Dict[str, Any]:
    params = dict(params or {})
    params.setdefault("recvWindow", 5000)
    params["timestamp"] = int(time.time() * 1000)
    qs = urlencode(params)
    signature = hmac.new(api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
    params["signature"] = signature
    headers = {"X-MBX-APIKEY": api_key}
    url = f"{base_url.rstrip('/')}{path}"
    response = requests.get(url, params=params, headers=headers, timeout=_HTTP_TIMEOUT)
    response.raise_for_status()
    return response.json()


def probe_position_mode(user_id: str) -> Dict[str, Any]:
    creds = _resolve_creds(user_id)
    try:
        data = _signed_get(creds.base_url, "/fapi/v1/positionSide/dual", creds.api_key, creds.api_secret)
    except Exception as exc:  # pragma: no cover - network dependent
        log.exception("probe_position_mode failed | user_id=%s", user_id)
        return {"ok": False, "error": str(exc)}

    raw_value = data.get("dualSidePosition")
    mode = "unknown"
    if isinstance(raw_value, bool):
        mode = "hedge" if raw_value else "one_way"
    elif isinstance(raw_value, str):
        lower = raw_value.lower()
        if lower in {"true", "1"}:
            mode = "hedge"
        elif lower in {"false", "0"}:
            mode = "one_way"

    return {"ok": True, "mode": mode, "raw": data}


def _exchange_info(base_url: str, symbol: str) -> Dict[str, Any]:
    url = f"{base_url.rstrip('/')}/fapi/v1/exchangeInfo"
    resp = requests.get(url, params={"symbol": symbol.upper()}, timeout=_HTTP_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def _extract_filters(info: Mapping[str, Any], symbol: str) -> Dict[str, Any]:
    symbols = info.get("symbols") or []
    for sym in symbols:
        if str(sym.get("symbol")) == symbol.upper():
            filters: Dict[str, Any] = {}
            for f in sym.get("filters", []):
                f_type = f.get("filterType")
                if f_type == "PRICE_FILTER":
                    filters["tick_size"] = f.get("tickSize")
                elif f_type == "LOT_SIZE":
                    filters["step_size"] = f.get("stepSize")
                    filters["min_qty"] = f.get("minQty")
                elif f_type in {"MIN_NOTIONAL", "NOTIONAL"}:
                    filters["min_notional"] = f.get("notional") or f.get("minNotional")
            return filters
    return {}


def _selftest_probe(user_id: str, symbol: str) -> Dict[str, Any]:
    creds = _resolve_creds(user_id)
    try:
        info = _exchange_info(creds.base_url, symbol)
        filters = _extract_filters(info, symbol)
        position_mode = probe_position_mode(user_id)
    except Exception as exc:  # pragma: no cover - network dependent
        log.exception("_selftest_probe failed | user_id=%s", user_id)
        return {"ok": False, "error": str(exc)}

    ok = position_mode.get("ok", False)
    return {
        "ok": ok,
        "exchange_info": info,
        "filters": filters,
        "position_mode": position_mode,
    }


__all__ = [
    "set_creds_lookup",
    "clear_client_cache",
    "new_order",
    "cancel_order",
    "get_open_orders",
    "get_positions",
    "set_leverage",
    "set_margin_type",
    "probe_position_mode",
    "_selftest_probe",
    "_client_for_user",  # exposed for tests to monkeypatch
]
