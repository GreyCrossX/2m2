"""
Exchange client facade (REAL Binance USDS Futures SDK, MAINNET only)

This module is the ONLY place that touches the Binance USDS Futures SDK.
Everyone else calls here.

ENV (mainnet only):
  BINANCE_USDSF_API_KEY
  BINANCE_USDSF_API_SECRET
  BINANCE_USDSF_BASE_URL  (optional; defaults to SDK's PROD URL)

Optional per-user creds:
  Call set_creds_lookup(fn) where fn: (user_id: str) -> tuple[str, str, str|None]
  returning (api_key, api_secret, base_url_or_none). If fn returns None or
  empty, we fall back to env defaults.

Public API (return dicts shaped for tasks/actions):

- new_order(user_id, **kwargs) -> {ok: bool, order_id: str|None, client_order_id: str|None, raw|error}
    kwargs (mirrors your internal signature; we pass non-Nones only):
      symbol: str
      side: "BUY"|"SELL"
      order_type: str  # "MARKET"|"LIMIT"|"STOP_MARKET"|"TAKE_PROFIT_MARKET"|...
      quantity: float | None
      reduce_only: bool | None
      stop_price: float | None
      working_type: "MARK_PRICE"|"LAST_PRICE"|"CONTRACT_PRICE" | None
      close_position: bool | None
      client_order_id: str | None
      positionSide: str | None        # hedge mode
      price: float | None             # LIMIT/STOP/TP non-market
      timeInForce: str | None         # e.g., "GTC"
      ...any extra SDK-supported kwargs (activationPrice, priceProtect, etc.)

- cancel_order(user_id, *, symbol, order_id=None, orig_client_order_id=None)
    -> {ok: bool, raw|error}

- get_open_orders(user_id, *, symbol: str|None=None) -> {ok: bool, orders: list, raw|error}
- get_positions(user_id, *, symbol: str|None=None)   -> {ok: bool, positions: list, raw|error}
- set_leverage(user_id, *, symbol: str, leverage: int) -> {ok: bool, raw|error}
- set_margin_type(user_id, *, symbol: str, margin_type: str) -> {ok: bool, raw|error}

Notes:
- MAINNET ONLY. We intentionally do not support testnet here.
- We pass only non-None params to the SDK.
- We never raise; callers get {ok: False, error: "..."} on failure.
"""

from __future__ import annotations

import os
import logging
from typing import Any, Callable, Dict, Optional, Tuple
from functools import lru_cache

from binance_sdk_derivatives_trading_usds_futures.derivatives_trading_usds_futures import (
    DerivativesTradingUsdsFutures,
    ConfigurationRestAPI,
    DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL,
)
from binance_sdk_derivatives_trading_usds_futures.rest_api.models import (
    NewOrderSideEnum,
)

LOG = logging.getLogger("exchange")

# ──────────────────────────────────────────────────────────────────────────────
# Configuration & Credentials
# ──────────────────────────────────────────────────────────────────────────────

ENV_KEY = "BINANCE_USDSF_API_KEY"
ENV_SECRET = "BINANCE_USDSF_API_SECRET"
ENV_BASE_URL = "BINANCE_USDSF_BASE_URL"  # optional

_default_api_key = os.getenv(ENV_KEY, "") or ""
_default_api_secret = os.getenv(ENV_SECRET, "") or ""
_default_base_url = os.getenv(ENV_BASE_URL, "") or DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL

# Optional user->creds lookup hook (e.g., fetch from DB). Should return
# (api_key, api_secret, base_url_or_none) or None.
_creds_lookup: Optional[Callable[[str], Optional[Tuple[str, str, Optional[str]]]]] = None


def set_creds_lookup(fn: Callable[[str], Optional[Tuple[str, str, Optional[str]]]]) -> None:
    """
    Install a function that returns user-specific credentials:
      fn(user_id) -> (api_key, api_secret, base_url_or_none) or None
    If None, env defaults are used.
    """
    global _creds_lookup
    _creds_lookup = fn
    # Clear cache so new hook takes effect immediately
    _client_for_user.cache_clear()


# ──────────────────────────────────────────────────────────────────────────────
# Client Factory (cached per (user_id, api_key, api_secret, base_url))
# ──────────────────────────────────────────────────────────────────────────────

def _resolve_creds(user_id: str) -> Tuple[str, str, str]:
    """
    Resolve (api_key, api_secret, base_url) for user.
    Falls back to env defaults if no user-specific creds are found.
    """
    if _creds_lookup:
        try:
            res = _creds_lookup(user_id)
            if res and len(res) >= 2:
                k, s, b = (res + (None,))[:3]  # pad if needed
                base = (b or "").strip() or _default_base_url
                return (k or "", s or "", base)
        except Exception as e:
            LOG.error("creds_lookup failed for user_id=%s: %s", user_id, e)

    return (_default_api_key, _default_api_secret, _default_base_url)


@lru_cache(maxsize=256)
def _client_for_user(user_id: str, api_key: str, api_secret: str, base_url: str) -> DerivativesTradingUsdsFutures:
    """
    Build & cache a DerivativesTradingUsdsFutures client for the given creds.
    Cache key includes the resolved creds so changing keys yields a new client.
    """
    cfg = ConfigurationRestAPI(api_key=api_key, api_secret=api_secret, base_path=base_url)
    return DerivativesTradingUsdsFutures(config_rest_api=cfg)


def _get_client(user_id: str) -> DerivativesTradingUsdsFutures:
    k, s, b = _resolve_creds(user_id)
    if not k or not s:
        LOG.error("Missing Binance credentials (user_id=%s). Check %s / %s or creds lookup.", user_id, ENV_KEY, ENV_SECRET)
    return _client_for_user(user_id, k, s, b)


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _side_to_enum(side: str) -> str:
    """
    Map 'BUY'/'SELL' to SDK enum values; pass-through unknowns.
    """
    s = (side or "").upper()
    try:
        return NewOrderSideEnum[s].value  # returns "BUY" or "SELL"
    except Exception:
        return s


def _extract_order_ids(resp_data: Any) -> Tuple[Optional[str], Optional[str]]:
    """
    Try common shapes for order identifiers. Returns (order_id, client_order_id).
    """
    if isinstance(resp_data, dict):
        oid = resp_data.get("orderId") or resp_data.get("orderID") or resp_data.get("order_id")
        coid = (
            resp_data.get("clientOrderId")
            or resp_data.get("clientOrderID")
            or resp_data.get("newClientOrderId")
            or resp_data.get("origClientOrderId")
        )
        return (str(oid) if oid is not None else None, str(coid) if coid is not None else None)
    return None, None


def _drop_nones(d: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in d.items() if v is not None}


def _wrap_ok(data: Any, **extra) -> Dict[str, Any]:
    return {"ok": True, **extra, "raw": data}


def _wrap_err(err: Exception | str) -> Dict[str, Any]:
    msg = str(err)
    return {"ok": False, "error": msg}


# ──────────────────────────────────────────────────────────────────────────────
# Public Facade
# ──────────────────────────────────────────────────────────────────────────────

def new_order(
    user_id: str,
    *,
    symbol: str,
    side: str,
    order_type: str,
    quantity: float | None = None,
    reduce_only: bool | None = None,
    stop_price: float | None = None,
    working_type: str | None = "MARK_PRICE",
    close_position: bool | None = None,
    # Optional passthroughs:
    client_order_id: str | None = None,
    positionSide: str | None = None,   # hedge mode
    price: float | None = None,        # for LIMIT/STOP/TP non-market
    timeInForce: str | None = None,    # e.g., "GTC"
    **extra_kwargs: Any,
) -> Dict[str, Any]:
    """
    Place a NEW order via SDK. Returns a normalized dict.
    """
    try:
        client = _get_client(user_id)

        params: Dict[str, Any] = {
            "symbol": symbol,
            "side": _side_to_enum(side),
            "type": order_type,
            "quantity": quantity,
            "reduceOnly": reduce_only,
            "stopPrice": stop_price,
            "workingType": working_type,
            "closePosition": close_position,
            "newClientOrderId": client_order_id,
            "positionSide": positionSide,
            "price": price,
            "timeInForce": timeInForce,
        }
        params.update(extra_kwargs)
        params = _drop_nones(params)

        resp = client.rest_api.new_order(**params)

        try:
            LOG.debug("new_order rate limits: %s", getattr(resp, "rate_limits", None))
        except Exception:
            pass

        data = resp.data()
        order_id, coid = _extract_order_ids(data)
        return _wrap_ok(data, order_id=order_id, client_order_id=coid)

    except Exception as e:
        LOG.error("new_order error (user=%s sym=%s side=%s): %s", user_id, symbol, side, e)
        return _wrap_err(e)


def cancel_order(
    user_id: str,
    *,
    symbol: str,
    order_id: str | int | None = None,
    orig_client_order_id: str | None = None,
) -> Dict[str, Any]:
    """
    Cancel a single order by orderId or clientOrderId.
    """
    try:
        client = _get_client(user_id)
        params: Dict[str, Any] = {"symbol": symbol}
        if order_id is not None:
            params["orderId"] = order_id
        if orig_client_order_id:
            params["origClientOrderId"] = orig_client_order_id

        resp = client.rest_api.cancel_order(**params)
        return _wrap_ok(resp.data())

    except Exception as e:
        LOG.error("cancel_order error (user=%s sym=%s order_id=%s orig=%s): %s",
                  user_id, symbol, order_id, orig_client_order_id, e)
        return _wrap_err(e)


def get_open_orders(user_id: str, *, symbol: Optional[str] = None) -> Dict[str, Any]:
    """
    Fetch open orders (optionally filtered by symbol).
    """
    try:
        client = _get_client(user_id)
        if symbol:
            resp = client.rest_api.current_all_open_orders(symbol=symbol)
        else:
            resp = client.rest_api.current_all_open_orders()

        data = resp.data()
        orders = data if isinstance(data, list) else (data.get("orders", []) if isinstance(data, dict) else [])
        return _wrap_ok(data, orders=orders)

    except Exception as e:
        LOG.error("get_open_orders error (user=%s sym=%s): %s", user_id, symbol, e)
        return _wrap_err(e)


def get_positions(user_id: str, *, symbol: Optional[str] = None) -> Dict[str, Any]:
    """
    Fetch position information.
    """
    try:
        client = _get_client(user_id)
        if symbol:
            resp = client.rest_api.position_information(symbol=symbol)
        else:
            resp = client.rest_api.position_information()

        data = resp.data()
        positions = data if isinstance(data, list) else (data.get("positions", []) if isinstance(data, dict) else [])
        return _wrap_ok(data, positions=positions)

    except Exception as e:
        LOG.error("get_positions error (user=%s sym=%s): %s", user_id, symbol, e)
        return _wrap_err(e)


def set_leverage(user_id: str, *, symbol: str, leverage: int) -> Dict[str, Any]:
    """
    Set leverage for a symbol.
    """
    try:
        client = _get_client(user_id)
        resp = client.rest_api.change_initial_leverage(symbol=symbol, leverage=int(leverage))
        return _wrap_ok(resp.data())

    except Exception as e:
        LOG.error("set_leverage error (user=%s sym=%s lev=%s): %s", user_id, symbol, leverage, e)
        return _wrap_err(e)


def set_margin_type(user_id: str, *, symbol: str, margin_type: str) -> Dict[str, Any]:
    """
    Set margin type: "ISOLATED" or "CROSSED".
    """
    try:
        client = _get_client(user_id)
        resp = client.rest_api.change_margin_type(symbol=symbol, marginType=margin_type.upper())
        return _wrap_ok(resp.data())

    except Exception as e:
        LOG.error("set_margin_type error (user=%s sym=%s type=%s): %s", user_id, symbol, margin_type, e)
        return _wrap_err(e)
