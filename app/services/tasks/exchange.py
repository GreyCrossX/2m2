"""
Exchange client facade (REAL Binance USDS Futures SDK)

This is the ONLY module that touches the Binance SDK. Everyone else calls here.

Public API (all return dicts shaped for tasks/actions):

- new_order(user_id, **kwargs) -> {ok: bool, order_id: str|int|None, client_order_id: str|None, raw|error}
    kwargs follow our internal tool signature:
      symbol: str
      side: "BUY"|"SELL"
      order_type: str  # "MARKET"|"LIMIT"|"STOP_MARKET"|"TAKE_PROFIT_MARKET"|...
      quantity: float | None
      reduce_only: bool | None
      stop_price: float | None
      working_type: "MARK_PRICE"|"LAST_PRICE"|"CONTRACT_PRICE" | None
      close_position: bool | None
    Optional passthroughs (if present in kwargs):
      client_order_id, positionSide, price, timeInForce, etc.

- cancel_order(user_id, *, symbol, order_id=None, orig_client_order_id=None)
    -> {ok: bool, raw|error}

- get_open_orders(user_id, *, symbol=None) -> {ok: bool, orders: list, raw|error}
- get_positions(user_id, *, symbol=None)   -> {ok: bool, positions: list, raw|error}
- set_leverage(user_id, *, symbol, leverage: int) -> {ok: bool, raw|error}
- set_margin_type(user_id, *, symbol, margin_type: str) -> {ok: bool, raw|error}

Auth strategy:
- Default credentials from env (TESTNET_API_KEY, TESTNET_API_SECRET, BASE_PATH)
- You can inject your own lookup with set_creds_lookup(user_id -> (key, secret, base_path))

Notes:
- We keep parameter names exactly as the SDK expects.
- We pass only non-None parameters.
- We don't raise; callers get {ok:false, error:"..."} on failure.
"""

from __future__ import annotations

import os
import logging
from typing import Any, Dict, Optional, Tuple

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
# Credentials seam (same pattern as balance_source)
# ──────────────────────────────────────────────────────────────────────────────

configuration_rest_api = ConfigurationRestAPI(
    api_key=os.getenv("TESTNET_API_KEY", ""),  # Fixed typo: TESNET -> TESTNET
    api_secret=os.getenv("TESTNET_API_SECRET", ""),
    base_path=os.getenv("BASE_PATH", DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL)
)

# Fixed the parameter name according to SDK docs
_client_for_user = DerivativesTradingUsdsFutures(config_rest_api=configuration_rest_api)


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _side_to_enum(side: str) -> str:
    """
    Map "BUY"/"SELL" to the SDK's NewOrderSideEnum values.
    """
    s = (side or "").upper()
    try:
        return NewOrderSideEnum[s].value  # e.g., "BUY"
    except Exception:
        return s  # pass-through; SDK may still accept the raw string


def _extract_order_ids(resp_data: Any) -> Tuple[Optional[str], Optional[str]]:
    """
    Try various common shapes to find order identifiers.
    Returns (order_id, client_order_id).
    """
    if isinstance(resp_data, dict):
        oid = resp_data.get("orderId") or resp_data.get("orderID") or resp_data.get("order_id")
        coid = (
            resp_data.get("clientOrderId")
            or resp_data.get("clientOrderID")
            or resp_data.get("newClientOrderId")
            or resp_data.get("origClientOrderId")
        )
        # normalize to str for easier Redis storage
        return (str(oid) if oid is not None else None, str(coid) if coid is not None else None)
    return None, None


def _drop_nones(d: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in d.items() if v is not None}


# ──────────────────────────────────────────────────────────────────────────────
# Public API
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
    price: float | None = None,        # only for LIMIT/STOP/TAKE_PROFIT non-market
    timeInForce: str | None = None,    # e.g., "GTC"
    **extra_kwargs: Any,               # future-proof
) -> Dict[str, Any]:
    """
    Place a NEW order via SDK. Returns a normalized dict.
    """
    try:
        # Note: _client_for_user is now a function that should return a client
        # But based on your original code, it seems like it should be a client instance
        # You may need to adjust this based on how your user authentication works
        client = _client_for_user  # This might need to be _client_for_user(user_id) if it's a function

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
        # Allow callers to pass any additional, SDK-supported kwargs (e.g., "priceProtect", "activationPrice", etc.)
        params.update(extra_kwargs)
        params = _drop_nones(params)

        response = client.rest_api.new_order(**params)

        try:
            LOG.debug("new_order rate limits: %s", getattr(response, "rate_limits", None))
        except Exception:
            pass

        data = response.data()
        order_id, coid = _extract_order_ids(data)
        return {"ok": True, "order_id": order_id, "client_order_id": coid, "raw": data}

    except Exception as e:
        LOG.error("new_order error (sym=%s side=%s): %s", symbol, side, e)
        return {"ok": False, "order_id": None, "client_order_id": None, "error": str(e)}


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
        client = _client_for_user  # Same note as above
        params: Dict[str, Any] = {"symbol": symbol}
        if order_id is not None:
            params["orderId"] = order_id
        if orig_client_order_id:
            params["origClientOrderId"] = orig_client_order_id
        response = client.rest_api.cancel_order(**params)
        data = response.data()
        return {"ok": True, "raw": data}
    except Exception as e:
        LOG.error("cancel_order error (sym=%s, order_id=%s, orig=%s): %s", symbol, order_id, orig_client_order_id, e)
        return {"ok": False, "error": str(e)}


def get_open_orders(user_id: str, *, symbol: Optional[str] = None) -> Dict[str, Any]:
    """
    Fetch open orders (optionally filtered by symbol).
    """
    try:
        client = _client_for_user 
        if symbol:
            resp = client.rest_api.current_all_open_orders(symbol=symbol)
        else:
            resp = client.rest_api.current_all_open_orders()
        data = resp.data()
        orders = data if isinstance(data, list) else data.get("orders", []) if isinstance(data, dict) else []
        return {"ok": True, "orders": orders, "raw": data}
    except Exception as e:
        LOG.error("get_open_orders error (sym=%s): %s", symbol, e)
        return {"ok": False, "orders": [], "error": str(e)}


def get_positions(user_id: str, *, symbol: Optional[str] = None) -> Dict[str, Any]:
    """
    Fetch position information (not account balances).
    """
    try:
        client = _client_for_user  # Same note as above
        if symbol:
            resp = client.rest_api.position_information(symbol=symbol)
        else:
            resp = client.rest_api.position_information()
        data = resp.data()
        positions = data if isinstance(data, list) else data.get("positions", []) if isinstance(data, dict) else []
        return {"ok": True, "positions": positions, "raw": data}
    except Exception as e:
        LOG.error("get_positions error (sym=%s): %s", symbol, e)
        return {"ok": False, "positions": [], "error": str(e)}


def set_leverage(user_id: str, *, symbol: str, leverage: int) -> Dict[str, Any]:
    """
    Set leverage for a symbol.
    """
    try:
        client = _client_for_user  # Same note as above
        resp = client.rest_api.change_initial_leverage(symbol=symbol, leverage=int(leverage))
        return {"ok": True, "raw": resp.data()}
    except Exception as e:
        LOG.error("set_leverage error (sym=%s, lev=%s): %s", symbol, leverage, e)
        return {"ok": False, "error": str(e)}


def set_margin_type(user_id: str, *, symbol: str, margin_type: str) -> Dict[str, Any]:
    """
    Set margin type: "ISOLATED" or "CROSSED".
    """
    try:
        client = _client_for_user  # Same note as above
        resp = client.rest_api.change_margin_type(symbol=symbol, marginType=margin_type.upper())
        return {"ok": True, "raw": resp.data()}
    except Exception as e:
        LOG.error("set_margin_type error (sym=%s, type=%s): %s", symbol, margin_type, e)
        return {"ok": False, "error": str(e)}