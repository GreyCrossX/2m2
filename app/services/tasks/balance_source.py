from __future__ import annotations

import os
import time
import hmac
import hashlib
import logging
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple, Callable
from urllib.parse import urlencode

import requests

from binance_sdk_derivatives_trading_usds_futures.derivatives_trading_usds_futures import (
    DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL,
)

LOG = logging.getLogger("balance_source")

ENV_KEY = "BINANCE_USDSF_API_KEY"
ENV_SECRET = "BINANCE_USDSF_API_SECRET"
ENV_BASE_URL = "BINANCE_USDSF_BASE_URL"

RECV_WINDOW_MS = int(os.getenv("BINANCE_RECV_WINDOW_MS", "60000"))

_creds_lookup: Optional[Callable[[str], Optional[Tuple[str, str, Optional[str]]]]] = None
_sessions: Dict[Tuple[str, str], requests.Session] = {}


def set_creds_lookup(fn: Callable[[str], Optional[Tuple[str, str, Optional[str]]]]) -> None:
    """
    Install a function that returns user-specific credentials:
      fn(user_id) -> (api_key, api_secret, base_url_or_none) or None
    """
    global _creds_lookup
    _creds_lookup = fn


def _resolve_creds(user_id: str) -> Tuple[str, str, str]:
    if _creds_lookup:
        try:
            res = _creds_lookup(user_id)
            if res and len(res) >= 2:
                k, s, b = (res + (None,))[:3]
                base = (b or "").strip() or os.getenv(ENV_BASE_URL, "") or DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL
                return (k or "", s or "", base)
        except Exception as e:
            LOG.error("balance creds_lookup failed for user_id=%s: %s", user_id, e)

    k = os.getenv(ENV_KEY, "") or ""
    s = os.getenv(ENV_SECRET, "") or ""
    b = os.getenv(ENV_BASE_URL, "") or DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL
    return (k, s, b)


def _session_for(api_key: str, base_url: str) -> requests.Session:
    key = (api_key, base_url)
    sess = _sessions.get(key)
    if sess is None:
        sess = requests.Session()
        if api_key:
            sess.headers.update({"X-MBX-APIKEY": api_key})
        _sessions[key] = sess
    return sess


def _sign(secret: str, params: Dict[str, Any]) -> Dict[str, Any]:
    query = urlencode(params)
    signature = hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
    params["signature"] = signature
    return params


def _get(user_id: str, path: str, params: Dict[str, Any], auth: bool, timeout: int = 15) -> requests.Response:
    api_key, api_secret, base_url = _resolve_creds(user_id)
    url = f"{base_url}{path}"
    if auth:
        now = int(time.time() * 1000)
        params.setdefault("timestamp", now)
        params.setdefault("recvWindow", RECV_WINDOW_MS)
        params = _sign(api_secret, params)
    sess = _session_for(api_key, base_url)
    return sess.get(url, params=params, timeout=timeout)


def get_balances_raw(user_id: str) -> List[Dict[str, Any]]:
    """
    GET /fapi/v3/balance  (USDS-M futures)
    Returns list of {asset, availableBalance, balance, ...} dicts.
    """
    try:
        resp = _get(user_id, "/fapi/v3/balance", {}, auth=True)
        if resp.status_code != 200:
            LOG.error("balance HTTP %s: %s", resp.status_code, resp.text[:500])
            return []
        data = resp.json()
        if isinstance(data, list):
            return data
        LOG.error("Unexpected balance payload type: %s body=%s", type(data), resp.text[:500])
        return []
    except Exception as e:
        LOG.error("get_balances_raw error for user %s: %s", user_id or "<none>", e)
        return []


def get_free_balance(user_id: str, asset: str = "USDT") -> Decimal:
    """Return available balance for asset as Decimal; Decimal('0') on error."""
    try:
        rows = get_balances_raw(user_id)
        want = asset.upper()
        for row in rows:
            if str(row.get("asset", "")).upper() == want:
                free = row.get("availableBalance") or row.get("balance") or "0"
                return Decimal(str(free))
        return Decimal("0")
    except Exception:
        return Decimal("0")
