# app/services/tasks/balance_source.py
from __future__ import annotations

import os
import time
import hmac
import json
import hashlib
import logging
from decimal import Decimal
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import requests

LOG = logging.getLogger("balance_source")

# ──────────────────────────────────────────────────────────────────────────────
# Env + helpers
# ──────────────────────────────────────────────────────────────────────────────

def _s(x: Optional[str]) -> str:
    """Sanitize env strings - more conservative approach."""
    if x is None:
        return ""
    x = x.strip()
    # Only remove quotes if they wrap the entire string and are matching
    if len(x) >= 2 and x[0] == x[-1] and x[0] in ("'", '"'):
        return x[1:-1].strip()
    return x

def _validate_api_credentials() -> tuple[str, str, bool]:
    """Validate API credentials and return (key, secret, is_valid)."""
    api_key = _s(os.getenv("TESTNET_API_KEY") or os.getenv("API_KEY"))
    api_secret = _s(os.getenv("TESTNET_API_SECRET") or os.getenv("API_SECRET"))
    
    # Basic validation - Binance API keys should be 64 chars, secrets should be 64 chars
    is_valid = (
        len(api_key) >= 32 and  # Minimum reasonable length
        len(api_secret) >= 32 and
        api_key.replace('-', '').replace('_', '').isalnum() and  # Should be alphanumeric (with possible dashes/underscores)
        api_secret.replace('-', '').replace('_', '').isalnum()
    )
    
    if not is_valid:
        LOG.error(
            "Invalid API credentials format. Key length: %d, Secret length: %d. "
            "Expected: both should be at least 32 chars and alphanumeric",
            len(api_key), len(api_secret)
        )
    
    return api_key, api_secret, is_valid

API_KEY, API_SECRET, _CREDS_VALID = _validate_api_credentials()
BASE_URL = _s(os.getenv("BASE_PATH", "https://testnet.binancefuture.com"))

# Optional: bump if clocks drift
RECV_WINDOW_MS = int(os.getenv("BINANCE_RECV_WINDOW_MS", "60000"))

_session: Optional[requests.Session] = None

def _session_get() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        if API_KEY:  # Only set header if we have a key
            _session.headers.update({"X-MBX-APIKEY": API_KEY})
    return _session

def _sign(params: Dict[str, Any]) -> Dict[str, Any]:
    query = urlencode(params)
    signature = hmac.new(API_SECRET.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
    params["signature"] = signature
    return params

def _get(path: str, params: Dict[str, Any], auth: bool = False, timeout: int = 15) -> requests.Response:
    url = f"{BASE_URL}{path}"
    if auth:
        now = int(time.time() * 1000)
        params.setdefault("timestamp", now)
        params.setdefault("recvWindow", RECV_WINDOW_MS)
        params = _sign(params)
    return _session_get().get(url, params=params, timeout=timeout)

# ──────────────────────────────────────────────────────────────────────────────
# Public API used by handlers / domain
# ──────────────────────────────────────────────────────────────────────────────

def get_balances_raw(user_id: str) -> List[Dict[str, Any]]:
    """
    GET /fapi/v3/balance  (USDS-M futures)
    Returns list of {asset, availableBalance, balance, ...} dicts.
    """
    if not _CREDS_VALID:
        LOG.error("Cannot fetch balances - invalid API credentials for user %s", user_id or "<none>")
        return []

    try:
        # Ping /fapi/v1/time once in case BASE_URL is wrong; non-fatal if it fails
        try:
            _get("/fapi/v1/time", {}, auth=False, timeout=5)
        except Exception as e:
            LOG.debug("time ping failed (non-fatal): %s", e)

        resp = _get("/fapi/v3/balance", {}, auth=True)
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
    if not _CREDS_VALID:
        LOG.warning("Skipping balance fetch - invalid API credentials")
        return Decimal("0")
        
    rows = get_balances_raw(user_id)
    want = asset.upper()
    for row in rows:
        try:
            if str(row.get("asset", "")).upper() == want:
                free = row.get("availableBalance") or row.get("balance") or "0"
                return Decimal(str(free))
        except Exception:
            continue
    return Decimal("0")