# app/services/tasks/filters_source.py
from __future__ import annotations

from typing import Any, Dict, Optional

from app.services.ingestor.redis_io import r

# Keep the key format local to this module to avoid surprises elsewhere.
def _key(sym: str) -> str:
    return f"filters:{sym.upper()}"

def _d(x: Any) -> str:
    """Return a plain str whether x is bytes or already a str (or None)."""
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8", errors="replace")
    if x is None:
        return ""
    return str(x)

def _decode_hash(h: Dict[Any, Any]) -> Dict[str, str]:
    """Works for Dict[bytes, bytes] and Dict[str, str]."""
    return {_d(k): _d(v) for k, v in h.items()}

def _encode_hash(d: Dict[str, Any]) -> Dict[str, str]:
    """Persist everything as strings in Redis."""
    out: Dict[str, str] = {}
    for k, v in d.items():
        if v is None:
            continue
        out[str(k)] = str(v)
    return out

# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────

def set_symbol_filters(sym: str, filters: Dict[str, Any]) -> None:
    """
    Save exchange filters (tickSize, stepSize, etc.) for a symbol.
    Values are stored as strings for consistency.
    """
    r.hset(_key(sym), mapping=_encode_hash(filters))

def get_symbol_filters(sym: str) -> Optional[Dict[str, str]]:
    """
    Load exchange filters for a symbol. Returns a dict of strings or None.
    """
    raw = r.hgetall(_key(sym))
    if not raw:
        return None
    return _decode_hash(raw)
