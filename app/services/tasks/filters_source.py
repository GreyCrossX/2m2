from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, Optional

from app.services.ingestor.redis_io import r


def _key(sym: str) -> str:
    return f"filters:{sym.upper()}"


def _d(x: Any) -> str:
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8", errors="replace")
    if x is None:
        return ""
    return str(x)


def _decode_hash(h: Dict[Any, Any]) -> Dict[str, str]:
    return {_d(k): _d(v) for k, v in h.items()}


def _encode_hash(d: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in d.items():
        if v is None:
            continue
        out[str(k)] = str(v)
    return out


def _normalize_decimals(d: Dict[str, str]) -> Dict[str, Any]:
    """
    Convert known numeric fields to Decimal / int so downstream logic
    receives proper types (contracts.ExchangeFilters).
    """
    out: Dict[str, Any] = dict(d)
    if "tick_size" in out:
        out["tick_size"] = Decimal(out["tick_size"])
    if "step_size" in out:
        out["step_size"] = Decimal(out["step_size"])
    if "min_qty" in out:
        out["min_qty"] = Decimal(out["min_qty"])
    if "min_notional" in out:
        out["min_notional"] = Decimal(out["min_notional"])
    if "price_precision" in out:
        try:
            out["price_precision"] = int(out["price_precision"])
        except Exception:
            del out["price_precision"]
    if "quantity_precision" in out:
        try:
            out["quantity_precision"] = int(out["quantity_precision"])
        except Exception:
            del out["quantity_precision"]
    return out


def set_symbol_filters(sym: str, filters: Dict[str, Any]) -> None:
    """
    Save exchange filters (tick_size, step_size, etc.) for a symbol.
    Values are stored as strings for consistency.
    """
    r.hset(_key(sym), mapping=_encode_hash(filters))


def get_symbol_filters(sym: str) -> Optional[Dict[str, Any]]:
    """
    Load exchange filters for a symbol.
    Returns a dict with Decimal/int for known fields; None if absent.
    """
    raw = r.hgetall(_key(sym))
    if not raw:
        return None
    decoded = _decode_hash(raw)
    return _normalize_decimals(decoded)
