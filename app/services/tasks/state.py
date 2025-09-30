"""
Redis State I/O (thin adapters only; no business logic)

- read/write BotConfig and BotState as Redis hashes
- idempotency (processed signals) as a Redis set
- open order tracking as a Redis set
- symbol index for fan-out

All Decimal fields are serialized to strings in Redis and parsed back on read.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, Iterable, Optional, Set, Tuple, List

from app.services.ingestor.redis_io import r
from .contracts import BotConfig, BotState
from .keys import (
    key_bot_cfg,
    key_bot_state,
    key_bot_signals,
    key_open_orders,
    key_symbol_index,
)


# ──────────────────────────────────────────────────────────────────────────────
# Helpers: (de)serialization for hashes
# ──────────────────────────────────────────────────────────────────────────────

def _d(x: Any) -> str:
    """Return a plain str whether x is bytes or already a str."""
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8", errors="replace")
    return "" if x is None else str(x)


def _decode_hash(h: Dict[Any, Any]) -> Dict[str, str]:
    """Works whether h is Dict[bytes, bytes] or Dict[str, str]."""
    return {_d(k): _d(v) for k, v in h.items()}


def _decode_set(s: Iterable[Any]) -> Set[str]:
    """For SMEMBERS / SISMEMBER etc."""
    return {_d(v) for v in s}


def _encode_hash(d: Dict[str, Any]) -> Dict[str, str]:
    """
    Serialize values to strings for Redis storage.
    Decimals are stringified to preserve precision.
    """
    out: Dict[str, str] = {}
    for k, v in d.items():
        if v is None:
            continue
        if isinstance(v, Decimal):
            out[k] = str(v)
        else:
            out[k] = str(v)
    return out


def _to_bot_config(d: Dict[str, str]) -> BotConfig:
    """
    Convert a Redis hash (decoded to str->str) into a BotConfig.
    Only known keys are mapped; extras are ignored.
    """
    cfg: BotConfig = {
        "bot_id": d.get("bot_id", ""),
        "user_id": d.get("user_id", ""),
        "sym": d.get("sym", ""),
        "side_mode": d.get("side_mode", "both"),   # type: ignore[assignment]
        "status": d.get("status", "active"),       # type: ignore[assignment]
    }
    if "risk_per_trade" in d:
        try:
            cfg["risk_per_trade"] = Decimal(d["risk_per_trade"])
        except Exception:
            pass
    if "leverage" in d:
        try:
            cfg["leverage"] = Decimal(d["leverage"])
        except Exception:
            pass
    if "tp_ratio" in d:
        try:
            cfg["tp_ratio"] = Decimal(d["tp_ratio"])
        except Exception:
            pass
    if "max_qty" in d:
        try:
            cfg["max_qty"] = Decimal(d["max_qty"])
        except Exception:
            pass
    return cfg


def _to_bot_state(d: Dict[str, str]) -> BotState:
    """
    Convert a Redis hash (decoded to str->str) into a BotState.
    Only known keys are mapped; extras are ignored.
    """
    st: BotState = {}
    if "last_signal_id" in d:
        st["last_signal_id"] = d["last_signal_id"] or None
    if "armed_entry_order_id" in d:
        st["armed_entry_order_id"] = d["armed_entry_order_id"] or None
    if "bracket_ids" in d:
        st["bracket_ids"] = d["bracket_ids"] or None
    if "position_side" in d:
        st["position_side"] = d["position_side"] or None  # type: ignore[assignment]
    if "position_qty" in d:
        try:
            st["position_qty"] = Decimal(d["position_qty"])
        except Exception:
            st["position_qty"] = None
    if "avg_entry_price" in d:
        try:
            st["avg_entry_price"] = Decimal(d["avg_entry_price"])
        except Exception:
            st["avg_entry_price"] = None
    return st


# ──────────────────────────────────────────────────────────────────────────────
# Config CRUD
# ──────────────────────────────────────────────────────────────────────────────

def read_bot_config(bot_id: str) -> Optional[BotConfig]:
    key = key_bot_cfg(bot_id)
    raw = r.hgetall(key)
    if not raw:
        return None
    decoded = _decode_hash(raw)
    return _to_bot_config(decoded)


def write_bot_config(bot_id: str, cfg: BotConfig) -> None:
    key = key_bot_cfg(bot_id)
    r.hset(key, mapping=_encode_hash(cfg))  # type: ignore[arg-type]


# ──────────────────────────────────────────────────────────────────────────────
# State CRUD
# ──────────────────────────────────────────────────────────────────────────────

def read_bot_state(bot_id: str) -> BotState:
    key = key_bot_state(bot_id)
    h = r.hgetall(key)
    if not h:
        return {}
    raw = _decode_hash(h)
    return _to_bot_state(raw)


def write_bot_state(bot_id: str, st: BotState) -> None:
    key = key_bot_state(bot_id)
    r.hset(key, mapping=_encode_hash(st))  # type: ignore[arg-type]


# ──────────────────────────────────────────────────────────────────────────────
# Idempotency (processed signals)
# ──────────────────────────────────────────────────────────────────────────────

def mark_signal_processed(bot_id: str, signal_id: str) -> int:
    return r.sadd(key_bot_signals(bot_id), signal_id)


def is_signal_processed(bot_id: str, signal_id: str) -> bool:
    return bool(r.sismember(key_bot_signals(bot_id), signal_id))


def list_processed_signals(bot_id: str) -> Set[str]:
    """Get all processed signal IDs for a bot."""
    members = r.smembers(key_bot_signals(bot_id))
    return _decode_set(members)


# ──────────────────────────────────────────────────────────────────────────────
# Open order tracking
# ──────────────────────────────────────────────────────────────────────────────

def track_open_order(bot_id: str, order_id: str) -> int:
    return r.sadd(key_open_orders(bot_id), order_id)


def untrack_open_order(bot_id: str, order_id: str) -> int:
    return r.srem(key_open_orders(bot_id), order_id)


def list_tracked_orders(bot_id: str) -> List[str]:
    members = r.smembers(key_open_orders(bot_id))
    return [_d(m) for m in members]


# Back-compat aliases
def add_open_order(bot_id: str, order_id: str) -> int:
    return track_open_order(bot_id, order_id)


def list_open_orders(bot_id: str) -> List[str]:
    return list_tracked_orders(bot_id)


# ──────────────────────────────────────────────────────────────────────────────
# Symbol index (fan-out)
# ──────────────────────────────────────────────────────────────────────────────

def index_bot(symbol: str, bot_id: str) -> int:
    return r.sadd(key_symbol_index(symbol), bot_id)


def deindex_bot(symbol: str, bot_id: str) -> int:
    return r.srem(key_symbol_index(symbol), bot_id)


def bots_for_symbol(sym: str) -> Set[str]:
    members = r.smembers(key_symbol_index(sym)) or set()
    return _decode_set(members)
