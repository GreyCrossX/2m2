"""
Redis Keys & Conventions (single source of truth)

NOTE: Calc uses the hash-tag helper in services.ingestor.keys.tag(sym, tf).
We import it only for the convenience helpers that accept (sym, tf).
"""

from __future__ import annotations

from services.ingestor.keys import tag as tag_tf  # existing helper


# ── Bot-scoped hashes/sets ────────────────────────────────────────────────────

def key_bot_cfg(bot_id: str) -> str:
    return f"bot:{bot_id}:cfg"


def key_bot_state(bot_id: str) -> str:
    return f"bot:{bot_id}:state"


def key_bot_signals(bot_id: str) -> str:
    """Idempotency ledger for processed signals."""
    return f"bot:{bot_id}:signals"


def key_open_orders(bot_id: str) -> str:
    """Set of orderIds (or client IDs) currently tracked as open for the bot."""
    return f"bot:{bot_id}:open_orders"


# ── Symbol indexes for fast fan-out (calc → bots) ─────────────────────────────

def key_symbol_index(sym: str) -> str:
    """Primary: set of bot_ids active on a symbol."""
    return f"idx:bots|{{{sym.upper()}}}"


def key_symbol_tf_index(sym: str, tf: str) -> str:
    """Optional per-timeframe index (not required by poller)."""
    return f"idx:bots|{{{sym.upper()}}}:{tf}"


# Back-compat alias used by older code
def key_bots_index(sym: str) -> str:
    """Alias kept for older imports; equals key_symbol_index."""
    return key_symbol_index(sym)


# ── Calc streams (poller supplies precomputed hash tag) ───────────────────────

def stream_signal_by_tag(hash_tag: str) -> str:
    """Calc high-level signal stream (ARM/DISARM), using a precomputed hash tag."""
    return f"stream:signal|{{{hash_tag}}}"


def stream_indicator_by_tag(hash_tag: str) -> str:
    """Indicator events stream, using a precomputed hash tag."""
    return f"stream:ind|{{{hash_tag}}}"


def stream_signal(sym: str, tf: str = "2m") -> str:
    """Convenience helper if you have (sym, tf) instead of a precomputed tag."""
    return f"stream:signal|{{{tag_tf(sym, tf)}}}"
