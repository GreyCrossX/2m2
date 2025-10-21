from __future__ import annotations

from typing import Iterable, List, Tuple

# -----------------------------------------------------------------------------
# Redis key helpers for Calc → Worker streams
#
#   stream:signal|{SYMBOL:TF}
#   stream:ind|{SYMBOL:TF}          (optional; for indicators if you ever need)
#   worker:offset:signal|{SYMBOL:TF}
#
# Notes:
# - SYMBOL is always uppercased when building keys.
# - TF is passed through (e.g., "2m").
# - Curly braces {...} ensure Redis Cluster hash-tagging keeps related keys on
#   the same hash slot if you ever shard.
# -----------------------------------------------------------------------------

_SIGNAL_PREFIX = "stream:signal|"
_IND_PREFIX = "stream:ind|"
_OFFSET_PREFIX = "worker:offset:signal|"


def stream_signal(sym: str, tf: str) -> str:
    """
    Calc → Worker signal stream key.

    Example:
        >>> stream_signal("btcusdt", "2m")
        'stream:signal|{BTCUSDT:2m}'
    """
    return f"{_SIGNAL_PREFIX}{{{sym.upper()}:{tf}}}"


def stream_indicator(sym: str, tf: str) -> str:
    """
    Calc indicator stream key (optional, not consumed by the Worker right now).

    Example:
        >>> stream_indicator("ETHUSDT", "2m")
        'stream:ind|{ETHUSDT:2m}'
    """
    return f"{_IND_PREFIX}{{{sym.upper()}:{tf}}}"


def offset_key(sym: str, tf: str) -> str:
    """
    Per-stream last delivered ID for XREAD mode (used to resume after restarts).

    Example:
        >>> offset_key("BTCUSDT", "2m")
        'worker:offset:signal|{BTCUSDT:2m}'
    """
    return f"{_OFFSET_PREFIX}{{{sym.upper()}:{tf}}}"


def parse_stream_key(stream_key: str) -> Tuple[str, str, str]:
    """
    Parse a stream key into (kind, symbol, timeframe).

    Returns:
        kind: "signal" | "ind" | "unknown"
        symbol: uppercased symbol or "" if not parseable
        timeframe: original timeframe string or ""

    Example:
        >>> parse_stream_key('stream:signal|{BTCUSDT:2m}')
        ('signal', 'BTCUSDT', '2m')
    """
    try:
        if stream_key.startswith(_SIGNAL_PREFIX):
            kind = "signal"
        elif stream_key.startswith(_IND_PREFIX):
            kind = "ind"
        else:
            kind = "unknown"

        inner = stream_key.split("{", 1)[1].split("}", 1)[0]
        sym, tf = inner.split(":", 1)
        return kind, sym.upper(), tf
    except Exception:
        return "unknown", "", ""


def build_signal_keys(symbols: Iterable[str], timeframe: str) -> List[str]:
    """
    Build all signal stream keys for a list of symbols and a single timeframe.

    Example:
        >>> build_signal_keys(["BTCUSDT", "ethusdt"], "2m")
        ['stream:signal|{BTCUSDT:2m}', 'stream:signal|{ETHUSDT:2m}']
    """
    return [stream_signal(s, timeframe) for s in symbols]


__all__ = [
    "stream_signal",
    "stream_indicator",
    "offset_key",
    "parse_stream_key",
    "build_signal_keys",
]
