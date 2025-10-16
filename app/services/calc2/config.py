# services/calc2/config.py
from __future__ import annotations
import os
from dataclasses import dataclass, field
from typing import List

def _env_list(name: str, default: str) -> List[str]:
    raw = os.getenv(name, default)
    return [s.strip() for s in raw.split(",") if s.strip()]

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default

def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default

@dataclass(frozen=True)
class Config:
    redis_url: str = field(default_factory=lambda: os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    symbols: List[str] = field(default_factory=lambda: _env_list("SYMBOLS", "BTCUSDT,ETHUSDT"))
    timeframe: str = field(default_factory=lambda: os.getenv("TIMEFRAME", "2m"))

    stream_block_ms: int = field(default_factory=lambda: _env_int("STREAM_BLOCK_MS", 15000))
    stream_maxlen_ind: int = field(default_factory=lambda: _env_int("STREAM_MAXLEN_IND", 5000))
    stream_maxlen_signal: int = field(default_factory=lambda: _env_int("STREAM_MAXLEN_SIGNAL", 1000))

    tick_size: float = field(default_factory=lambda: _env_float("TICK_SIZE", 0.01))
    backoff_min_s: float = field(default_factory=lambda: _env_float("BACKOFF_MIN_S", 0.5))
    backoff_max_s: float = field(default_factory=lambda: _env_float("BACKOFF_MAX_S", 30.0))

    # Bootstrapping history depth for moving averages
    ma20_window: int = 20
    ma200_window: int = 200

    @classmethod
    def from_env(cls) -> "Config":
        # All fields already read from env via default_factories,
        # but we keep this to match the call site.
        return cls()
