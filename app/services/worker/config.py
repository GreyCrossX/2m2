from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default)


def _env_first(*names: str, default: str = "") -> str:
    for n in names:
        val = os.getenv(n)
        if val:
            return val
    return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(_env(name, str(default)))
    except Exception:
        return default


def _env_bool(name: str, default: bool) -> bool:
    v = _env(name, "").lower()
    if v in {"1", "true", "yes", "y"}:
        return True
    if v in {"0", "false", "no", "n"}:
        return False
    return default


def _normalize_async_dsn(dsn: str) -> str:
    """Ensure SQLAlchemy DSN uses asyncpg driver."""
    if not dsn:
        return dsn
    if "://" not in dsn:
        return dsn
    scheme, rest = dsn.split("://", 1)
    if "+asyncpg" in scheme:
        return dsn
    if scheme in {"postgres", "postgresql"}:
        return f"postgresql+asyncpg://{rest}"
    return dsn


@dataclass(frozen=True)
class Config:
    # Core
    service_name: str
    log_level: str

    # Redis / Postgres
    redis_url: str
    postgres_dsn: str

    # Symbols / timeframe
    symbols: List[str]
    timeframe: str

    # Stream behavior
    stream_block_ms: int
    catchup_threshold_ms: int
    router_refresh_seconds: int
    order_monitor_interval_seconds: int

    # Balance cache
    balance_ttl_seconds: int

    # Binance
    binance_connector: str  # "modular" (default)
    # (we derive testnet/prod from bot/env + creds)

    # Execution mode
    dry_run_mode: bool

    @classmethod
    def from_env(cls) -> "Config":
        syms = [
            s.strip().upper()
            for s in _env("WORKER_SYMBOLS", "BTCUSDT").split(",")
            if s.strip()
        ]

        raw_dsn = _env_first("POSTGRES_DSN", "DATABASE_URL", "DB_URL")
        dsn = _normalize_async_dsn(
            raw_dsn or "postgresql+asyncpg://user:pass@localhost:5432/app"
        )

        return cls(
            service_name=_env("SERVICE_NAME", "worker"),
            log_level=_env("LOG_LEVEL", "INFO"),
            redis_url=_env("REDIS_URL", "redis://localhost:6379/0"),
            postgres_dsn=dsn,
            symbols=syms,
            timeframe=_env("TIMEFRAME", "2m"),
            stream_block_ms=_env_int("STREAM_BLOCK_MS", 15000),
            catchup_threshold_ms=_env_int("CATCHUP_THRESHOLD_MS", 15000),
            router_refresh_seconds=_env_int("ROUTER_REFRESH_SECONDS", 60),
            order_monitor_interval_seconds=_env_int(
                "ORDER_MONITOR_INTERVAL_SECONDS", 3
            ),
            balance_ttl_seconds=_env_int("BALANCE_TTL_SECONDS", 30),
            binance_connector=_env("BINANCE_CONNECTOR", "modular"),
            dry_run_mode=_env_bool("DRY_RUN_MODE", False),
        )
