from __future__ import annotations

import os

from app.services.infrastructure.binance import BinanceUSDS, BinanceUSDSConfig


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    lowered = value.strip().lower()
    if lowered in {"1", "true", "yes", "y"}:
        return True
    if lowered in {"0", "false", "no", "n"}:
        return False
    return default


def build_binance_usds() -> BinanceUSDS:
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    if not api_key or not api_secret:
        raise ValueError("BINANCE_API_KEY and BINANCE_API_SECRET must be set")

    timeout_env = os.getenv("BINANCE_TIMEOUT_MS", "5000")
    try:
        timeout_ms = int(timeout_env)
    except ValueError as exc:  # pragma: no cover - defensive
        raise ValueError("BINANCE_TIMEOUT_MS must be an integer") from exc

    config = BinanceUSDSConfig(
        api_key=api_key,
        api_secret=api_secret,
        testnet=_env_bool("BINANCE_TESTNET", False),
        timeout_ms=timeout_ms,
    )
    return BinanceUSDS(config)
