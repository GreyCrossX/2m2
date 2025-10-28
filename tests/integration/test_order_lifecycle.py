from __future__ import annotations

import os
import pytest

from app.infrastructure.exchange.binance_usds import BinanceUSDS, BinanceUSDSConfig
from app.services.worker.domain.exceptions import DomainExchangeDown


def _build_gateway() -> BinanceUSDS:
    if os.getenv("BINANCE_TESTNET", "").lower() != "true":
        pytest.skip("BINANCE_TESTNET=true not set; skipping live integration test")

    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    if not api_key or not api_secret:
        pytest.skip("Binance API credentials not provided")

    timeout = int(os.getenv("BINANCE_TIMEOUT_MS", "5000"))
    cfg = BinanceUSDSConfig(
        api_key=api_key,
        api_secret=api_secret,
        testnet=True,
        timeout_ms=timeout,
    )
    return BinanceUSDS(cfg)


def test_position_information_returns_list() -> None:
    gateway = _build_gateway()
    try:
        all_positions = gateway.position_information()
        btc_only = gateway.position_information("BTCUSDT")
    except DomainExchangeDown as exc:
        pytest.skip(f"Exchange unavailable: {exc}")

    assert isinstance(all_positions, list)
    assert isinstance(btc_only, list)


def test_get_set_position_mode_roundtrip() -> None:
    gateway = _build_gateway()
    try:
        mode = gateway.get_position_mode()
        current = bool(mode.get("dualSidePosition"))
        gateway.set_position_mode(current)
    except DomainExchangeDown as exc:
        pytest.skip(f"Exchange unavailable: {exc}")


def test_order_lifecycle() -> None:
    gateway = _build_gateway()
    try:
        new_order = gateway.new_order(
            symbol="BTCUSDT",
            side="BUY",
            type="LIMIT",
            quantity="0.001",
            price="10000",
            timeInForce="GTC",
            reduceOnly=False,
        )
        order_id = new_order.get("orderId")
        assert order_id is not None
        queried = gateway.query_order(symbol="BTCUSDT", orderId=order_id)
        assert queried.get("symbol") == "BTCUSDT"
        cancel = gateway.cancel_order(symbol="BTCUSDT", orderId=order_id)
        assert cancel.get("status") in {"CANCELED", "PENDING_CANCEL", None}
    except DomainExchangeDown as exc:
        pytest.skip(f"Exchange unavailable: {exc}")
