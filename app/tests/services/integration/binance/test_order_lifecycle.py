from __future__ import annotations

import os
from decimal import Decimal

import pytest

from app.services.domain.exceptions import DomainExchangeDown
from app.services.infrastructure.binance import BinanceClient, BinanceTrading


@pytest.fixture(scope="module")
def testnet_client() -> BinanceClient:
    key = os.getenv("BINANCE_TESTNET_KEY") or os.getenv("BINANCE_API_KEY")
    secret = os.getenv("BINANCE_TESTNET_SECRET") or os.getenv("BINANCE_API_SECRET")
    if not key or not secret:
        pytest.skip("Binance testnet credentials not configured")
    return BinanceClient(api_key=key, api_secret=secret, testnet=True, timeout_ms=30_000)


@pytest.mark.integration()
@pytest.mark.asyncio()
async def test_order_lifecycle_limit_and_stop(testnet_client: BinanceClient) -> None:
    trading = BinanceTrading(testnet_client)
    symbol = "BTCUSDT"
    try:
        limit = await trading.create_limit_order(
            symbol,
            "BUY",
            Decimal("0.002"),
            Decimal("10000"),
            reduce_only=False,
            time_in_force="GTC",
        )
    except DomainExchangeDown as exc:
        pytest.skip(f"Exchange unavailable: {exc}")
        return

    order_id = limit.get("orderId")
    assert order_id is not None

    status = await trading.get_order_status(symbol, int(order_id))
    assert status in {"NEW", "PENDING_NEW", "FILLED", "PARTIALLY_FILLED"}

    await trading.cancel_order(symbol, int(order_id))

    try:
        stop = await trading.create_stop_market_order(
            symbol,
            "SELL",
            Decimal("0.002"),
            Decimal("9000"),
            working_type="MARK_PRICE",
        )
    except DomainExchangeDown as exc:
        pytest.skip(f"Exchange unavailable placing stop: {exc}")
        return

    stop_id = stop.get("orderId")
    if stop_id:
        await trading.cancel_order(symbol, int(stop_id))
