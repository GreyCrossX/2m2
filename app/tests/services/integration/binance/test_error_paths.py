from __future__ import annotations

import os
from decimal import Decimal

import pytest

from app.services.domain.exceptions import DomainBadRequest, DomainExchangeDown
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
async def test_rejects_under_min_notional(testnet_client: BinanceClient) -> None:
    trading = BinanceTrading(testnet_client)
    try:
        with pytest.raises(DomainBadRequest):
            await trading.create_limit_order(
                "BTCUSDT",
                "BUY",
                Decimal("0.00001"),
                Decimal("100"),
            )
    except DomainExchangeDown as exc:
        pytest.skip(f"Exchange unavailable: {exc}")
