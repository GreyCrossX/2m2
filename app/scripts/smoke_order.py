"""Simple smoke test for placing/canceling Binance futures orders."""
from __future__ import annotations

import asyncio
import os
from decimal import Decimal

from app.services.infrastructure.binance import BinanceClient, BinanceTrading


async def main() -> None:
    key = os.getenv("BINANCE_TESTNET_KEY") or os.getenv("BINANCE_API_KEY")
    secret = os.getenv("BINANCE_TESTNET_SECRET") or os.getenv("BINANCE_API_SECRET")
    if not key or not secret:
        raise RuntimeError("BINANCE_TESTNET_KEY/SECRET required for smoke test")

    symbol = os.getenv("SMOKE_SYMBOL", "BTCUSDT").upper()
    qty = Decimal(os.getenv("SMOKE_QTY", "0.002"))
    price = Decimal(os.getenv("SMOKE_PRICE", "10000"))

    client = BinanceClient(api_key=key, api_secret=secret, testnet=True, timeout_ms=30_000)
    trading = BinanceTrading(client)

    q_qty, q_price = await trading.quantize_limit_order(symbol, qty, price)
    print(f"Quantized quantity={q_qty} price={q_price}")

    order = await trading.create_limit_order(symbol, "BUY", q_qty, q_price, time_in_force="GTC")
    order_id = order.get("orderId")
    print(f"Order placed: {order}")

    if order_id:
        status = await trading.get_order_status(symbol, int(order_id))
        print(f"Order status: {status}")
        cancel = await trading.cancel_order(symbol, int(order_id))
        print(f"Cancel response: {cancel}")


if __name__ == "__main__":
    asyncio.run(main())
