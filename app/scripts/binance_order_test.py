"""
CLI helper to hit Binance Futures /fapi/v1/order/test using our validated client.

Usage (env-driven):
  BINANCE_API_KEY=... BINANCE_API_SECRET=... BINANCE_TESTNET=true \\
  python app/scripts/binance_order_test.py --symbol BTCUSDT --side BUY --type LIMIT --qty 0.001 --price 20000 --timeInForce GTC
"""

from __future__ import annotations

import argparse
import asyncio
import os
from decimal import Decimal

from app.services.infrastructure.binance.binance_client import BinanceClient


def _bool_env(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "y"}


async def main() -> None:
    parser = argparse.ArgumentParser(description="Binance /fapi/v1/order/test helper")
    parser.add_argument("--symbol", required=True, help="Symbol, e.g. BTCUSDT")
    parser.add_argument("--side", required=True, help="BUY or SELL")
    parser.add_argument(
        "--type",
        required=True,
        help="LIMIT, MARKET, STOP_MARKET, TAKE_PROFIT_MARKET, TAKE_PROFIT_LIMIT",
    )
    parser.add_argument(
        "--qty", required=True, type=Decimal, help="Quantity (base asset)"
    )
    parser.add_argument("--price", type=Decimal, help="Price (LIMIT/TP_LIMIT)")
    parser.add_argument("--stopPrice", type=Decimal, help="Stop price (STOP/TP)")
    parser.add_argument("--timeInForce", help="GTC/IOC/FOK/GTX")
    parser.add_argument("--positionSide", help="BOTH/LONG/SHORT (hedge mode)")
    parser.add_argument("--workingType", help="CONTRACT_PRICE/MARK_PRICE")
    parser.add_argument("--reduceOnly", action="store_true", help="Set reduceOnly=true")
    parser.add_argument(
        "--closePosition",
        action="store_true",
        help="Set closePosition=true (STOP/TP market only)",
    )
    parser.add_argument(
        "--recvWindow", type=int, default=5000, help="recvWindow ms (default 5000)"
    )
    args = parser.parse_args()

    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    if not api_key or not api_secret:
        raise SystemExit("BINANCE_API_KEY and BINANCE_API_SECRET are required")

    testnet = _bool_env("BINANCE_TESTNET", True)

    client = BinanceClient(
        api_key=api_key,
        api_secret=api_secret,
        testnet=testnet,
        recv_window_ms=args.recvWindow,
    )

    payload = {
        "symbol": args.symbol,
        "side": args.side,
        "type": args.type,
        "quantity": args.qty,
        "price": args.price,
        "stopPrice": args.stopPrice,
        "timeInForce": args.timeInForce,
        "positionSide": args.positionSide,
        "workingType": args.workingType,
        "reduceOnly": args.reduceOnly,
        "closePosition": args.closePosition,
    }

    result = await client.test_order(**payload)
    print("Test order accepted by Binance:")
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
