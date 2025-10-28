from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any, Dict, List

import pytest

pytestmark = pytest.mark.unit

from app.services.domain.exceptions import DomainBadRequest
from app.services.infrastructure.binance.binance_client import BinanceClient


class _StubGateway:
    def __init__(self) -> None:
        self.exchange_calls = 0
        self.new_order_calls: List[Dict[str, Any]] = []
        self.query_calls: List[Dict[str, Any]] = []
        self.cancel_calls: List[Dict[str, Any]] = []
        self.change_mode_calls: List[bool] = []
        self.leverage_calls: List[Dict[str, Any]] = []

    def exchange_information(self) -> dict:
        self.exchange_calls += 1
        return {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.1", "minPrice": "0.1"},
                        {
                            "filterType": "LOT_SIZE",
                            "stepSize": "0.001",
                            "minQty": "0.001",
                            "maxQty": "100",
                        },
                        {"filterType": "MIN_NOTIONAL", "notional": "5"},
                    ],
                }
            ]
        }

    def account_information(self) -> dict:
        return {"foo": "bar"}

    def account_balance(self) -> list[dict]:
        return [{"asset": "USDT", "balance": "1"}]

    def position_information(self, symbol: str | None = None) -> list[dict]:
        return [{"symbol": symbol or "BTCUSDT"}]

    def get_position_mode(self) -> dict:
        return {"dualSidePosition": False}

    def set_position_mode(self, dual_side: bool) -> dict:
        self.change_mode_calls.append(dual_side)
        return {"dualSidePosition": dual_side}

    def change_leverage(self, symbol: str, leverage: int) -> dict:
        self.leverage_calls.append({"symbol": symbol, "leverage": leverage})
        return {"leverage": leverage}

    def new_order(self, **payload: Any) -> dict:
        self.new_order_calls.append(payload)
        return {"status": "NEW"}

    def query_order(self, **payload: Any) -> dict:
        self.query_calls.append(payload)
        return {"status": "FILLED"}

    def cancel_order(self, **payload: Any) -> dict:
        self.cancel_calls.append(payload)
        return {"status": "CANCELED"}


@pytest.fixture()
def client() -> BinanceClient:
    gateway = _StubGateway()
    return BinanceClient("k", "s", gateway=gateway)  # type: ignore[arg-type]


@pytest.mark.asyncio()
async def test_get_symbol_filters_caches(client: BinanceClient) -> None:
    first, second = await asyncio.gather(
        client.get_symbol_filters("btcusdt"),
        client.get_symbol_filters("BTCUSDT"),
    )
    assert first is second
    assert client._exchange_info is not None  # type: ignore[attr-defined]
    assert client._gateway.exchange_calls == 1  # type: ignore[attr-defined]


@pytest.mark.asyncio()
async def test_new_order_payload_conversion(client: BinanceClient) -> None:
    await client.new_order(symbol="btcusdt", quantity=Decimal("0.01"), price=Decimal("100"))
    payload = client._gateway.new_order_calls[0]  # type: ignore[attr-defined]
    assert payload["symbol"] == "BTCUSDT"
    assert payload["quantity"] == "0.01"
    assert payload["price"] == "100"


@pytest.mark.asyncio()
async def test_close_position_market_validates_qty(client: BinanceClient) -> None:
    with pytest.raises(DomainBadRequest):
        await client.close_position_market("btcusdt", "SELL", Decimal("0.0001"))


@pytest.mark.asyncio()
async def test_close_position_market_calls_new_order(client: BinanceClient) -> None:
    await client.close_position_market("btcusdt", "BUY", Decimal("0.01"), position_side="short")
    payload = client._gateway.new_order_calls[-1]  # type: ignore[attr-defined]
    assert payload["reduceOnly"] is True
    assert payload["positionSide"] == "SHORT"


@pytest.mark.asyncio()
async def test_change_leverage_delegates(client: BinanceClient) -> None:
    await client.change_leverage("btcusdt", 20)
    calls = client._gateway.leverage_calls  # type: ignore[attr-defined]
    assert calls[0] == {"symbol": "btcusdt", "leverage": 20}


@pytest.mark.asyncio()
async def test_set_position_mode_delegates(client: BinanceClient) -> None:
    await client.set_position_mode(True)
    assert client._gateway.change_mode_calls == [True]  # type: ignore[attr-defined]


@pytest.mark.asyncio()
async def test_query_and_cancel_uppercase(client: BinanceClient) -> None:
    await client.query_order(symbol="btcusdt", orderId=11)
    await client.cancel_order(symbol="btcusdt", orderId=11)
    assert client._gateway.query_calls[0]["symbol"] == "BTCUSDT"  # type: ignore[attr-defined]
    assert client._gateway.cancel_calls[0]["symbol"] == "BTCUSDT"  # type: ignore[attr-defined]


@pytest.mark.asyncio()
async def test_balance_and_account(client: BinanceClient) -> None:
    balance = await client.balance()
    account = await client.account()
    assert balance[0]["asset"] == "USDT"
    assert account["foo"] == "bar"


@pytest.mark.asyncio()
async def test_position_information_passes_symbol(client: BinanceClient) -> None:
    positions = await client.position_information("btcusdt")
    assert positions[0]["symbol"] == "btcusdt"
