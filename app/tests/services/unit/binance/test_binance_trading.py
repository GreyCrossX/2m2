from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List

import pytest

pytestmark = pytest.mark.unit

from app.services.domain.exceptions import DomainBadRequest
from app.services.infrastructure.binance.binance_trading import BinanceTrading
from app.services.worker.domain.enums import OrderSide  # <-- updated

class _StubClient:
    def __init__(self) -> None:
        self.new_order_calls: List[Dict[str, Any]] = []
        self.cancel_calls: List[Dict[str, Any]] = []
        self.query_calls: List[Dict[str, Any]] = []
        self.close_calls: List[Dict[str, Any]] = []
        self.filters = {
            "BTCUSDT": {
                "MIN_NOTIONAL": {"notional": "5"},
                "LOT_SIZE": {"stepSize": "0.001", "minQty": "0.001"},
                "PRICE_FILTER": {"tickSize": "0.1", "minPrice": "0.1"},
            }
        }
        self.quantize_return = (Decimal("0.010"), Decimal("100.0"))

    async def get_symbol_filters(self, symbol: str) -> Dict[str, Dict[str, Any]]:
        return self.filters[symbol.upper()]

    async def quantize_order(
        self, symbol: str, quantity: Decimal, price: Decimal | None
    ) -> tuple[Decimal, Decimal | None]:
        return self.quantize_return

    async def new_order(self, **payload: Any) -> Dict[str, Any]:
        self.new_order_calls.append(payload)
        return {"orderId": 123, "payload": payload}

    async def cancel_order(self, **payload: Any) -> Dict[str, Any]:
        self.cancel_calls.append(payload)
        return payload

    async def query_order(self, **payload: Any) -> Dict[str, Any]:
        self.query_calls.append(payload)
        return {"status": "NEW"}

    async def close_position_market(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        self.close_calls.append({"args": args, "kwargs": kwargs})
        return {"closed": True}

    async def change_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        return {"symbol": symbol, "leverage": leverage}


@pytest.fixture()
def stub_client() -> _StubClient:
    return _StubClient()


@pytest.fixture()
def trading(stub_client: _StubClient) -> BinanceTrading:
    return BinanceTrading(stub_client)


@pytest.mark.asyncio()
async def test_create_limit_order_sends_payload(trading: BinanceTrading, stub_client: _StubClient) -> None:
    stub_client.quantize_return = (Decimal("0.05"), Decimal("100.0"))
    result = await trading.create_limit_order(
        "btcusdt",
        OrderSide.LONG,  # <-- updated
        Decimal("0.05"),
        Decimal("100.0"),
        reduce_only=True,
        time_in_force="GTC",
        new_client_order_id="abc",
    )
    assert result["orderId"] == 123
    assert stub_client.new_order_calls[0]["reduceOnly"] is True
    assert stub_client.new_order_calls[0]["timeInForce"] == "GTC"
    assert stub_client.new_order_calls[0]["newClientOrderId"] == "abc"


@pytest.mark.asyncio()
async def test_create_limit_order_min_notional(trading: BinanceTrading, stub_client: _StubClient) -> None:
    stub_client.quantize_return = (Decimal("0.001"), Decimal("1.0"))
    with pytest.raises(DomainBadRequest):
        await trading.create_limit_order("btcusdt", OrderSide.LONG, Decimal("0.001"), Decimal("1"))


@pytest.mark.asyncio()
async def test_create_limit_order_invalid_time_in_force(trading: BinanceTrading) -> None:
    with pytest.raises(DomainBadRequest):
        await trading.create_limit_order("btcusdt", OrderSide.SHORT, Decimal("1"), Decimal("10"), time_in_force="foo")


@pytest.mark.asyncio()
async def test_create_stop_market_requires_supported_type(trading: BinanceTrading) -> None:
    with pytest.raises(DomainBadRequest):
        await trading.create_stop_market_order(
            "btcusdt",
            OrderSide.SHORT,
            Decimal("0.01"),
            Decimal("90"),
            order_type="LIMIT",
        )


@pytest.mark.asyncio()
async def test_create_stop_market_calls_client(trading: BinanceTrading, stub_client: _StubClient) -> None:
    stub_client.quantize_return = (Decimal("0.01"), Decimal("95"))
    await trading.create_stop_market_order(
        "btcusdt",
        OrderSide.SHORT,
        Decimal("0.02"),
        Decimal("95"),
        working_type="MARK_PRICE",
        position_side="SHORT",
    )
    payload = stub_client.new_order_calls[-1]
    assert payload["type"] == "STOP_MARKET"
    assert payload["positionSide"] == "SHORT"
    assert payload["workingType"] == "MARK_PRICE"


@pytest.mark.asyncio()
async def test_create_stop_market_take_profit(trading: BinanceTrading, stub_client: _StubClient) -> None:
    stub_client.quantize_return = (Decimal("0.01"), Decimal("105"))
    await trading.create_stop_market_order(
        "btcusdt",
        OrderSide.LONG,
        Decimal("0.01"),
        Decimal("105"),
        order_type="TAKE_PROFIT_MARKET",
    )
    payload = stub_client.new_order_calls[-1]
    assert payload["type"] == "TAKE_PROFIT_MARKET"


@pytest.mark.asyncio()
async def test_create_limit_order_accepts_string_side(trading: BinanceTrading, stub_client: _StubClient) -> None:
    stub_client.quantize_return = (Decimal("0.05"), Decimal("110"))
    await trading.create_limit_order("btcusdt", "sell", Decimal("0.05"), Decimal("110"))
    payload = stub_client.new_order_calls[-1]
    assert payload["side"] == "SELL"


@pytest.mark.asyncio()
async def test_cancel_and_query_delegates(trading: BinanceTrading, stub_client: _StubClient) -> None:
    await trading.cancel_order("btcusdt", 12)
    await trading.get_order_status("btcusdt", 12)
    assert stub_client.cancel_calls[0]["symbol"] == "BTCUSDT"
    assert stub_client.query_calls[0]["orderId"] == 12


@pytest.mark.asyncio()
async def test_close_position_market_validates_position_side(trading: BinanceTrading, stub_client: _StubClient) -> None:
    await trading.close_position_market("btcusdt", OrderSide.SHORT, Decimal("0.01"), position_side="long")
    call = stub_client.close_calls[0]
    assert call["kwargs"]["position_side"] == "LONG"


@pytest.mark.asyncio()
async def test_close_position_market_invalid_side(trading: BinanceTrading) -> None:
    with pytest.raises(DomainBadRequest):
        await trading.close_position_market("btcusdt", "BAD", Decimal("1"))
