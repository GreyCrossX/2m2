from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import pytest

from app.services.worker.core.worker import BotWorker
from app.services.worker.domain.enums import OrderSide, OrderStatus, SideWhitelist
from app.services.worker.domain.models import ArmSignal, BotConfig, OrderState


class OrderExecutorStub:
    def __init__(self, state: OrderState) -> None:
        self._state = state

    async def execute_order(self, bot: BotConfig, signal: ArmSignal) -> OrderState:
        return self._state


class PositionManagerStub:
    def __init__(self, take_profit: Decimal) -> None:
        self.calls: list[tuple] = []
        self._tp = take_profit

    async def open_position(self, bot_id, order_state):
        self.calls.append((bot_id, order_state))
        return SimpleNamespace(
            take_profit=self._tp,
            quantity=order_state.quantity,
            symbol=order_state.symbol,
        )


class TradingStub:
    def __init__(self, *, order_status: str = "FILLED") -> None:
        self.stop_orders: list[dict] = []
        self._status = order_status

    async def create_stop_market_order(
        self,
        *,
        symbol: str,
        side,
        quantity: Decimal,
        stop_price: Decimal,
        order_type: str = "STOP_MARKET",
        **_: dict,
    ) -> dict:
        self.stop_orders.append(
            {
                "symbol": symbol,
                "side": side,
                "quantity": quantity,
                "stop_price": stop_price,
                "order_type": order_type,
            }
        )
        return {"orderType": order_type}

    async def cancel_order(self, *, symbol: str, order_id: int) -> None:  # pragma: no cover - unused in tests
        self.cancelled = getattr(self, "cancelled", [])
        self.cancelled.append({"symbol": symbol, "order_id": order_id})

    async def get_order_status(self, *, symbol: str, order_id: int) -> str:
        return self._status


def _make_bot() -> BotConfig:
    return BotConfig(
        id=uuid4(),
        user_id=uuid4(),
        cred_id=uuid4(),
        symbol="BTCUSDT",
        timeframe="2m",
        enabled=True,
        env="testnet",
        side_whitelist=SideWhitelist.BOTH,
        leverage=5,
        use_balance_pct=False,
        balance_pct=Decimal("0"),
        fixed_notional=Decimal("10"),
        max_position_usdt=None,
    )


def _make_signal(side: OrderSide, *, trigger: Decimal, stop: Decimal) -> ArmSignal:
    now = datetime.now(timezone.utc)
    return ArmSignal(
        version="1",
        side=side,
        symbol="BTCUSDT",
        timeframe="2m",
        ts_ms=int(now.timestamp() * 1000),
        ts=now,
        ind_ts_ms=int(now.timestamp() * 1000),
        ind_ts=now,
        ind_high=trigger + Decimal("1"),
        ind_low=stop - Decimal("1"),
        trigger=trigger,
        stop=stop,
    )


def _make_state(bot_id, side: OrderSide, *, trigger: Decimal, stop: Decimal) -> OrderState:
    return OrderState(
        bot_id=bot_id,
        signal_id="sig-1",
        status=OrderStatus.PENDING,
        side=side,
        symbol="BTCUSDT",
        trigger_price=trigger,
        stop_price=stop,
        quantity=Decimal("0.01"),
        order_id=12345,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "entry_side, expected_exit, trigger, stop",
    [
        (OrderSide.LONG, OrderSide.SHORT, Decimal("100"), Decimal("98")),
        (OrderSide.SHORT, OrderSide.LONG, Decimal("100"), Decimal("102")),
    ],
)
async def test_stop_loss_uses_exit_side(entry_side, expected_exit, trigger, stop) -> None:
    bot = _make_bot()
    signal = _make_signal(entry_side, trigger=trigger, stop=stop)
    state = _make_state(bot.id, entry_side, trigger=trigger, stop=stop)
    executor = OrderExecutorStub(state)
    positions = PositionManagerStub(take_profit=trigger)
    trading = TradingStub(order_status="NEW")

    worker = BotWorker(bot, executor, positions, trading)
    await worker.handle_arm_signal(signal, "msg-1")

    assert trading.stop_orders, "Stop-loss order should be placed"
    stop_order = trading.stop_orders[0]
    assert stop_order["order_type"] == "STOP_MARKET"
    assert stop_order["side"] == expected_exit


@pytest.mark.asyncio
async def test_take_profit_order_created_on_fill() -> None:
    bot = _make_bot()
    trigger = Decimal("100")
    stop = Decimal("98")
    signal = _make_signal(OrderSide.LONG, trigger=trigger, stop=stop)
    state = _make_state(bot.id, OrderSide.LONG, trigger=trigger, stop=stop)
    executor = OrderExecutorStub(state)
    positions = PositionManagerStub(take_profit=Decimal("105"))
    trading = TradingStub(order_status="FILLED")

    worker = BotWorker(bot, executor, positions, trading)
    await worker.handle_arm_signal(signal, "msg-1")

    # Clear any previous stop order recordings for clarity
    trading.stop_orders.clear()

    await worker._check_order_fill()

    assert state.status == OrderStatus.FILLED
    assert positions.calls, "open_position should have been invoked"
    assert len(trading.stop_orders) == 1
    take_profit_order = trading.stop_orders[0]
    assert take_profit_order["order_type"] == "TAKE_PROFIT_MARKET"
    assert take_profit_order["side"] == OrderSide.SHORT
    assert take_profit_order["stop_price"] == Decimal("105")
