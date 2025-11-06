from __future__ import annotations

import asyncio
from dataclasses import replace
from decimal import Decimal
from typing import Dict, Iterable, List, Optional
from uuid import UUID, uuid4

import pytest

from app.services.worker.application.order_monitor import BinanceOrderMonitor
from app.services.worker.application.position_manager import PositionManager
from app.services.worker.domain.enums import OrderStatus, OrderSide, SideWhitelist
from app.services.worker.domain.models import BotConfig, OrderState


class BotRepoStub:
    def __init__(self, bot: BotConfig) -> None:
        self._bot = bot

    async def get_bot(self, bot_id: UUID) -> Optional[BotConfig]:
        if bot_id == self._bot.id:
            return self._bot
        return None


class OrderGatewayStub:
    def __init__(self, states: Iterable[OrderState]) -> None:
        self.states: List[OrderState] = list(states)
        self.saved: List[OrderState] = []

    async def list_states_by_statuses(self, statuses: Iterable[OrderStatus]) -> List[OrderState]:
        wanted = set(statuses)
        return [state for state in self.states if state.status in wanted]

    async def save_state(self, state: OrderState) -> None:
        for idx, existing in enumerate(self.states):
            if existing.id == state.id:
                self.states[idx] = state
                break
        else:
            self.states.append(state)
        self.saved.append(state)


class TradingStub:
    def __init__(self) -> None:
        self.orders: Dict[int, Dict[str, str]] = {}
        self.cancelled: List[int] = []

    def set_order(
        self,
        order_id: int,
        *,
        status: str,
        executed: str,
        price: str,
        stop_price: str | None = None,
    ) -> None:
        payload = {
            "orderId": order_id,
            "status": status,
            "executedQty": executed,
            "avgPrice": price,
        }
        if stop_price is not None:
            payload["stopPrice"] = stop_price
        self.orders[order_id] = payload

    async def get_order(self, symbol: str, order_id: int) -> Dict[str, str]:
        entry = self.orders.get(int(order_id))
        if entry is None:
            raise RuntimeError("order not found")
        return entry

    async def cancel_order(self, symbol: str, order_id: int) -> None:
        self.cancelled.append(int(order_id))
        entry = self.orders.get(int(order_id))
        if entry is not None:
            entry["status"] = "CANCELED"

    async def list_open_orders(self, symbol: str | None = None) -> List[Dict[str, str]]:
        return [o for o in self.orders.values() if o.get("status") not in {"CANCELED", "FILLED"}]


def _bot() -> BotConfig:
    return BotConfig(
        id=uuid4(),
        user_id=uuid4(),
        cred_id=uuid4(),
        symbol="BTCUSDT",
        timeframe="2m",
        enabled=True,
        env="testnet",
        side_whitelist=SideWhitelist.BOTH,
        leverage=1,
        use_balance_pct=False,
        balance_pct=Decimal("0"),
        fixed_notional=Decimal("50"),
        max_position_usdt=None,
    )


def _order_state(bot: BotConfig, status: OrderStatus, *, order_id: int = 1) -> OrderState:
    return OrderState(
        bot_id=bot.id,
        signal_id="sig",
        status=status,
        side=OrderSide.LONG,
        symbol=bot.symbol,
        trigger_price=Decimal("100"),
        stop_price=Decimal("95"),
        quantity=Decimal("1"),
        order_id=order_id,
    )


@pytest.mark.asyncio
async def test_entry_fill_transitions_to_armed() -> None:
    bot = _bot()
    trading = TradingStub()
    trading.set_order(1, status="FILLED", executed="1", price="101")

    state = _order_state(bot, OrderStatus.PENDING, order_id=1)
    gateway = OrderGatewayStub([state])
    positions = PositionManager()

    monitor = BinanceOrderMonitor(
        bot_repository=BotRepoStub(bot),
        order_gateway=gateway,
        position_manager=positions,
        trading_factory=lambda _bot: asyncio.sleep(0, trading),
        poll_interval=0.1,
    )

    await monitor.run_once()

    saved_state = gateway.states[0]
    assert saved_state.status == OrderStatus.ARMED
    assert saved_state.filled_quantity == Decimal("1")
    assert positions.get_position(bot.id) is not None


@pytest.mark.asyncio
async def test_exit_fill_closes_position_and_cancels_other_leg() -> None:
    bot = _bot()
    trading = TradingStub()
    trading.set_order(1, status="FILLED", executed="1", price="101")
    trading.set_order(2, status="NEW", executed="0", price="0", stop_price="98")
    trading.set_order(3, status="FILLED", executed="1", price="105")

    state = replace(
        _order_state(bot, OrderStatus.ARMED, order_id=1),
        stop_order_id=2,
        take_profit_order_id=3,
        filled_quantity=Decimal("1"),
        avg_fill_price=Decimal("101"),
    )
    gateway = OrderGatewayStub([state])
    positions = PositionManager()
    await positions.open_position(bot.id, replace(state, status=OrderStatus.FILLED))

    monitor = BinanceOrderMonitor(
        bot_repository=BotRepoStub(bot),
        order_gateway=gateway,
        position_manager=positions,
        trading_factory=lambda _bot: asyncio.sleep(0, trading),
        poll_interval=0.1,
    )

    await monitor.run_once()

    updated = gateway.states[0]
    assert updated.status == OrderStatus.CLOSED
    assert positions.get_position(bot.id) is None
    assert 2 in trading.cancelled


@pytest.mark.asyncio
async def test_sync_on_startup_rehydrates_position() -> None:
    bot = _bot()
    trading = TradingStub()
    trading.set_order(1, status="FILLED", executed="1", price="100")

    state = _order_state(bot, OrderStatus.FILLED, order_id=1)
    gateway = OrderGatewayStub([state])
    positions = PositionManager()

    monitor = BinanceOrderMonitor(
        bot_repository=BotRepoStub(bot),
        order_gateway=gateway,
        position_manager=positions,
        trading_factory=lambda _bot: asyncio.sleep(0, trading),
        poll_interval=0.1,
    )

    await monitor.sync_on_startup()

    stored = gateway.states[0]
    assert stored.status == OrderStatus.ARMED
    assert stored.filled_quantity == Decimal("1")
    assert positions.get_position(bot.id) is not None
