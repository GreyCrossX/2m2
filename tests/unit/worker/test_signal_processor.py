import pytest

from decimal import Decimal
from datetime import datetime, timezone
from typing import Iterable, List
from uuid import UUID, uuid4

from app.services.worker.application.signal_processor import SignalProcessor
from app.services.worker.domain.enums import OrderSide, OrderStatus, SideWhitelist
from app.services.worker.domain.models import ArmSignal, BotConfig, DisarmSignal, OrderState


class RouterStub:
    def __init__(self, bot_ids: Iterable[UUID]) -> None:
        self._bot_ids = list(bot_ids)

    def get_bot_ids(self, symbol: str, timeframe: str) -> Iterable[UUID]:
        return list(self._bot_ids)


class BotRepositoryStub:
    def __init__(self, bot: BotConfig) -> None:
        self._bot = bot

    async def get_bot(self, bot_id):
        if bot_id == self._bot.id:
            return self._bot
        return None


class OrderGatewayStub:
    def __init__(self, states: List[OrderState]) -> None:
        self._states = states
        self.saved: List[OrderState] = []

    async def list_pending_order_states(self, bot_id, symbol, side, statuses=None):
        return list(self._states)

    async def save_state(self, state: OrderState) -> None:
        self.saved.append(state)


class TradingStub:
    def __init__(self) -> None:
        self.cancelled: List[int] = []

    async def cancel_order(self, symbol: str, order_id: int) -> None:
        self.cancelled.append(order_id)


class OrderExecutorStub:
    def __init__(self) -> None:
        self.executed = False

    async def execute_order(self, bot: BotConfig, signal: ArmSignal) -> OrderState:
        self.executed = True
        raise AssertionError("execute_order should not be called when active trades exist")


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
        fixed_notional=Decimal("50"),
        max_position_usdt=None,
    )


def _arm_signal(side: OrderSide) -> ArmSignal:
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
        ind_high=Decimal("110"),
        ind_low=Decimal("90"),
        trigger=Decimal("100"),
        stop=Decimal("95"),
    )


def _disarm_signal(prev_side: OrderSide) -> DisarmSignal:
    now = datetime.now(timezone.utc)
    return DisarmSignal(
        version="1",
        prev_side=prev_side,
        symbol="BTCUSDT",
        timeframe="2m",
        ts_ms=int(now.timestamp() * 1000),
        ts=now,
        reason="trend_flip",
    )


@pytest.mark.asyncio
async def test_disarm_cancels_all_related_orders() -> None:
    bot = _make_bot()
    state = OrderState(
        bot_id=bot.id,
        signal_id="sig-1",
        status=OrderStatus.PENDING,
        side=OrderSide.LONG,
        symbol="BTCUSDT",
        trigger_price=Decimal("100"),
        stop_price=Decimal("95"),
        quantity=Decimal("0.5"),
        order_id=111,
        stop_order_id=222,
        take_profit_order_id=333,
    )

    router = RouterStub([bot.id])
    bots = BotRepositoryStub(bot)
    orders = OrderGatewayStub([state])
    trading = TradingStub()

    async def trading_factory(_: BotConfig):
        return trading

    processor = SignalProcessor(router, bots, order_executor=OrderExecutorStub(), order_gateway=orders, trading_factory=trading_factory)

    disarm = _disarm_signal(OrderSide.LONG)
    cancelled = await processor.process_disarm_signal(disarm, "1-0")

    assert cancelled == ["111", "222", "333"]
    assert trading.cancelled == [111, 222, 333]
    assert orders.saved
    assert orders.saved[0].status == OrderStatus.CANCELLED


@pytest.mark.asyncio
async def test_arm_signal_skips_when_active_trade_present() -> None:
    bot = _make_bot()
    active_state = OrderState(
        bot_id=bot.id,
        signal_id="sig-1",
        status=OrderStatus.PENDING,
        side=OrderSide.LONG,
        symbol="BTCUSDT",
        trigger_price=Decimal("100"),
        stop_price=Decimal("95"),
        quantity=Decimal("0.5"),
        order_id=111,
        stop_order_id=222,
        take_profit_order_id=333,
    )

    router = RouterStub([bot.id])
    bots = BotRepositoryStub(bot)
    orders = OrderGatewayStub([active_state])
    executor = OrderExecutorStub()

    async def trading_factory(_: BotConfig):
        return TradingStub()

    processor = SignalProcessor(router, bots, executor, orders, trading_factory)

    arm_signal = _arm_signal(OrderSide.LONG)
    results = await processor.process_arm_signal(arm_signal, "1-0")

    assert results == []
    assert not executor.executed
    assert not orders.saved
