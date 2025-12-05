import pytest

from dataclasses import replace
from decimal import Decimal
from datetime import datetime, timezone
from typing import Iterable, List
from uuid import UUID, uuid4

from app.services.worker.application.signal_processor import SignalProcessor
from app.services.worker.domain.enums import OrderSide, OrderStatus, SideWhitelist
from app.services.worker.domain.models import (
    ArmSignal,
    BotConfig,
    DisarmSignal,
    OrderState,
    Position,
)


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
    def __init__(self, states: List[OrderState] | None = None) -> None:
        self._states = list(states or [])
        self.saved: List[OrderState] = []

    async def list_pending_order_states(self, bot_id, symbol, side, statuses=None):
        records = []
        status_filter = set(statuses) if statuses else None
        for state in self._states + self.saved:
            if state.bot_id != bot_id or state.symbol != symbol or state.side != side:
                continue
            if status_filter and state.status not in status_filter:
                continue
            records.append(state)
        return list(records)

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
        raise AssertionError(
            "execute_order should not be called when active trades exist"
        )


class SuccessfulExecutorStub:
    def __init__(self) -> None:
        self.calls: List[tuple[UUID, OrderSide]] = []
        self.counter = 0

    async def execute_order(self, bot: BotConfig, signal: ArmSignal) -> OrderState:
        self.calls.append((bot.id, signal.side))
        self.counter += 1
        quantity = Decimal("0.1") * self.counter
        return OrderState(
            bot_id=bot.id,
            signal_id=signal.signal_msg_id or "",
            status=OrderStatus.PENDING,
            side=signal.side,
            symbol=signal.symbol,
            trigger_price=signal.trigger,
            stop_price=signal.stop,
            quantity=quantity,
        )


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

    processor = SignalProcessor(
        router,
        bots,
        order_executor=OrderExecutorStub(),
        order_gateway=orders,
        trading_factory=trading_factory,
    )

    disarm = _disarm_signal(OrderSide.LONG)
    cancelled = await processor.process_disarm_signal(disarm, "1-0")

    assert cancelled == ["111", "222", "333"]
    assert trading.cancelled == [111, 222, 333]
    assert orders.saved
    assert orders.saved[0].status == OrderStatus.CANCELLED


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


async def test_arm_signal_records_skipped_whitelist_state() -> None:
    bot = replace(_make_bot(), side_whitelist=SideWhitelist.LONG)

    router = RouterStub([bot.id])
    bots = BotRepositoryStub(bot)
    orders = OrderGatewayStub()
    executor = SuccessfulExecutorStub()

    async def trading_factory(_: BotConfig):
        return TradingStub()

    processor = SignalProcessor(router, bots, executor, orders, trading_factory)

    arm_signal = _arm_signal(OrderSide.SHORT)
    results = await processor.process_arm_signal(arm_signal, "msg-1")

    assert results
    state = orders.saved[0]
    assert state.status == OrderStatus.SKIPPED_WHITELIST
    assert state.symbol == arm_signal.symbol
    assert state.side == OrderSide.SHORT
    assert state.bot_id == bot.id
    assert executor.calls == []


async def test_disabled_bot_logs_and_skips(caplog: pytest.LogCaptureFixture) -> None:
    bot = replace(_make_bot(), enabled=False)

    router = RouterStub([bot.id])
    bots = BotRepositoryStub(bot)
    orders = OrderGatewayStub()

    async def trading_factory(_: BotConfig):
        return TradingStub()

    processor = SignalProcessor(
        router, bots, SuccessfulExecutorStub(), orders, trading_factory
    )

    with caplog.at_level("INFO"):
        results = await processor.process_arm_signal(
            _arm_signal(OrderSide.LONG), "msg-2"
        )

    assert not results
    assert not orders.saved
    assert any(
        f"Skipping disabled bot {bot.id}" in message for message in caplog.messages
    )


async def test_second_signal_skipped_when_pyramiding_disabled() -> None:
    bot = _make_bot()

    router = RouterStub([bot.id])
    bots = BotRepositoryStub(bot)

    initial_state = OrderState(
        bot_id=bot.id,
        signal_id="msg-0",
        status=OrderStatus.PENDING,
        side=OrderSide.LONG,
        symbol=bot.symbol,
        trigger_price=Decimal("100"),
        stop_price=Decimal("95"),
        quantity=Decimal("1"),
    )

    orders = OrderGatewayStub([initial_state])
    executor = OrderExecutorStub()

    async def trading_factory(_: BotConfig):
        return TradingStub()

    processor = SignalProcessor(router, bots, executor, orders, trading_factory)

    results = await processor.process_arm_signal(_arm_signal(OrderSide.LONG), "msg-3")

    assert not results
    assert not orders.saved
    assert not executor.executed


class PositionStoreStub:
    def __init__(self) -> None:
        self.positions: List[Position] = []

    def get_position(self, bot_id: UUID):
        matches = self.get_positions(bot_id)
        return matches[-1] if matches else None

    def get_positions(self, bot_id: UUID):
        return [p for p in self.positions if p.bot_id == bot_id]


async def test_dual_signal_processed_when_pyramiding_enabled() -> None:
    bot = _make_bot()
    object.__setattr__(bot, "allow_pyramiding", True)

    router = RouterStub([bot.id])
    bots = BotRepositoryStub(bot)
    orders = OrderGatewayStub()
    executor = SuccessfulExecutorStub()
    position_store = PositionStoreStub()

    async def trading_factory(_: BotConfig):
        return TradingStub()

    processor = SignalProcessor(
        router,
        bots,
        executor,
        orders,
        trading_factory,
        position_store=position_store,
    )

    signal = _arm_signal(OrderSide.LONG)
    results_first = await processor.process_arm_signal(signal, "msg-4")
    # simulate fill storing positions
    first_state = results_first[0]
    position_store.positions.append(
        Position(
            bot_id=bot.id,
            symbol=first_state.symbol,
            side=first_state.side,
            entry_price=first_state.trigger_price,
            quantity=first_state.quantity,
            stop_loss=first_state.stop_price,
            take_profit=first_state.trigger_price + Decimal("10"),
        )
    )

    results_second = await processor.process_arm_signal(signal, "msg-5")

    second_state = results_second[0]
    position_store.positions.append(
        Position(
            bot_id=bot.id,
            symbol=second_state.symbol,
            side=second_state.side,
            entry_price=second_state.trigger_price,
            quantity=second_state.quantity,
            stop_loss=second_state.stop_price,
            take_profit=second_state.trigger_price + Decimal("12"),
        )
    )

    assert len(results_first) == 1
    assert len(results_second) == 1
    assert len(orders.saved) == 2
    assert orders.saved[0].signal_id == "msg-4"
    assert orders.saved[1].signal_id == "msg-5"
    assert len(position_store.get_positions(bot.id)) == 2
