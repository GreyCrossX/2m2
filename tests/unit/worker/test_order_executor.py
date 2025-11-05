import pytest
from decimal import Decimal
from datetime import datetime, timezone
from uuid import uuid4

from app.services.worker.application.order_executor import OrderExecutor
from app.services.worker.domain.enums import OrderSide, OrderStatus, SideWhitelist
from app.services.worker.domain.models import ArmSignal, BotConfig


class BalanceValidatorStub:
    def __init__(self, balance: Decimal) -> None:
        self._balance = balance

    async def validate_balance(
        self,
        bot: BotConfig,
        required_margin: Decimal,
        *,
        available_balance: Decimal | None = None,
    ) -> tuple[bool, Decimal]:
        balance = self._balance if available_balance is None else available_balance
        return True, balance

    async def get_available_balance(self, cred_id, env) -> Decimal:
        return self._balance


class TradingStub:
    def __init__(self, *, fail_stop: bool = False, fail_tp: bool = False) -> None:
        self.fail_stop = fail_stop
        self.fail_tp = fail_tp
        self.limit_orders: list[dict] = []
        self.stop_orders: list[dict] = []
        self.tp_orders: list[dict] = []
        self.cancelled: list[dict] = []

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        return None

    async def quantize_limit_order(self, symbol: str, quantity: Decimal, price: Decimal) -> tuple[Decimal, Decimal]:
        return quantity, price

    async def create_limit_order(self, **payload) -> dict:
        self.limit_orders.append(payload)
        return {"orderId": 111}

    async def create_stop_market_order(self, **payload) -> dict:
        self.stop_orders.append(payload)
        if self.fail_stop:
            raise RuntimeError("stop failed")
        return {"orderId": 222}

    async def create_take_profit_limit(self, **payload) -> dict:
        self.tp_orders.append(payload)
        if self.fail_tp:
            raise RuntimeError("tp failed")
        return {"orderId": 333}

    async def get_symbol_filters(self, symbol: str) -> dict:
        return {}

    async def cancel_order(self, *, symbol: str, order_id: int) -> None:
        self.cancelled.append({"symbol": symbol, "order_id": order_id})


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


def _make_signal(side: OrderSide) -> ArmSignal:
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


@pytest.mark.asyncio
async def test_execute_order_places_stop_and_tp() -> None:
    bot = _make_bot()
    signal = _make_signal(OrderSide.LONG)
    balance = BalanceValidatorStub(Decimal("1000"))
    trading = TradingStub()

    executor = OrderExecutor(balance_validator=balance, binance_client=trading)
    state = await executor.execute_order(bot, signal)

    assert trading.limit_orders, "Entry order should be placed"
    assert trading.stop_orders, "Stop-loss should be placed"
    assert trading.tp_orders, "Take-profit should be placed"

    stop_call = trading.stop_orders[0]
    assert stop_call["side"] == OrderSide.SHORT
    assert stop_call.get("reduce_only") is True
    assert stop_call["stop_price"] == signal.stop

    tp_call = trading.tp_orders[0]
    expected_tp = signal.trigger + (signal.trigger - signal.stop) * Decimal("1.5")
    assert tp_call["price"] == expected_tp
    assert tp_call["side"] == OrderSide.SHORT

    assert state.order_id == 111
    assert state.stop_order_id == 222
    assert state.take_profit_order_id == 333
    assert state.status == OrderStatus.PENDING


@pytest.mark.asyncio
async def test_execute_order_rolls_back_when_tp_fails() -> None:
    bot = _make_bot()
    signal = _make_signal(OrderSide.LONG)
    balance = BalanceValidatorStub(Decimal("1000"))
    trading = TradingStub(fail_tp=True)

    executor = OrderExecutor(balance_validator=balance, binance_client=trading)

    state = await executor.execute_order(bot, signal)

    assert state.status == OrderStatus.FAILED
    assert state.order_id is None
    assert state.stop_order_id is None
    assert state.take_profit_order_id is None

    # stop should be placed before failure
    assert trading.stop_orders
    # entry and stop must be cancelled on rollback
    cancelled_ids = {item["order_id"] for item in trading.cancelled}
    assert cancelled_ids == {111, 222}


@pytest.mark.asyncio
async def test_execute_order_rolls_back_when_stop_fails() -> None:
    bot = _make_bot()
    signal = _make_signal(OrderSide.LONG)
    balance = BalanceValidatorStub(Decimal("1000"))
    trading = TradingStub(fail_stop=True)

    executor = OrderExecutor(balance_validator=balance, binance_client=trading)

    state = await executor.execute_order(bot, signal)

    assert state.status == OrderStatus.FAILED
    assert state.order_id is None
    assert state.stop_order_id is None
    assert state.take_profit_order_id is None

    # entry must be cancelled when stop placement fails
    cancelled_ids = [item["order_id"] for item in trading.cancelled]
    assert cancelled_ids == [111]
    # stop order should not remain recorded on failure
    assert trading.tp_orders == []


def test_compute_tp_price_handles_inverted_stops() -> None:
    balance = BalanceValidatorStub(Decimal("1000"))
    executor = OrderExecutor(balance_validator=balance, binance_client=TradingStub())

    long_tp = executor._compute_tp_price(OrderSide.LONG, Decimal("100"), Decimal("105"))
    short_tp = executor._compute_tp_price(OrderSide.SHORT, Decimal("100"), Decimal("95"))

    rr = executor._tp_r
    assert long_tp == Decimal("100") + (Decimal("5") * rr)
    assert short_tp == Decimal("100") - (Decimal("5") * rr)
