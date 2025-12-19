from dataclasses import replace
from decimal import Decimal
from datetime import datetime, timezone
from uuid import uuid4

from app.services.worker.application.order_executor import (
    OrderExecutor,
    _bot_client_prefix,
)
from app.services.worker.domain.enums import OrderSide, OrderStatus, SideWhitelist
from app.services.worker.domain.exceptions import DomainExchangeDown, DomainRateLimit
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
    def __init__(
        self,
        *,
        fail_stop: bool = False,
        fail_tp: bool = False,
        entry_exc: Exception | None = None,
        stop_exc: Exception | None = None,
        tp_exc: Exception | None = None,
        filters: dict | None = None,
    ) -> None:
        self.fail_stop = fail_stop
        self.fail_tp = fail_tp
        self.entry_exc = entry_exc
        self.stop_exc = stop_exc
        self.tp_exc = tp_exc
        self.limit_orders: list[dict] = []
        self.stop_orders: list[dict] = []
        self.tp_orders: list[dict] = []
        self.cancelled: list[dict] = []
        self.open_orders: list[dict] = []
        self.call_log: list[tuple[str, dict]] = []
        self._filters = filters or {
            "LOT_SIZE": {"stepSize": "0.001", "minQty": "0"},
            "META": {"quantityPrecision": 3},
            "NOTIONAL": {"notional": "20"},
        }

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        return None

    async def quantize_limit_order(
        self, symbol: str, quantity: Decimal, price: Decimal
    ) -> tuple[Decimal, Decimal]:
        return quantity, price

    async def create_limit_order(self, **payload) -> dict:
        if self.entry_exc is not None:
            raise self.entry_exc
        self.limit_orders.append(payload)
        self.call_log.append(("entry", payload))
        return {"orderId": 111}

    async def create_stop_market_order(self, **payload) -> dict:
        order_type = payload.get("order_type", "STOP_MARKET")
        target = (
            self.tp_orders if order_type == "TAKE_PROFIT_MARKET" else self.stop_orders
        )
        target.append(payload)
        if self.stop_exc is not None and order_type == "STOP_MARKET":
            raise self.stop_exc
        if self.tp_exc is not None and order_type == "TAKE_PROFIT_MARKET":
            raise self.tp_exc
        if self.fail_stop and order_type == "STOP_MARKET":
            raise RuntimeError("stop failed")
        if self.fail_tp and order_type == "TAKE_PROFIT_MARKET":
            raise RuntimeError("tp failed")
        order = {
            "orderId": 222 if order_type == "STOP_MARKET" else 333,
            "clientOrderId": payload.get("new_client_order_id"),
        }
        self.open_orders.append(order)
        self.call_log.append(("stop" if order_type == "STOP_MARKET" else "tp", payload))
        return order

    async def get_symbol_filters(self, symbol: str) -> dict:
        return self._filters

    async def cancel_order(self, *, symbol: str, order_id: int) -> None:
        self.cancelled.append({"symbol": symbol, "order_id": order_id})
        self.open_orders = [o for o in self.open_orders if o.get("orderId") != order_id]

    async def list_open_orders(self, symbol: str | None = None) -> list[dict]:
        return list(self.open_orders)


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


def test_calculate_position_size_uses_fixed_notional() -> None:
    bot = replace(_make_bot(), fixed_notional=Decimal("200"))
    balance = BalanceValidatorStub(Decimal("1000"))
    executor = OrderExecutor(balance_validator=balance, binance_client=TradingStub())

    qty = executor._calculate_position_size(bot, Decimal("1000"), Decimal("100"))

    assert qty == Decimal("2")


def test_calculate_position_size_uses_balance_percentage() -> None:
    bot = replace(
        _make_bot(),
        fixed_notional=None,
        use_balance_pct=True,
        balance_pct=Decimal("0.25"),
    )
    balance = BalanceValidatorStub(Decimal("1000"))
    executor = OrderExecutor(balance_validator=balance, binance_client=TradingStub())

    qty = executor._calculate_position_size(bot, Decimal("1000"), Decimal("100"))

    assert qty == Decimal("2.5")


def test_calculate_position_size_caps_balance_percentage_at_one() -> None:
    bot = replace(
        _make_bot(),
        fixed_notional=None,
        use_balance_pct=True,
        balance_pct=Decimal("1.5"),
    )
    balance = BalanceValidatorStub(Decimal("1000"))
    executor = OrderExecutor(balance_validator=balance, binance_client=TradingStub())

    qty = executor._calculate_position_size(bot, Decimal("1000"), Decimal("100"))

    assert qty == Decimal("10")


def test_calculate_position_size_returns_zero_without_configured_method() -> None:
    bot = replace(_make_bot(), fixed_notional=None, use_balance_pct=False)
    balance = BalanceValidatorStub(Decimal("1000"))
    executor = OrderExecutor(balance_validator=balance, binance_client=TradingStub())

    qty = executor._calculate_position_size(bot, Decimal("1000"), Decimal("100"))

    assert qty == Decimal("0")


def test_calculate_position_size_honors_max_position_cap() -> None:
    bot = replace(
        _make_bot(),
        fixed_notional=Decimal("500"),
        max_position_usdt=Decimal("100"),
    )
    balance = BalanceValidatorStub(Decimal("1000"))
    executor = OrderExecutor(balance_validator=balance, binance_client=TradingStub())

    qty = executor._calculate_position_size(bot, Decimal("1000"), Decimal("100"))

    assert qty == Decimal("1")


async def test_preflight_qty_respects_symbol_precision() -> None:
    filters = {
        "LOT_SIZE": {"stepSize": "0", "minQty": "0"},
        "META": {"quantityPrecision": 6},
        "NOTIONAL": {"notional": "5"},
    }
    trading = TradingStub(filters=filters)
    balance = BalanceValidatorStub(Decimal("1000"))
    executor = OrderExecutor(balance_validator=balance, binance_client=trading)

    ok, qty, reason = await executor._preflight_qty(
        trading,
        "BTCUSDT",
        Decimal("0.12345678"),
        Decimal("100"),
        Decimal("1"),
        Decimal("1000"),
    )

    assert ok is True
    assert qty == Decimal("0.123456")
    assert reason is None


async def test_preflight_qty_bumps_to_min_notional_and_checks_margin() -> None:
    filters = {
        "LOT_SIZE": {"stepSize": "0.01", "minQty": "0.05"},
        "META": {"quantityPrecision": 2},
        "NOTIONAL": {"notional": "50"},
    }
    trading = TradingStub(filters=filters)
    balance = BalanceValidatorStub(Decimal("500"))
    executor = OrderExecutor(balance_validator=balance, binance_client=trading)

    ok, qty, reason = await executor._preflight_qty(
        trading,
        "BTCUSDT",
        Decimal("0.02"),
        Decimal("1000"),
        Decimal("5"),
        Decimal("500"),
    )

    assert ok is True
    assert qty == Decimal("0.05")
    assert reason is None


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
    assert tp_call["stop_price"] == expected_tp
    assert tp_call["side"] == OrderSide.SHORT
    assert tp_call.get("order_type") == "TAKE_PROFIT_MARKET"
    assert tp_call.get("time_in_force") == "GTE_GTC"
    # ensure trio submitted in order: entry -> stop -> tp
    assert [c[0] for c in trading.call_log[:3]] == ["entry", "stop", "tp"]

    assert state.order_id == 111
    assert state.stop_order_id == 222
    assert state.take_profit_order_id == 333
    assert state.status == OrderStatus.PENDING


async def test_cleanup_cancels_tagged_exits_and_tags_new_orders() -> None:
    bot = _make_bot()
    signal = _make_signal(OrderSide.LONG)
    balance = BalanceValidatorStub(Decimal("1000"))
    trading = TradingStub()

    prefix = _bot_client_prefix(bot.id)
    trading.open_orders = [
        {"order_id": 9001, "client_order_id": f"{prefix}-sl-old"},
        {"order_id": 9002, "client_order_id": f"{prefix}-tp-old"},
        {"order_id": 9003, "client_order_id": "otherbot-tp"},
    ]

    executor = OrderExecutor(balance_validator=balance, binance_client=trading)
    state = await executor.execute_order(bot, signal)

    cancelled_ids = {item["order_id"] for item in trading.cancelled}
    assert {9001, 9002}.issubset(cancelled_ids)
    assert 9003 not in cancelled_ids

    stop_call = trading.stop_orders[0]
    tp_call = trading.tp_orders[0]
    assert stop_call.get("new_client_order_id", "").startswith(prefix)
    assert tp_call.get("new_client_order_id", "").startswith(prefix)
    assert state.status == OrderStatus.PENDING


async def test_execute_order_rolls_back_when_tp_fails() -> None:
    bot = _make_bot()
    signal = _make_signal(OrderSide.LONG)
    balance = BalanceValidatorStub(Decimal("1000"))
    trading = TradingStub(fail_tp=True)

    executor = OrderExecutor(balance_validator=balance, binance_client=trading)

    state = await executor.execute_order(bot, signal)

    assert state.status == OrderStatus.PENDING
    assert state.order_id == 111
    assert state.stop_order_id == 222
    assert state.take_profit_order_id is None

    # stop should be placed before failure
    assert trading.stop_orders
    assert trading.tp_orders  # attempted TP placement captured
    assert trading.cancelled == []


async def test_execute_order_rolls_back_when_stop_fails() -> None:
    bot = _make_bot()
    signal = _make_signal(OrderSide.LONG)
    balance = BalanceValidatorStub(Decimal("1000"))
    trading = TradingStub(fail_stop=True)

    executor = OrderExecutor(balance_validator=balance, binance_client=trading)

    state = await executor.execute_order(bot, signal)

    assert state.status == OrderStatus.PENDING
    assert state.order_id == 111
    assert state.stop_order_id is None
    assert state.take_profit_order_id == 333

    assert trading.stop_orders  # attempted stop placement captured
    assert trading.tp_orders
    assert trading.cancelled == []


async def test_execute_order_marks_failed_on_rate_limit_entry() -> None:
    bot = _make_bot()
    signal = _make_signal(OrderSide.LONG)
    balance = BalanceValidatorStub(Decimal("1000"))
    trading = TradingStub(entry_exc=DomainRateLimit("rate"))

    executor = OrderExecutor(balance_validator=balance, binance_client=trading)

    state = await executor.execute_order(bot, signal)

    assert state.status == OrderStatus.FAILED
    assert state.order_id is None
    assert trading.cancelled == []


async def test_execute_order_rolls_back_on_rate_limit_tp() -> None:
    bot = _make_bot()
    signal = _make_signal(OrderSide.LONG)
    balance = BalanceValidatorStub(Decimal("1000"))
    trading = TradingStub(tp_exc=DomainRateLimit("rate"))

    executor = OrderExecutor(balance_validator=balance, binance_client=trading)

    state = await executor.execute_order(bot, signal)

    assert state.status == OrderStatus.PENDING
    assert state.order_id == 111
    assert state.stop_order_id == 222
    assert state.take_profit_order_id is None
    assert trading.cancelled == []


async def test_execute_order_rolls_back_on_rate_limit_stop() -> None:
    bot = _make_bot()
    signal = _make_signal(OrderSide.LONG)
    balance = BalanceValidatorStub(Decimal("1000"))
    trading = TradingStub(stop_exc=DomainRateLimit("rate"))

    executor = OrderExecutor(balance_validator=balance, binance_client=trading)

    state = await executor.execute_order(bot, signal)

    assert state.status == OrderStatus.PENDING
    assert state.order_id == 111
    assert state.stop_order_id is None
    assert state.take_profit_order_id == 333
    assert trading.cancelled == []


async def test_execute_order_rolls_back_on_exchange_down_stop() -> None:
    bot = _make_bot()
    signal = _make_signal(OrderSide.LONG)
    balance = BalanceValidatorStub(Decimal("1000"))
    trading = TradingStub(stop_exc=DomainExchangeDown("down"))

    executor = OrderExecutor(balance_validator=balance, binance_client=trading)

    state = await executor.execute_order(bot, signal)

    assert state.status == OrderStatus.PENDING
    assert state.order_id == 111
    assert state.stop_order_id is None
    assert state.take_profit_order_id == 333
    assert trading.cancelled == []


def test_compute_tp_price_handles_inverted_stops() -> None:
    balance = BalanceValidatorStub(Decimal("1000"))
    executor = OrderExecutor(balance_validator=balance, binance_client=TradingStub())

    long_tp = executor._compute_tp_price(OrderSide.LONG, Decimal("100"), Decimal("105"))
    short_tp = executor._compute_tp_price(
        OrderSide.SHORT, Decimal("100"), Decimal("95")
    )

    rr = executor._tp_r
    assert long_tp == Decimal("100") + (Decimal("5") * rr)
    assert short_tp == Decimal("100") - (Decimal("5") * rr)
