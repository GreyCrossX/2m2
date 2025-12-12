from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Dict, Iterable, List, Optional
from uuid import UUID, uuid4

from app.services.worker.application.order_executor import OrderExecutor
from app.services.worker.application.signal_processor import SignalProcessor
from app.services.worker.domain.enums import OrderSide, OrderStatus
from app.services.worker.domain.models import (
    ArmSignal,
    BotConfig,
    DisarmSignal,
    OrderState,
)


class StubBalanceValidator:
    async def validate_balance(
        self,
        bot: BotConfig,
        required_margin: Decimal,
        *,
        available_balance: Decimal | None = None,
    ):
        available = available_balance or Decimal("1_000_000")
        return True, available

    async def get_available_balance(self, cred_id: UUID, env: str) -> Decimal:
        return Decimal("1_000_000")


class StubTrading:
    def __init__(self) -> None:
        self.cancelled: List[int] = []
        self.created: List[int] = []
        self.open_orders: List[dict] = []
        self._next_id = 1

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        return None

    async def quantize_limit_order(
        self, symbol: str, quantity: Decimal, price: Decimal
    ):
        return quantity, price

    async def create_limit_order(
        self,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        price: Decimal,
        reduce_only: bool = False,
        time_in_force: str = "GTC",
        new_client_order_id: Optional[str] = None,
    ) -> dict:
        oid = self._next()
        self.created.append(oid)
        return {"orderId": oid}

    async def create_stop_market_order(
        self,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        stop_price: Decimal,
        *,
        reduce_only: bool = True,
        order_type: str = "STOP_MARKET",
        time_in_force: str | None = None,
        new_client_order_id: Optional[str] = None,
    ) -> dict:
        oid = self._next()
        self.created.append(oid)
        record = {
            "orderId": oid,
            "clientOrderId": new_client_order_id,
            "order_type": order_type,
            "timeInForce": time_in_force,
        }
        self.open_orders.append(record)
        return record

    async def create_take_profit_limit(
        self,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        price: Decimal,
        stop_price: Decimal,
        reduce_only: bool = True,
        time_in_force: str = "GTC",
        new_client_order_id: Optional[str] = None,
    ) -> dict:
        oid = self._next()
        self.created.append(oid)
        record = {"orderId": oid, "clientOrderId": new_client_order_id}
        self.open_orders.append(record)
        return record

    async def get_symbol_filters(self, symbol: str) -> dict:
        # Minimal filter so quantization/min-notional passes
        return {
            "LOT_SIZE": {"stepSize": "0.001", "minQty": "0.001", "maxQty": "100000"},
            "PRICE_FILTER": {
                "tickSize": "0.01",
                "minPrice": "0.01",
                "maxPrice": "1000000",
            },
            "MIN_NOTIONAL": {"notional": "5"},
            "META": {"quantityPrecision": 3, "pricePrecision": 2},
        }

    async def cancel_order(self, symbol: str, order_id: int) -> None:
        self.cancelled.append(order_id)
        self.open_orders = [o for o in self.open_orders if o.get("orderId") != order_id]

    async def list_open_orders(self, symbol: str | None = None) -> List[dict]:
        return list(self.open_orders)

    def _next(self) -> int:
        oid = self._next_id
        self._next_id += 1
        return oid


class InMemoryGateway:
    def __init__(self) -> None:
        self.states: Dict[tuple[UUID, str], OrderState] = {}

    async def save_state(self, state: OrderState) -> None:
        self.states[(state.bot_id, state.signal_id)] = state

    async def list_pending_order_states(
        self,
        bot_id: UUID,
        symbol: str,
        side: OrderSide,
        statuses: Optional[Iterable[OrderStatus]] = None,
    ) -> List[OrderState]:
        out: List[OrderState] = []
        for key, state in self.states.items():
            if state.bot_id != bot_id:
                continue
            if state.symbol != symbol:
                continue
            if state.side != side:
                continue
            if statuses and state.status not in statuses:
                continue
            out.append(state)
        return out

    async def list_states_by_statuses(
        self, statuses: Iterable[OrderStatus]
    ) -> List[OrderState]:
        return [s for s in self.states.values() if s.status in statuses]


class StubRouter:
    def __init__(self, bot_ids: Iterable[UUID]) -> None:
        self.bot_ids = list(bot_ids)

    def get_bot_ids(self, symbol: str, timeframe: str):
        return self.bot_ids


class StubBotRepo:
    def __init__(self, bot: BotConfig) -> None:
        self.bot = bot

    async def get_bot(self, bot_id: UUID) -> Optional[BotConfig]:
        return self.bot if bot_id == self.bot.id else None


def _bot_config(symbol: str = "BTCUSDT") -> BotConfig:
    return BotConfig(
        id=uuid4(),
        user_id=uuid4(),
        cred_id=uuid4(),
        symbol=symbol,
        timeframe="2m",
        enabled=True,
        env="testnet",
        side_whitelist=OrderSide.LONG,  # allows long only
        leverage=5,
        use_balance_pct=True,
        balance_pct=Decimal("0.05"),
        fixed_notional=Decimal("0"),
        max_position_usdt=Decimal("0"),
    )


def _arm_signal(symbol: str = "BTCUSDT") -> ArmSignal:
    return ArmSignal.from_stream(
        {
            "v": "1",
            "type": "arm",
            "side": "long",
            "sym": symbol,
            "tf": "2m",
            "ts": "1700000000000",
            "ind_ts": "1699999999000",
            "ind_high": "35000.5",
            "ind_low": "34900.1",
            "trigger": "35010.0",
            "stop": "34900.0",
        }
    )


def _disarm_signal(symbol: str = "BTCUSDT") -> DisarmSignal:
    return DisarmSignal.from_stream(
        {
            "v": "1",
            "type": "disarm",
            "prev_side": "long",
            "sym": symbol,
            "tf": "2m",
            "ts": "1700000005000",
            "reason": "manual-cancel",
        }
    )


def _build_system():
    bot = _bot_config()
    bal = StubBalanceValidator()
    trading = StubTrading()

    async def trading_factory(_: BotConfig):
        return trading

    executor = OrderExecutor(
        balance_validator=bal,
        trading_factory=trading_factory,
    )
    gateway = InMemoryGateway()
    router = StubRouter([bot.id])
    repo = StubBotRepo(bot)
    processor = SignalProcessor(
        router=router,
        bot_repository=repo,
        order_executor=executor,
        order_gateway=gateway,
        trading_factory=trading_factory,
        position_store=None,
    )
    return bot, trading, gateway, processor


async def _run_arm(
    processor: SignalProcessor, gateway: InMemoryGateway, symbol: str = "BTCUSDT"
):
    sig = _arm_signal(symbol)
    res = await processor.process_arm_signal(sig, message_id="1-0", log_context={})
    assert len(res) == 1
    key = (res[0].bot_id, "1-0")
    state = gateway.states[key]
    return state


async def _run_disarm(processor: SignalProcessor, symbol: str = "BTCUSDT"):
    sig = _disarm_signal(symbol)
    return await processor.process_disarm_signal(sig, message_id="1-1", log_context={})


def test_signal_to_cancel_round_trip():
    bot, trading, gateway, processor = _build_system()
    state = asyncio.run(_run_arm(processor, gateway, bot.symbol))

    # ARM created a pending state with ids and persisted it
    assert state.status == OrderStatus.PENDING
    assert state.order_id is not None
    assert state.stop_order_id is not None
    assert state.take_profit_order_id is not None
    assert state.trigger_price > 0
    assert state.quantity > 0

    cancelled = asyncio.run(_run_disarm(processor, bot.symbol))

    # DISARM should cancel all legs and mark state cancelled
    assert len(cancelled) == 3
    assert set(trading.cancelled) == {
        state.order_id,
        state.stop_order_id,
        state.take_profit_order_id,
    }

    stored = gateway.states[(bot.id, "1-0")]
    assert stored.status == OrderStatus.CANCELLED
