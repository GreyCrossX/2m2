from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional
from uuid import UUID, uuid4

import pytest

from app.services.worker.application.order_executor import OrderExecutor
from app.services.worker.domain.enums import OrderStatus, Side
from app.services.worker.domain.exceptions import BinanceAPIException, DomainBadRequest
from app.services.worker.domain.models import ArmSignal, BotConfig


class _BalanceValidatorStub:
    async def validate_balance(self, bot: BotConfig, required_margin: Decimal) -> tuple[bool, Decimal]:
        return True, required_margin

    async def get_available_balance(self, cred_id: UUID, env: str) -> Decimal:
        return Decimal("100")


class _TradingPortStub:
    def __init__(self, quantized_qty: Decimal, quantized_price: Decimal, *, fail: bool = False) -> None:
        self.quantized_qty = quantized_qty
        self.quantized_price = quantized_price
        self.fail = fail
        self.received = None
        self.leverage_calls: list[tuple[str, int]] = []

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        self.leverage_calls.append((symbol, leverage))

    async def quantize_limit_order(
        self,
        symbol: str,
        quantity: Decimal,
        price: Decimal,
    ) -> tuple[Decimal, Decimal | None]:
        return self.quantized_qty, self.quantized_price

    async def create_limit_order(
        self,
        symbol: str,
        side: Side,
        quantity: Decimal,
        price: Decimal,
        reduce_only: bool = False,
        time_in_force: str = "GTC",
        new_client_order_id: Optional[str] = None,
    ) -> dict:
        self.received = {
            "symbol": symbol,
            "side": side,
            "quantity": quantity,
            "price": price,
            "reduce_only": reduce_only,
            "time_in_force": time_in_force,
            "client_id": new_client_order_id,
        }
        if self.fail:
            raise DomainBadRequest("simulated rejection")
        return {"orderId": 111}


@pytest.fixture
def order_executor() -> OrderExecutor:
    return OrderExecutor(balance_validator=_BalanceValidatorStub(), binance_client=None)


def _build_signal() -> ArmSignal:
    now = datetime.now(timezone.utc)
    return ArmSignal(
        version="1",
        side=Side.LONG,
        symbol="BTCUSDT",
        timeframe="2m",
        ts_ms=int(now.timestamp() * 1000),
        ts=now,
        ind_ts_ms=int(now.timestamp() * 1000),
        ind_ts=now,
        ind_high=Decimal("101"),
        ind_low=Decimal("99"),
        trigger=Decimal("100.1234"),
        stop=Decimal("98.5"),
    )


def _build_bot() -> BotConfig:
    return BotConfig(
        id=uuid4(),
        user_id=uuid4(),
        cred_id=uuid4(),
        symbol="BTCUSDT",
        timeframe="2m",
        enabled=True,
        env="testnet",
        side_whitelist=Side.BOTH,
        leverage=5,
        use_balance_pct=False,
        balance_pct=Decimal("0"),
        fixed_notional=Decimal("10"),
        max_position_usdt=None,
    )


@pytest.mark.asyncio
async def test_order_is_quantized_before_submission(order_executor: OrderExecutor) -> None:
    trading = _TradingPortStub(quantized_qty=Decimal("0.003"), quantized_price=Decimal("100.10"))
    order_executor._bx = trading  # inject stub synchronously

    bot = _build_bot()
    signal = _build_signal()

    state = await order_executor.execute_order(bot, signal)

    assert state.status == OrderStatus.PENDING
    assert state.quantity == Decimal("0.003")
    assert state.trigger_price == Decimal("100.10")
    assert trading.received["quantity"] == Decimal("0.003")
    assert trading.received["price"] == Decimal("100.10")


@pytest.mark.asyncio
async def test_adapter_rejection_raises_domain_error(order_executor: OrderExecutor) -> None:
    trading = _TradingPortStub(quantized_qty=Decimal("0.003"), quantized_price=Decimal("100.10"), fail=True)
    order_executor._bx = trading

    bot = _build_bot()
    signal = _build_signal()

    with pytest.raises(BinanceAPIException) as exc:
        await order_executor.execute_order(bot, signal)

    assert "raw_qty" in str(exc.value)
