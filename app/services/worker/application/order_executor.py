from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from typing import Optional, Protocol

from ..domain.models import ArmSignal, BotConfig, OrderState
from ..domain.enums import OrderStatus, Side
from ..domain.exceptions import InsufficientBalanceException, BinanceAPIException


# --------- Ports (to be implemented by Infra) ---------

class BalanceValidatorPort(Protocol):
    async def validate_balance(self, bot: BotConfig, required_margin: Decimal) -> tuple[bool, Decimal]: ...
    async def get_available_balance(self, user_id, env: str) -> Decimal: ...


class BinanceClient(Protocol):
    """Trading subset required by the use case (UMFutures)."""
    async def set_leverage(self, symbol: str, leverage: int) -> None: ...
    async def create_limit_order(
        self,
        symbol: str,
        side: str,          # "BUY" | "SELL"
        quantity: Decimal,  # exchange-quantized by infra
        price: Decimal,     # exchange-quantized by infra
        reduce_only: bool = False,
        time_in_force: str = "GTC",
        new_client_order_id: Optional[str] = None,
    ) -> dict: ...
    # (We don't query status here; that’s handled by websocket/order manager.)


class PositionManagerPort(Protocol):
    """We may notify PositionManager when order fills via ws; nothing needed synchronously here."""
    # kept for future extension


# --------- Use Case ---------

class OrderExecutor:
    """
    Creates a limit order after validating balance and computing size.
    The infra layer must handle symbol filters (stepSize, tickSize) and rounding.
    """

    def __init__(
        self,
        balance_validator: BalanceValidatorPort,
        binance_client: BinanceClient,
        position_manager: PositionManagerPort | None = None,
        *,
        tp_r_multiple: Decimal = Decimal("1.5"),
    ) -> None:
        self._bal = balance_validator
        self._bx = binance_client
        self._pm = position_manager
        self._tp_r = tp_r_multiple

    async def execute_order(
        self,
        bot: BotConfig,
        signal: ArmSignal,
    ) -> OrderState:
        # --- 1) Compute intended notional and margin, then validate balance
        available = await self._bal.get_available_balance(bot.user_id, bot.env)
        qty = self._calculate_position_size(bot, available, signal.trigger)
        if qty <= 0:
            st = OrderState(
                bot_id=bot.id,
                signal_id="",  # set by SignalProcessor
                status=OrderStatus.FAILED,
                side=signal.side,
                symbol=signal.symbol,
                trigger_price=signal.trigger,
                stop_price=signal.stop,
                quantity=Decimal("0"),
            )
            return st

        intended_notional = qty * signal.trigger
        required_margin = intended_notional / Decimal(bot.leverage if bot.leverage > 0 else 1)

        is_ok, avail_check = await self._bal.validate_balance(bot, required_margin)
        if not is_ok:
            st = OrderState(
                bot_id=bot.id,
                signal_id="",  # set by SignalProcessor
                status=OrderStatus.SKIPPED_LOW_BALANCE,
                side=signal.side,
                symbol=signal.symbol,
                trigger_price=signal.trigger,
                stop_price=signal.stop,
                quantity=Decimal("0"),
            )
            return st

        # --- 2) Set leverage (idempotent in infra)
        await self._bx.set_leverage(signal.symbol, bot.leverage)

        # --- 3) Place limit order
        side_str = "BUY" if signal.side == Side.LONG else "SELL"
        try:
            resp = await self._bx.create_limit_order(
                symbol=signal.symbol,
                side=side_str,
                quantity=qty,
                price=signal.trigger,
                reduce_only=False,
                time_in_force="GTC",
            )
        except Exception as exc:  # infra should raise clearer errors; we normalize
            raise BinanceAPIException(f"create_limit_order failed: {exc}") from exc

        order_id = resp.get("orderId")  # UMFutures returns int
        state = OrderState(
            bot_id=bot.id,
            signal_id="",  # SignalProcessor will inject the stream msg id
            status=OrderStatus.PENDING,
            side=signal.side,
            symbol=signal.symbol,
            trigger_price=signal.trigger,
            stop_price=signal.stop,
            quantity=qty,
            order_id=order_id,
        )
        return state

    # ----- sizing -----

    def _calculate_position_size(
        self,
        bot: BotConfig,
        available_balance: Decimal,
        entry_price: Decimal,
    ) -> Decimal:
        """
        Priority:
          1) fixed_notional if provided (>0)
          2) else if use_balance_pct: notional = available_balance * balance_pct
          3) else: notional = available_balance * 0 (skip)
        Then cap by max_position_usdt if provided.
        Convert notional -> qty using entry_price and leverage.
        """
        notional = Decimal("0")

        if bot.fixed_notional and bot.fixed_notional > 0:
            notional = bot.fixed_notional
        elif bot.use_balance_pct:
            pct = bot.balance_pct if bot.balance_pct > 0 else Decimal("0")
            notional = available_balance * pct
        else:
            return Decimal("0")

        # Cap by risk limit
        if bot.max_position_usdt and bot.max_position_usdt > 0:
            if notional > bot.max_position_usdt:
                notional = bot.max_position_usdt

        if entry_price <= 0:
            return Decimal("0")

        # Futures margin amplifies exposure by leverage; position notional = notional * leverage?
        # Convention: 'notional' above is *exposure* budget; margin = exposure / leverage.
        exposure = notional
        qty = exposure / entry_price
        # (Infra will quantize to stepSize/tickSize; keep raw here)
        return qty if qty > 0 else Decimal("0")
