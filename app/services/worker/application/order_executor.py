from __future__ import annotations

from decimal import Decimal
from typing import Awaitable, Callable, Optional, Protocol

from ..domain.models import ArmSignal, BotConfig, OrderState
from ..domain.enums import OrderStatus, Side
from ..domain.exceptions import BinanceAPIException


# --------- Ports (to be implemented by Infra) ---------

class BalanceValidatorPort(Protocol):
    async def validate_balance(self, bot: BotConfig, required_margin: Decimal) -> tuple[bool, Decimal]: ...
    async def get_available_balance(self, user_id, env: str) -> Decimal: ...


class TradingPort(Protocol):
    """Subset of infrastructure/binance/trading.BinanceTrading we need."""
    async def set_leverage(self, symbol: str, leverage: int) -> None: ...
    async def create_limit_order(
        self,
        symbol: str,
        side: Side,              # domain enum
        quantity: Decimal,       # infra will quantize/round
        price: Decimal,          # infra will quantize/round
        reduce_only: bool = False,
        time_in_force: str = "GTC",
        new_client_order_id: Optional[str] = None,
    ) -> dict: ...


class PositionManagerPort(Protocol):
    """Reserved for future fill/position notifications from WS."""
    ...


# --------- Use Case ---------

class OrderExecutor:
    """
    Creates a limit order after validating balance and computing size.

    Notes:
    - Position sizing stays here (domain logic).
    - Exchange-specific rounding/tick/step filtering happens in infra TradingPort.
    """

    def __init__(
        self,
        balance_validator: BalanceValidatorPort,
        binance_client: Optional[TradingPort] = None,
        position_manager: Optional[PositionManagerPort] = None,
        *,
        tp_r_multiple: Decimal = Decimal("1.5"),
        trading_factory: Optional[Callable[[BotConfig], Awaitable[TradingPort]]] = None,
    ) -> None:
        self._bal = balance_validator
        self._bx = binance_client
        self._pm = position_manager
        self._tp_r = tp_r_multiple
        self.trading_factory = trading_factory  # may be set/overridden from main.py

    async def _get_trading(self, bot: BotConfig) -> TradingPort:
        if self._bx is not None:
            return self._bx
        if self.trading_factory is None:
            raise RuntimeError("OrderExecutor.trading_factory not configured and no binance_client provided")
        return await self.trading_factory(bot)

    async def execute_order(
        self,
        bot: BotConfig,
        signal: ArmSignal,
    ) -> OrderState:
        # --- 1) Compute intended size using *available* balance
        available = await self._bal.get_available_balance(bot.user_id, bot.env)
        qty = self._calculate_position_size(bot, available, signal.trigger)
        if qty <= 0:
            return OrderState(
                bot_id=bot.id,
                signal_id="",
                status=OrderStatus.FAILED,
                side=signal.side,
                symbol=signal.symbol,
                trigger_price=signal.trigger,
                stop_price=signal.stop,
                quantity=Decimal("0"),
            )

        exposure = qty * signal.trigger
        required_margin = exposure / Decimal(bot.leverage if bot.leverage > 0 else 1)

        is_ok, _avail_check = await self._bal.validate_balance(bot, required_margin)
        if not is_ok:
            return OrderState(
                bot_id=bot.id,
                signal_id="",
                status=OrderStatus.SKIPPED_LOW_BALANCE,
                side=signal.side,
                symbol=signal.symbol,
                trigger_price=signal.trigger,
                stop_price=signal.stop,
                quantity=Decimal("0"),
            )

        # --- 2) Get trading port (from factory or prewired), set leverage idempotently
        trading = await self._get_trading(bot)
        await trading.set_leverage(signal.symbol, bot.leverage)

        # --- 3) Place limit order (infra handles precision/filters)
        try:
            resp = await trading.create_limit_order(
                symbol=signal.symbol,
                side=signal.side,          # pass domain enum; infra maps to BUY/SELL
                quantity=qty,
                price=signal.trigger,
                reduce_only=False,
                time_in_force="GTC",
            )
        except Exception as exc:
            raise BinanceAPIException(f"create_limit_order failed: {exc}") from exc

        order_id = resp.get("orderId")
        return OrderState(
            bot_id=bot.id,
            signal_id="",  # SignalProcessor fills with Redis message id
            status=OrderStatus.PENDING,
            side=signal.side,
            symbol=signal.symbol,
            trigger_price=signal.trigger,
            stop_price=signal.stop,
            quantity=qty,
            order_id=order_id,
        )

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
          3) else: notional = 0 (skip)
        Cap by max_position_usdt if provided.
        Convert notional -> qty using entry_price (exposure = notional).
        """
        notional = Decimal("0")

        if bot.fixed_notional and bot.fixed_notional > 0:
            notional = bot.fixed_notional
        elif bot.use_balance_pct:
            pct = bot.balance_pct if bot.balance_pct and bot.balance_pct > 0 else Decimal("0")
            notional = available_balance * pct
        else:
            return Decimal("0")

        if bot.max_position_usdt and bot.max_position_usdt > 0 and notional > bot.max_position_usdt:
            notional = bot.max_position_usdt

        if entry_price <= 0:
            return Decimal("0")

        qty = notional / entry_price
        return qty if qty > 0 else Decimal("0")
