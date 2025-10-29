from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation
from typing import Awaitable, Callable, Optional, Protocol
from uuid import UUID

from ..domain.models import ArmSignal, BotConfig, OrderState
from ..domain.enums import OrderStatus, OrderSide
from ..domain.exceptions import BinanceAPIException, DomainExchangeError

log = logging.getLogger(__name__)

# --------- Ports (to be implemented by Infra) ---------

class BalanceValidatorPort(Protocol):
    async def validate_balance(self, bot: BotConfig, required_margin: Decimal) -> tuple[bool, Decimal]: ...
    async def get_available_balance(self, cred_id: UUID, env: str) -> Decimal: ...


class TradingPort(Protocol):
    """Subset of infrastructure/binance/trading.BinanceTrading we need."""
    async def set_leverage(self, symbol: str, leverage: int) -> None: ...
    async def quantize_limit_order(
        self,
        symbol: str,
        quantity: Decimal,
        price: Decimal,
    ) -> tuple[Decimal, Decimal | None]: ...
    async def create_limit_order(
        self,
        symbol: str,
        side: OrderSide,        # domain enum (infra maps to BUY/SELL)
        quantity: Decimal,
        price: Decimal,
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
        # --- 0) Validate inputs defensively
        if signal.trigger <= 0:
            log.error("Invalid trigger price <= 0 | symbol=%s trigger=%s", signal.symbol, signal.trigger)
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

        # --- 1) Fetch available balance (authoritative, cached by BalanceValidator)
        available = await self._bal.get_available_balance(bot.cred_id, bot.env)

        # --- 2) Compute intended size
        qty = self._calculate_position_size(bot, available, signal.trigger)
        if qty <= 0:
            log.info(
                "[sizing] zero/negative qty -> skip | bot=%s sym=%s available=%s trigger=%s cfg: (use_pct=%s pct=%s fixed=%s max_usdt=%s)",
                str(bot.id), signal.symbol, str(available), str(signal.trigger),
                bot.use_balance_pct, str(bot.balance_pct), str(bot.fixed_notional), str(bot.max_position_usdt),
            )
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

        # Required margin = exposure / leverage
        leverage = bot.leverage if bot.leverage and bot.leverage > 0 else 1
        try:
            exposure = qty * signal.trigger
            required_margin = exposure / Decimal(leverage)
        except (InvalidOperation, ZeroDivisionError):
            log.exception("Required margin calculation failed | qty=%s trigger=%s leverage=%s", qty, signal.trigger, leverage)
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

        log.info(
            "[sizing] qty=%s price=%s exposure=%s lev=%s required_margin=%s",
            str(qty), str(signal.trigger), str(exposure), leverage, str(required_margin),
        )

        # --- 3) Validate balance (uses same source as get_available_balance under the hood)
        is_ok, avail_check = await self._bal.validate_balance(bot, required_margin)
        log.info("[balance] available=%s required=%s pass=%s", str(avail_check), str(required_margin), is_ok)
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

        # --- 4) Get trading port (from factory or prewired), set leverage idempotently
        trading = await self._get_trading(bot)
        try:
            await trading.set_leverage(signal.symbol, bot.leverage)
        except Exception as exc:
            log.exception("set_leverage failed | sym=%s lev=%s", signal.symbol, bot.leverage)
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

        # --- 5) Quantize order to exchange constraints
        q_qty, q_price = await trading.quantize_limit_order(signal.symbol, qty, signal.trigger)
        if q_qty <= 0 or q_price is None or q_price <= 0:
            log.error(
                "Quantization invalid | sym=%s raw_qty=%s raw_price=%s -> q_qty=%s q_price=%s",
                signal.symbol, str(qty), str(signal.trigger), str(q_qty), str(q_price),
            )
            return OrderState(
                bot_id=bot.id,
                signal_id="",
                status=OrderStatus.FAILED,
                side=signal.side,
                symbol=signal.symbol,
                trigger_price=q_price or Decimal("0"),
                stop_price=signal.stop,
                quantity=Decimal("0"),
            )

        # --- 6) Place limit order (infra handles retries/error mapping)
        try:
            resp = await trading.create_limit_order(
                symbol=signal.symbol,
                side=signal.side,      # pass OrderSide; infra maps to BUY/SELL
                quantity=q_qty,
                price=q_price,
                reduce_only=False,
                time_in_force="GTC",
            )
        except DomainExchangeError as exc:
            diagnostics = (
                f"create_limit_order failed: {exc} | symbol={signal.symbol} "
                f"raw_qty={qty} q_qty={q_qty} q_price={q_price}"
            )
            log.error(diagnostics)
            raise BinanceAPIException(diagnostics) from exc
        except Exception as exc:
            log.exception("create_limit_order unexpected error | sym=%s", signal.symbol)
            raise BinanceAPIException(f"create_limit_order failed: {exc}") from exc

        order_id = resp.get("orderId")
        log.info(
            "Order placed | sym=%s side=%s qty=%s price=%s order_id=%s",
            signal.symbol, signal.side.value if hasattr(signal.side, "value") else str(signal.side),
            str(q_qty), str(q_price), str(order_id),
        )
        return OrderState(
            bot_id=bot.id,
            signal_id="",  # SignalProcessor fills with Redis message id
            status=OrderStatus.PENDING,
            side=signal.side,
            symbol=signal.symbol,
            trigger_price=q_price,
            stop_price=signal.stop,
            quantity=q_qty,
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
        Cap by max_position_usdt if provided (> 0).
        Convert notional -> qty using entry_price (exposure = notional).
        """
        if entry_price <= 0:
            return Decimal("0")

        notional = Decimal("0")

        if bot.fixed_notional and bot.fixed_notional > 0:
            notional = bot.fixed_notional
        elif bot.use_balance_pct:
            pct = bot.balance_pct if bot.balance_pct and bot.balance_pct > 0 else Decimal("0")
            # clamp absurd pct values defensively (e.g., >1.0)
            if pct > Decimal("1"):
                pct = Decimal("1")
            notional = available_balance * pct
        else:
            return Decimal("0")

        # apply cap only if it's a positive value (0 or None means "no cap")
        if bot.max_position_usdt and bot.max_position_usdt > 0 and notional > bot.max_position_usdt:
            notional = bot.max_position_usdt

        qty = notional / entry_price
        return qty if qty > 0 else Decimal("0")
