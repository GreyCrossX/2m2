from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation, ROUND_FLOOR, ROUND_CEILING
from typing import Awaitable, Callable, Optional, Protocol
from uuid import UUID

from ..domain.models import ArmSignal, BotConfig, OrderState
from ..domain.enums import OrderStatus, OrderSide, exit_side_for
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

    async def create_stop_market_order(
        self,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        stop_price: Decimal,
        *,
        reduce_only: bool = True,
        order_type: str = "STOP_MARKET",
    ) -> dict: ...

    # explicit TP-LIMIT order
    async def create_take_profit_limit(
        self,
        symbol: str,
        side: OrderSide,        # opposite of entry
        quantity: Decimal,
        price: Decimal,         # limit price
        stop_price: Decimal,    # TP trigger; for TP-LIMIT it's normally equal to price
        reduce_only: bool = True,
        time_in_force: str = "GTC",
        new_client_order_id: Optional[str] = None,
    ) -> dict: ...

    # NEW: filters for minNotional/LOT_SIZE cache
    async def get_symbol_filters(self, symbol: str) -> dict: ...

    async def cancel_order(self, symbol: str, order_id: int) -> None: ...


class PositionManagerPort(Protocol):
    """Reserved for future fill/position notifications from WS."""
    ...


# --------- Use Case ---------

Q_STEP = Decimal("0.001")  # hard 3-decimal precision for qty (per requirement)
NOTIONAL_FALLBACK = Decimal("20")  # safety default if filters missing

def _floor3(x: Decimal) -> Decimal:
    return (x / Q_STEP).to_integral_value(rounding=ROUND_FLOOR) * Q_STEP

def _ceil3(x: Decimal) -> Decimal:
    return (x / Q_STEP).to_integral_value(rounding=ROUND_CEILING) * Q_STEP


class OrderExecutor:
    """
    Creates a limit order after validating balance and computing size,
    then places a TAKE_PROFIT_LIMIT reduce-only order.

    Notes:
    - Position sizing stays here (domain logic).
    - Exchange-specific rounding/tick/step filtering happens in infra TradingPort.
    - We also perform a pre-flight min-notional bump (3 dp) and re-validate margin.
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

    # --- TP math ---
    def _compute_tp_price(self, side: OrderSide, trigger: Decimal, stop: Decimal) -> Decimal:
        """
        TP distance = tp_r_multiple × |trigger - stop|.
        - LONG:  TP = trigger + (trigger - stop) * R
        - SHORT: TP = trigger - (stop - trigger) * R
        """
        r = self._tp_r
        if side == OrderSide.LONG:
            distance = (trigger - stop) * r
            return trigger + distance
        else:
            distance = (stop - trigger) * r
            return trigger - distance

    async def _min_notional_for(self, trading: TradingPort, symbol: str) -> Decimal:
        f = await trading.get_symbol_filters(symbol)
        mn = (f.get("MIN_NOTIONAL") or f.get("NOTIONAL") or {})
        mn_val = mn.get("notional") or mn.get("minNotional") or mn.get("minNotionalValue")
        try:
            return Decimal(str(mn_val)) if mn_val not in (None, "", "0") else NOTIONAL_FALLBACK
        except Exception:
            return NOTIONAL_FALLBACK

    async def _preflight_qty(
        self,
        trading: TradingPort,
        symbol: str,
        raw_qty: Decimal,
        price: Decimal,
        lev: Decimal,
        balance_free: Decimal,
    ) -> tuple[bool, Decimal, str | None]:
        """
        Enforce 3dp qty and min-notional; re-check margin after bump.
        Returns (ok, q_qty, reason_if_not_ok)
        """
        q_qty = _floor3(raw_qty)
        min_notional = await self._min_notional_for(trading, symbol)
        notional = q_qty * price
        if notional < min_notional:
            min_qty = _ceil3(min_notional / price)
            q_qty = min_qty

        required_margin = (q_qty * price) / lev
        if required_margin > balance_free:
            return (False, q_qty, f"insufficient_margin_after_min_notional(required={required_margin}, free={balance_free})")

        if q_qty <= 0:
            return (False, q_qty, "qty_zero_after_round")

        log.info(
            "[preflight] sym=%s raw_qty=%s q_qty=%s price=%s min_notional=%s notional=%s lev=%s req_margin=%s free=%s",
            symbol, str(raw_qty), str(q_qty), str(price), str(min_notional),
            str(q_qty * price), str(lev), str(required_margin), str(balance_free),
        )
        return (True, q_qty, None)

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

        # --- 3) Validate balance
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

        # --- 4) Get trading port, set leverage idempotently
        trading = await self._get_trading(bot)
        try:
            await trading.set_leverage(signal.symbol, bot.leverage)
        except Exception:
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

        # --- 5) Pre-flight: enforce 3dp & min-notional bump, re-check margin
        balance_free = await self._bal.get_available_balance(bot.cred_id, bot.env)
        lev_d = Decimal(str(leverage))
        ok_pf, q_qty_pf, reason_pf = await self._preflight_qty(
            trading, signal.symbol, qty, signal.trigger, lev_d, balance_free
        )
        if not ok_pf:
            log.info("Preflight failed | sym=%s reason=%s", signal.symbol, reason_pf)
            status = OrderStatus.SKIPPED_LOW_BALANCE if "insufficient_margin" in (reason_pf or "") else OrderStatus.FAILED
            return OrderState(
                bot_id=bot.id,
                signal_id="",
                status=status,
                side=signal.side,
                symbol=signal.symbol,
                trigger_price=signal.trigger,
                stop_price=signal.stop,
                quantity=Decimal("0"),
            )

        # --- 6) Quantize ENTRY (exchange ticks), price may adjust; qty should still meet notional
        q_qty, q_price = await trading.quantize_limit_order(signal.symbol, q_qty_pf, signal.trigger)
        if q_qty <= 0 or q_price is None or q_price <= 0:
            log.error(
                "Quantization invalid | sym=%s raw_qty=%s pf_qty=%s raw_price=%s -> q_qty=%s q_price=%s",
                signal.symbol, str(qty), str(q_qty_pf), str(signal.trigger), str(q_qty), str(q_price),
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

        # --- 7) Place ENTRY limit order
        try:
            resp = await trading.create_limit_order(
                symbol=signal.symbol,
                side=signal.side,      # domain enum; infra maps to BUY/SELL
                quantity=q_qty,
                price=q_price,
                reduce_only=False,
                time_in_force="GTC",
            )
        except DomainExchangeError as exc:
            diagnostics = (
                f"create_limit_order failed: {exc} | symbol={signal.symbol} "
                f"raw_qty={qty} pf_qty={q_qty_pf} q_qty={q_qty} q_price={q_price}"
            )
            log.error(diagnostics)
            # IMPORTANT: no dangling PENDING created here; caller should persist FAILED if needed
            raise BinanceAPIException(diagnostics) from exc
        except Exception as exc:
            log.exception("create_limit_order unexpected error | sym=%s", signal.symbol)
            raise BinanceAPIException(f"create_limit_order failed: {exc}") from exc

        order_id = resp.get("orderId")
        log.info(
            "Order placed | sym=%s side=%s qty=%s price=%s order_id=%s",
            signal.symbol, getattr(signal.side, "value", str(signal.side)),
            str(q_qty), str(q_price), str(order_id),
        )

        stop_order_id: Optional[int] = None
        take_profit_order_id: Optional[int] = None

        # --- 7.1) Place STOP MARKET reduce-only immediately
        stop_side = exit_side_for(signal.side)
        try:
            stop_resp = await trading.create_stop_market_order(
                symbol=signal.symbol,
                side=stop_side,
                quantity=q_qty,
                stop_price=signal.stop,
                reduce_only=True,
                order_type="STOP_MARKET",
            )
            stop_order_id = stop_resp.get("orderId")
            log.info(
                "Stop-loss placed | sym=%s side=%s qty=%s stop=%s stop_order_id=%s",
                signal.symbol,
                getattr(stop_side, "value", str(stop_side)),
                str(q_qty),
                str(signal.stop),
                str(stop_order_id),
            )
        except Exception as exc:
            log.error("Stop-loss placement failed, cancelling entry | sym=%s err=%s", signal.symbol, exc)
            if order_id:
                try:
                    await trading.cancel_order(symbol=signal.symbol, order_id=int(order_id))
                except Exception:
                    log.exception(
                        "Failed to cancel entry after stop-loss error | sym=%s order_id=%s",
                        signal.symbol,
                        order_id,
                    )
            raise BinanceAPIException(f"create_stop_market_order failed: {exc}") from exc

        # --- 7.2) Place TAKE_PROFIT_LIMIT as reduce-only (required)
        try:
            tp_target = self._compute_tp_price(signal.side, signal.trigger, signal.stop)
            # Re-quantize price only; qty stays the same
            _, q_tp_price = await trading.quantize_limit_order(signal.symbol, q_qty, tp_target)
            if q_tp_price is None or q_tp_price <= 0:
                raise BinanceAPIException("Quantized TP price invalid")

            tp_side = exit_side_for(signal.side)

            tp_resp = await trading.create_take_profit_limit(
                symbol=signal.symbol,
                side=tp_side,
                quantity=q_qty,
                price=q_tp_price,
                stop_price=q_tp_price,   # TP-LIMIT: stop == price
                reduce_only=True,
                time_in_force="GTC",
            )
            take_profit_order_id = tp_resp.get("orderId")
            log.info(
                "TP placed | sym=%s side=%s qty=%s price=%s tp_order_id=%s",
                signal.symbol,
                getattr(tp_side, "value", str(tp_side)),
                str(q_qty),
                str(q_tp_price),
                str(take_profit_order_id),
            )
        except Exception as exc:
            log.error("Take-profit placement failed, rolling back entry | sym=%s err=%s", signal.symbol, exc)
            # Attempt to cancel protective stop first, then entry
            if stop_order_id:
                try:
                    await trading.cancel_order(symbol=signal.symbol, order_id=int(stop_order_id))
                except Exception:
                    log.exception(
                        "Failed to cancel stop-loss during rollback | sym=%s stop_order_id=%s",
                        signal.symbol,
                        stop_order_id,
                    )
            if order_id:
                try:
                    await trading.cancel_order(symbol=signal.symbol, order_id=int(order_id))
                except Exception:
                    log.exception(
                        "Failed to cancel entry during rollback | sym=%s order_id=%s",
                        signal.symbol,
                        order_id,
                    )
            raise BinanceAPIException(f"create_take_profit_limit failed: {exc}") from exc

        # --- 8) Return ORDER STATE (entry)
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
            stop_order_id=stop_order_id,
            take_profit_order_id=take_profit_order_id,
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
            if pct > Decimal("1"):
                pct = Decimal("1")
            notional = available_balance * pct
        else:
            return Decimal("0")

        if bot.max_position_usdt and bot.max_position_usdt > 0 and notional > bot.max_position_usdt:
            notional = bot.max_position_usdt

        qty = notional / entry_price
        return qty if qty > 0 else Decimal("0")
