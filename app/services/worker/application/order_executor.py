from __future__ import annotations

import logging
import secrets
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from typing import Awaitable, Callable, Optional, Protocol
from uuid import UUID

from app.services.order_placement import OrderPlacementService, TrioOrderResult
from ..domain.models import ArmSignal, BotConfig, OrderState
from ..domain.enums import OrderStatus, OrderSide
from ..domain.exceptions import (
    BinanceAPIException,
    BinanceBadRequestException,
    BinanceExchangeDownException,
    BinanceRateLimitException,
)
from ..infrastructure.metrics import WorkerMetrics

log = logging.getLogger(__name__)

# --------- Ports (to be implemented by Infra) ---------


class BalanceValidatorPort(Protocol):
    async def validate_balance(
        self,
        bot: BotConfig,
        required_margin: Decimal,
        *,
        available_balance: Decimal | None = None,
    ) -> tuple[bool, Decimal]: ...

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
        side: OrderSide,  # domain enum (infra maps to BUY/SELL)
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
        time_in_force: str | None = None,
        new_client_order_id: Optional[str] = None,
    ) -> dict: ...

    # explicit TP-LIMIT order
    async def create_take_profit_limit(
        self,
        symbol: str,
        side: OrderSide,  # opposite of entry
        quantity: Decimal,
        price: Decimal,  # limit price
        stop_price: Decimal,  # TP trigger; for TP-LIMIT it's normally equal to price
        reduce_only: bool = True,
        time_in_force: str = "GTC",
        new_client_order_id: Optional[str] = None,
    ) -> dict: ...

    # NEW: filters for minNotional/LOT_SIZE cache
    async def get_symbol_filters(self, symbol: str) -> dict: ...

    async def cancel_order(self, symbol: str, order_id: int) -> None: ...

    async def list_open_orders(self, symbol: str | None = None) -> list[dict]: ...


class PositionManagerPort(Protocol):
    """Reserved for future fill/position notifications from WS."""

    ...


# --------- Use Case ---------

NOTIONAL_FALLBACK = Decimal("20")  # safety default if filters missing
_CLIENT_ID_MAX_LEN = 36
_CLIENT_ID_PREFIX_LEN = 20
_MIN_STOP_DIST_RATIO = Decimal("0.0005")  # 5 bps guard to avoid ultra-tight stops
_MARK_DRIFT_RATIO = Decimal("0.0015")  # 15 bps guard vs current mark/last


def _bot_client_prefix(bot_id: UUID) -> str:
    compact = str(bot_id).replace("-", "")
    return f"b{compact[:_CLIENT_ID_PREFIX_LEN]}"


def _bot_exit_client_id(bot_id: UUID, label: str) -> str:
    """
    Generate a short newClientOrderId tagged to the bot for TP/SL legs.
    Binance futures allows up to 36 chars; we keep a stable bot prefix to
    make filtering/cancellation cheap.
    """
    rand = secrets.token_hex(3)  # 6 chars of entropy
    base = f"{_bot_client_prefix(bot_id)}-{label}-{rand}"
    return base[:_CLIENT_ID_MAX_LEN]


def _to_decimal(value: object, *, default: Decimal = Decimal("0")) -> Decimal:
    try:
        if value in (None, ""):
            return default
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return default


def _quantize_by_precision(value: Decimal, precision: int, rounding) -> Decimal:
    precision = max(0, precision)
    quantum = Decimal(1).scaleb(-precision)
    return value.quantize(quantum, rounding=rounding)


def _floor_to_step(value: Decimal, step: Decimal, precision: int | None) -> Decimal:
    if value <= 0:
        return Decimal("0")
    if step > 0:
        multiples = (value / step).to_integral_value(rounding=ROUND_FLOOR)
        floored = multiples * step
        return floored.quantize(step, rounding=ROUND_FLOOR)
    if precision is not None:
        try:
            return _quantize_by_precision(value, precision, ROUND_FLOOR)
        except (InvalidOperation, ValueError):
            return _quantize_by_precision(value, 6, ROUND_FLOOR)
    return value.quantize(Decimal("0.000001"), rounding=ROUND_FLOOR)


def _ceil_to_step(value: Decimal, step: Decimal, precision: int | None) -> Decimal:
    if value <= 0:
        return Decimal("0")
    if step > 0:
        multiples = (value / step).to_integral_value(rounding=ROUND_CEILING)
        ceiled = multiples * step
        return ceiled.quantize(step, rounding=ROUND_CEILING)
    if precision is not None:
        try:
            return _quantize_by_precision(value, precision, ROUND_CEILING)
        except (InvalidOperation, ValueError):
            return _quantize_by_precision(value, 6, ROUND_CEILING)
    return value.quantize(Decimal("0.000001"), rounding=ROUND_CEILING)


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
        metrics: WorkerMetrics | None = None,
    ) -> None:
        self._bal = balance_validator
        self._bx = binance_client
        self._pm = position_manager
        self._tp_r = tp_r_multiple
        self.trading_factory = trading_factory  # may be set/overridden from main.py
        self._metrics = metrics

    def _log_context(self, bot: BotConfig, signal: ArmSignal) -> dict[str, str]:
        return {
            "bot_id": str(bot.id),
            "symbol": signal.symbol,
            "signal_id": signal.signal_msg_id or "",
        }

    def _context_str(self, context: dict[str, str]) -> str:
        return " ".join(f"{k}={v}" for k, v in context.items() if v)

    def _failure_state(
        self,
        bot: BotConfig,
        signal: ArmSignal,
        *,
        status: OrderStatus = OrderStatus.FAILED,
        quantity: Decimal | None = None,
        trigger_price: Decimal | None = None,
    ) -> OrderState:
        qty = quantity if quantity and quantity > 0 else Decimal("0")
        trig = trigger_price if trigger_price and trigger_price > 0 else signal.trigger
        return OrderState(
            bot_id=bot.id,
            signal_id="",
            status=status,
            side=signal.side,
            symbol=signal.symbol,
            trigger_price=trig,
            stop_price=signal.stop,
            quantity=qty,
            filled_quantity=Decimal("0"),
        )

    async def _place_order_trio(
        self,
        trading: TradingPort,
        bot: BotConfig,
        signal: ArmSignal,
        *,
        quantity: Decimal,
        entry_price: Decimal,
        stop_price: Decimal,
        take_profit_price: Decimal,
    ) -> TrioOrderResult:
        context = self._log_context(bot, signal)
        placement = OrderPlacementService(trading, logger=log)
        stop_client_id = _bot_exit_client_id(bot.id, "sl")
        tp_client_id = _bot_exit_client_id(bot.id, "tp")
        return await placement.place_trio_orders(
            symbol=signal.symbol,
            side=signal.side,
            quantity=quantity,
            entry_price=entry_price,
            stop_price=stop_price,
            take_profit_price=take_profit_price,
            reduce_only=False,
            context=context,
            stop_client_order_id=stop_client_id,
            tp_client_order_id=tp_client_id,
        )

    async def _current_mark_or_last(self, trading: TradingPort, symbol: str) -> Decimal:
        """
        Fetch current mark/last price to guard against stale signals.
        TradingPort may optionally provide get_mark_price; otherwise, quantize a 1qty limit to approximate.
        """
        for attr in ("get_mark_price", "get_ticker_price", "get_price"):
            getter = getattr(trading, attr, None)
            if callable(getter):
                try:
                    price = await getter(symbol)
                    return Decimal(str(price))
                except Exception:
                    log.warning("Failed to fetch price via %s for %s", attr, symbol)
                    continue
        return Decimal("0")

    async def _cleanup_bot_exit_orders(
        self, trading: TradingPort, bot: BotConfig
    ) -> int:
        """
        Cancel any open TP/SL orders that were tagged to this bot via clientOrderId.
        This guards against lingering exits before we place a new trio.
        """
        prefix = _bot_client_prefix(bot.id)
        try:
            open_orders = await trading.list_open_orders(bot.symbol)
        except Exception as exc:
            log.warning(
                "Exit cleanup skipped (list_open_orders failed) | bot_id=%s err=%s",
                bot.id,
                exc,
            )
            return 0

        cancelled = 0
        for order in open_orders:
            client_id = str(
                order.get("client_order_id") or order.get("clientOrderId") or ""
            )
            if not client_id.startswith(prefix):
                continue
            order_id = order.get("order_id") or order.get("orderId")
            if order_id in (None, ""):
                continue
            try:
                await trading.cancel_order(symbol=bot.symbol, order_id=int(order_id))
                cancelled += 1
                log.info(
                    "Cancelled lingering exit order | bot_id=%s symbol=%s order_id=%s client_id=%s",
                    bot.id,
                    bot.symbol,
                    order_id,
                    client_id,
                )
            except Exception as exc:
                log.warning(
                    "Failed to cancel lingering exit order | bot_id=%s symbol=%s order_id=%s err=%s",
                    bot.id,
                    bot.symbol,
                    order_id,
                    exc,
                )

        if cancelled:
            log.info(
                "Exit cleanup complete | bot_id=%s cancelled=%d", bot.id, cancelled
            )
        return cancelled

    async def _get_trading(self, bot: BotConfig) -> TradingPort:
        if self._bx is not None:
            return self._bx
        if self.trading_factory is None:
            raise RuntimeError(
                "OrderExecutor.trading_factory not configured and no binance_client provided"
            )
        return await self.trading_factory(bot)

    # --- TP math ---
    def _compute_tp_price(
        self,
        side: OrderSide,
        trigger: Decimal,
        stop: Decimal,
        *,
        tp_r_multiple: Decimal | None = None,
    ) -> Decimal:
        """
        TP distance = tp_r_multiple × |trigger - stop|.
        The absolute distance guards against malformed signals where the stop
        may be on the wrong side of the trigger. In those cases we still widen
        the take-profit away from the entry rather than inverting the RR.
        - LONG:  TP = trigger + distance * R
        - SHORT: TP = trigger - distance * R
        """
        distance = abs(trigger - stop)
        r = tp_r_multiple if tp_r_multiple is not None else self._tp_r
        if side == OrderSide.LONG:
            return trigger + (distance * r)
        return trigger - (distance * r)

    async def _min_notional_for(
        self,
        trading: TradingPort,
        symbol: str,
        *,
        filters: dict | None = None,
    ) -> Decimal:
        f = filters or await trading.get_symbol_filters(symbol)
        mn = f.get("MIN_NOTIONAL") or f.get("NOTIONAL") or {}
        mn_val = (
            mn.get("notional") or mn.get("minNotional") or mn.get("minNotionalValue")
        )
        try:
            return (
                Decimal(str(mn_val))
                if mn_val not in (None, "", "0")
                else NOTIONAL_FALLBACK
            )
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
        filters = await trading.get_symbol_filters(symbol)
        lot = filters.get("LOT_SIZE", {}) or {}
        step = _to_decimal(lot.get("stepSize"))
        min_qty = _to_decimal(lot.get("minQty"))
        max_qty = _to_decimal(lot.get("maxQty"))
        meta = filters.get("META", {}) or {}
        qty_precision: int | None = None
        qp_raw = meta.get("quantityPrecision")
        if qp_raw is not None:
            try:
                qty_precision = int(qp_raw)
            except (TypeError, ValueError):
                qty_precision = None

        q_qty = _floor_to_step(raw_qty, step, qty_precision)

        if min_qty > 0 and (q_qty <= 0 or q_qty < min_qty):
            q_qty = _ceil_to_step(min_qty, step, qty_precision)

        if max_qty > 0 and q_qty > max_qty:
            q_qty = _floor_to_step(max_qty, step, qty_precision)

        min_notional = await self._min_notional_for(trading, symbol, filters=filters)
        notional = q_qty * price
        if notional < min_notional:
            needed_qty = _ceil_to_step(min_notional / price, step, qty_precision)
            q_qty = needed_qty

        if min_qty > 0 and q_qty < min_qty:
            q_qty = _ceil_to_step(min_qty, step, qty_precision)

        if max_qty > 0 and q_qty > max_qty:
            q_qty = _floor_to_step(max_qty, step, qty_precision)

        final_notional = q_qty * price
        if final_notional < min_notional:
            return (
                False,
                q_qty,
                f"min_notional_not_met(required={min_notional}, actual={final_notional})",
            )

        required_margin = final_notional / lev
        if required_margin > balance_free:
            return (
                False,
                q_qty,
                f"insufficient_margin_after_min_notional(required={required_margin}, free={balance_free})",
            )

        if q_qty <= 0:
            return (False, q_qty, "qty_zero_after_round")

        log.debug(
            "[preflight] sym=%s raw_qty=%s q_qty=%s price=%s min_notional=%s notional=%s lev=%s req_margin=%s free=%s",
            symbol,
            str(raw_qty),
            str(q_qty),
            str(price),
            str(min_notional),
            str(q_qty * price),
            str(lev),
            str(required_margin),
            str(balance_free),
        )
        return (True, q_qty, None)

    async def execute_order(
        self,
        bot: BotConfig,
        signal: ArmSignal,
    ) -> OrderState:
        context = self._log_context(bot, signal)
        ctx_str = self._context_str(context)

        # --- 0) Validate inputs defensively
        if signal.trigger <= 0:
            log.error("Invalid trigger price <= 0 | %s", ctx_str)
            return self._failure_state(bot, signal)

        # --- 1) Fetch available balance (authoritative, cached by BalanceValidator)
        available = await self._bal.get_available_balance(bot.cred_id, bot.env)
        log.debug("[balance.fetch] available=%s | %s", str(available), ctx_str)

        # --- 2) Compute intended size
        qty = self._calculate_position_size(bot, available, signal.trigger)
        if qty <= 0:
            log.debug(
                "[sizing] zero/negative qty -> skip | available=%s trigger=%s cfg: (use_pct=%s pct=%s fixed=%s max_usdt=%s) | %s",
                str(available),
                str(signal.trigger),
                bot.use_balance_pct,
                str(bot.balance_pct),
                str(bot.fixed_notional),
                str(bot.max_position_usdt),
                ctx_str,
            )
            return self._failure_state(bot, signal)

        # Required margin = exposure / leverage
        leverage = bot.leverage if bot.leverage and bot.leverage > 0 else 1
        try:
            exposure = qty * signal.trigger
            required_margin = exposure / Decimal(leverage)
        except (InvalidOperation, ZeroDivisionError):
            log.exception(
                "Required margin calculation failed | qty=%s trigger=%s leverage=%s | %s",
                qty,
                signal.trigger,
                leverage,
                ctx_str,
            )
            return self._failure_state(bot, signal)

        log.debug(
            "[sizing] qty=%s price=%s exposure=%s lev=%s required_margin=%s | %s",
            str(qty),
            str(signal.trigger),
            str(exposure),
            leverage,
            str(required_margin),
            ctx_str,
        )

        # --- 3) Validate balance
        is_ok, avail_check = await self._bal.validate_balance(
            bot,
            required_margin,
            available_balance=available,
        )
        log.debug(
            "[balance] available=%s required=%s pass=%s | %s",
            str(avail_check),
            str(required_margin),
            is_ok,
            ctx_str,
        )
        if not is_ok:
            return self._failure_state(
                bot, signal, status=OrderStatus.SKIPPED_LOW_BALANCE
            )

        # --- 4) Guard against ultra-tight stops (reduces immediate-trigger risk)
        try:
            stop_dist = abs(signal.trigger - signal.stop)
            if stop_dist <= 0 or stop_dist / signal.trigger < _MIN_STOP_DIST_RATIO:
                log.info(
                    "Skip signal: stop too tight | trigger=%s stop=%s dist_ratio=%.6f | %s",
                    signal.trigger,
                    signal.stop,
                    float(stop_dist / signal.trigger) if signal.trigger else -1.0,
                    ctx_str,
                )
                return self._failure_state(bot, signal, status=OrderStatus.FAILED)
        except Exception:
            pass

        # --- 4) Get trading port
        trading = await self._get_trading(bot)

        # --- 5) Cancel lingering bot-tagged exits before placing a new trio
        await self._cleanup_bot_exit_orders(trading, bot)

        # --- 6) Set leverage idempotently
        try:
            await trading.set_leverage(signal.symbol, bot.leverage)
        except Exception:
            log.exception(
                "set_leverage failed | sym=%s lev=%s | %s",
                signal.symbol,
                bot.leverage,
                ctx_str,
            )
            return self._failure_state(bot, signal)

        # --- 7) Pre-flight: enforce 3dp & min-notional bump, re-check margin
        lev_d = Decimal(str(leverage))
        ok_pf, q_qty_pf, reason_pf = await self._preflight_qty(
            trading,
            signal.symbol,
            qty,
            signal.trigger,
            lev_d,
            available,
        )
        if not ok_pf:
            log.info(
                "Preflight failed | sym=%s reason=%s | %s",
                signal.symbol,
                reason_pf,
                ctx_str,
            )
            status = (
                OrderStatus.SKIPPED_LOW_BALANCE
                if "insufficient_margin" in (reason_pf or "")
                else OrderStatus.FAILED
            )
            return self._failure_state(bot, signal, status=status)

        # --- 8) Quantize ENTRY (exchange ticks), price may adjust; qty should still meet notional
        q_qty, q_price = await trading.quantize_limit_order(
            signal.symbol, q_qty_pf, signal.trigger
        )
        if q_qty <= 0 or q_price is None or q_price <= 0:
            log.error(
                "Quantization invalid | sym=%s raw_qty=%s pf_qty=%s raw_price=%s -> q_qty=%s q_price=%s | %s",
                signal.symbol,
                str(qty),
                str(q_qty_pf),
                str(signal.trigger),
                str(q_qty),
                str(q_price),
                ctx_str,
            )
            return self._failure_state(bot, signal)

        # Additional guard: ensure quantized order still within balance
        try:
            required_quantized_margin = (q_qty * q_price) / Decimal(leverage)
        except InvalidOperation:
            required_quantized_margin = required_margin
        if required_quantized_margin > avail_check:
            log.info(
                "Quantized margin exceeds available | required=%s available=%s | %s",
                str(required_quantized_margin),
                str(avail_check),
                ctx_str,
            )
            return self._failure_state(
                bot, signal, status=OrderStatus.SKIPPED_LOW_BALANCE
            )

        # --- 9) Guard against stale price vs mark/last
        mark_price = await self._current_mark_or_last(trading, signal.symbol)
        if mark_price > 0:
            drift = _MARK_DRIFT_RATIO
            if signal.side == OrderSide.LONG:
                if signal.stop >= mark_price * (Decimal("1") - drift):
                    log.info(
                        "Skip signal: stop at/above current mark | stop=%s mark=%s drift=%s | %s",
                        signal.stop,
                        mark_price,
                        drift,
                        ctx_str,
                    )
                    return self._failure_state(bot, signal, status=OrderStatus.FAILED)
                if q_price > mark_price * (Decimal("1") + drift):
                    log.info(
                        "Skip signal: entry above current mark by drift | entry=%s mark=%s drift=%s | %s",
                        q_price,
                        mark_price,
                        drift,
                        ctx_str,
                    )
                    return self._failure_state(bot, signal, status=OrderStatus.FAILED)
            else:
                if signal.stop <= mark_price * (Decimal("1") + drift):
                    log.info(
                        "Skip signal: stop at/below current mark | stop=%s mark=%s drift=%s | %s",
                        signal.stop,
                        mark_price,
                        drift,
                        ctx_str,
                    )
                    return self._failure_state(bot, signal, status=OrderStatus.FAILED)
                if q_price < mark_price * (Decimal("1") - drift):
                    log.info(
                        "Skip signal: entry below current mark by drift | entry=%s mark=%s drift=%s | %s",
                        q_price,
                        mark_price,
                        drift,
                        ctx_str,
                    )
                    return self._failure_state(bot, signal, status=OrderStatus.FAILED)

        # --- 10) Quantize TP price separately
        tp_r = getattr(bot, "tp_r_multiple", None)
        tp_target = self._compute_tp_price(
            signal.side, signal.trigger, signal.stop, tp_r_multiple=tp_r
        )
        _, q_tp_price = await trading.quantize_limit_order(
            signal.symbol, q_qty, tp_target
        )
        if q_tp_price is None or q_tp_price <= 0:
            log.error(
                "Quantized TP price invalid | sym=%s | %s", signal.symbol, ctx_str
            )
            return self._failure_state(bot, signal)

        # --- 11) Place orders atomically
        try:
            trio = await self._place_order_trio(
                trading,
                bot,
                signal,
                quantity=q_qty,
                entry_price=q_price,
                stop_price=signal.stop,
                take_profit_price=q_tp_price,
            )
        except (
            BinanceBadRequestException,
            BinanceRateLimitException,
            BinanceExchangeDownException,
        ) as exc:
            if self._metrics:
                self._metrics.inc_binance_error(exc.__class__.__name__)
            log.warning("Order placement failed | %s reason=%s", ctx_str, exc)
            return self._failure_state(
                bot, signal, quantity=q_qty, trigger_price=q_price
            )
        except BinanceAPIException as exc:
            if self._metrics:
                self._metrics.inc_binance_error(exc.__class__.__name__)
            log.error("Order placement failed | %s reason=%s", ctx_str, exc)
            return self._failure_state(
                bot, signal, quantity=q_qty, trigger_price=q_price
            )

        if not trio.entry_order_id:
            log.error(
                "Order placement returned no entry id | entry=%s stop=%s tp=%s | %s",
                trio.entry_order_id,
                trio.stop_order_id,
                trio.take_profit_order_id,
                ctx_str,
            )
            return self._failure_state(
                bot, signal, quantity=q_qty, trigger_price=q_price
            )

        if trio.stop_order_id is None or trio.take_profit_order_id is None:
            log.warning(
                "Placed entry without full exits (monitor will recover) | entry=%s stop=%s tp=%s | %s",
                trio.entry_order_id,
                trio.stop_order_id,
                trio.take_profit_order_id,
                ctx_str,
            )

        log.info(
            "Order trio placed | qty=%s entry_price=%s stop=%s tp=%s entry_id=%s stop_id=%s tp_id=%s | %s",
            str(q_qty),
            str(q_price),
            str(signal.stop),
            str(q_tp_price),
            trio.entry_order_id,
            trio.stop_order_id,
            trio.take_profit_order_id,
            ctx_str,
        )

        # TODO: Position creation will be triggered once fill events are wired from websocket/user-data streams.
        return OrderState(
            bot_id=bot.id,
            signal_id="",
            status=OrderStatus.PENDING,
            side=signal.side,
            symbol=signal.symbol,
            trigger_price=q_price,
            stop_price=signal.stop,
            quantity=q_qty,
            filled_quantity=Decimal("0"),
            order_id=trio.entry_order_id,
            stop_order_id=trio.stop_order_id,
            take_profit_order_id=trio.take_profit_order_id,
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
            pct = (
                bot.balance_pct
                if bot.balance_pct and bot.balance_pct > 0
                else Decimal("0")
            )
            if pct > Decimal("1"):
                pct = Decimal("1")
            notional = available_balance * pct
        else:
            return Decimal("0")

        if (
            bot.max_position_usdt
            and bot.max_position_usdt > 0
            and notional > bot.max_position_usdt
        ):
            notional = bot.max_position_usdt

        qty = notional / entry_price
        return qty if qty > 0 else Decimal("0")
