from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from typing import (
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Protocol,
)
from uuid import UUID

from ..domain.enums import OrderSide, OrderStatus, exit_side_for
from ..domain.exceptions import WorkerException
from ..domain.models import BotConfig, OrderState
from .position_manager import PositionManager
from .order_executor import _bot_client_prefix, _bot_exit_client_id
from ..infrastructure.metrics import WorkerMetrics

log = logging.getLogger(__name__)


class OrderGateway(Protocol):
    async def list_states_by_statuses(
        self,
        statuses: Iterable[OrderStatus],
    ) -> List[OrderState]: ...

    async def save_state(self, state: OrderState) -> None: ...


class BotRepository(Protocol):
    async def get_bot(self, bot_id: UUID) -> Optional[BotConfig]: ...

    async def get_enabled_bots(self) -> List[BotConfig]: ...


class TradingClient(Protocol):
    async def get_order(self, symbol: str, order_id: int) -> dict: ...

    async def cancel_order(self, symbol: str, order_id: int) -> None: ...

    async def list_open_orders(self, symbol: Optional[str] = None) -> List[dict]: ...


def _to_decimal(value: object, default: Decimal = Decimal("0")) -> Decimal:
    try:
        if value in (None, ""):
            return default
        return Decimal(str(value))
    except Exception:
        return default


def _get_any(mapping: Mapping[str, object], *keys: str) -> object | None:
    for key in keys:
        if key not in mapping:
            continue
        value = mapping.get(key)
        if value not in (None, ""):
            return value
    return None


def _to_bool(value: object) -> bool | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "yes"}:
        return True
    if text in {"false", "0", "no"}:
        return False
    return None


def _to_int_or_none(value: object) -> int | None:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _is_exchange_filled(status: str) -> bool:
    normalized = status.upper()
    return normalized in {"FILLED", "PARTIALLY_FILLED"}


def _is_exchange_open(status: str) -> bool:
    normalized = status.upper()
    return normalized in {"NEW", "PARTIALLY_FILLED", "PENDING_NEW", "ACCEPTED"}


def _is_order_not_found_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(
        needle in msg
        for needle in (
            "unknown order",
            "order does not exist",
            "order not found",
            "unknown order sent",
        )
    )


def _is_immediate_trigger_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "immediately trigger" in msg or "would immediately trigger" in msg


class BinanceOrderMonitor:
    """Polls Binance for order status changes to drive state transitions."""

    ACTIVE_STATUSES = (OrderStatus.PENDING, OrderStatus.FILLED, OrderStatus.ARMED)
    _CLOSED_STATUSES = (
        OrderStatus.CANCELLED,
        OrderStatus.FAILED,
        OrderStatus.SKIPPED_LOW_BALANCE,
        OrderStatus.SKIPPED_WHITELIST,
    )

    def __init__(
        self,
        *,
        bot_repository: BotRepository,
        order_gateway: OrderGateway,
        position_manager: PositionManager,
        trading_factory: Callable[[BotConfig], Awaitable[TradingClient]],
        poll_interval: float = 2.0,
        metrics: WorkerMetrics | None = None,
    ) -> None:
        self._bots = bot_repository
        self._orders = order_gateway
        self._positions = position_manager
        self._trading_factory = trading_factory
        self._poll_interval = max(0.5, poll_interval)
        self._metrics = metrics

        self._bot_cache: Dict[UUID, BotConfig] = {}
        self._trading_cache: Dict[UUID, TradingClient] = {}
        self._trading_lock = asyncio.Lock()
        self._enabled_bots_refreshed_at: float = 0.0
        self._enabled_bots_ttl_seconds: float = 60.0

        self._task: Optional[asyncio.Task[None]] = None
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        if self._task is not None:
            return
        await self.sync_on_startup()
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_loop(), name="worker.order_monitor")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._stop_event.set()
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    async def run_once(self) -> None:
        await self._poll_states()

    async def sync_on_startup(self) -> None:
        states = await self._orders.list_states_by_statuses(self.ACTIVE_STATUSES)
        for state in states:
            bot = await self._get_bot(state.bot_id)
            if bot is None:
                continue
            trading = await self._get_trading(bot)
            if state.status == OrderStatus.PENDING:
                await self._handle_pending(bot, state, trading, allow_transition=True)
            else:
                await self._ensure_position(bot, state, trading)

    async def _run_loop(self) -> None:
        try:
            while not self._stop_event.is_set():
                try:
                    await self._poll_states()
                except Exception:  # pragma: no cover - guarded logging
                    log.exception("Order monitor poll failure")
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(), timeout=self._poll_interval
                    )
                except asyncio.TimeoutError:
                    continue
        except asyncio.CancelledError:
            raise

    async def _poll_states(self) -> None:
        await self._refresh_enabled_bots_cache()
        bots_seen: set[UUID] = set()
        active_bots: set[UUID] = set()

        states = await self._orders.list_states_by_statuses(self.ACTIVE_STATUSES)
        for state in states:
            bot = await self._get_bot(state.bot_id)
            if bot is None:
                continue
            bots_seen.add(bot.id)
            active_bots.add(bot.id)
            trading = await self._get_trading(bot)
            if state.status == OrderStatus.PENDING:
                await self._handle_pending(bot, state, trading)
            else:
                await self._handle_active(bot, state, trading)

        # Defensive cleanup: cancel lingering TP/SL on already closed/cancelled states
        closed_states = await self._orders.list_states_by_statuses(
            self._CLOSED_STATUSES
        )
        recovered: set[UUID] = set()
        for state in closed_states:
            if state.bot_id in recovered:
                continue
            if not (
                state.take_profit_order_id
                or state.stop_order_id
                or state.filled_quantity > 0
                or state.avg_fill_price is not None
            ):
                continue
            bot = await self._get_bot(state.bot_id)
            if bot is None:
                continue
            bots_seen.add(bot.id)
            trading = await self._get_trading(bot)
            if await self._exchange_has_open_position(trading, state.symbol):
                if state.filled_quantity > 0 or state.avg_fill_price is not None:
                    await self._recover_protective_orders(bot, state, trading)
                    recovered.add(state.bot_id)
                else:
                    log.warning(
                        "Closed state but exchange position open; skipping cleanup | bot_id=%s symbol=%s status=%s",
                        bot.id,
                        state.symbol,
                        state.status.value,
                    )
                continue

            if state.take_profit_order_id or state.stop_order_id:
                await self._cleanup_orphan_exits(bot, state, trading)

        # Exchange-level sweep: cancel any open orders tagged to these bots via clientOrderId prefix
        # (works even if we lost state, but only when no exchange position exists).
        for bot_id in set(self._bot_cache.keys()) - active_bots:
            bot = await self._get_bot(bot_id)
            if bot is None:
                continue
            trading = await self._get_trading(bot)
            await self._cleanup_tagged_exits(bot, trading)

    async def _get_bot(self, bot_id: UUID) -> Optional[BotConfig]:
        cached = self._bot_cache.get(bot_id)
        if cached is not None:
            return cached
        bot = await self._bots.get_bot(bot_id)
        if bot is not None:
            self._bot_cache[bot_id] = bot
        return bot

    async def _refresh_enabled_bots_cache(self) -> None:
        """
        Periodically refresh the bot cache from the DB so we can clean up tagged
        exit orders even when there are no tracked states (e.g., after a restart).
        """
        now = time.monotonic()
        if (now - self._enabled_bots_refreshed_at) < self._enabled_bots_ttl_seconds:
            return

        getter = getattr(self._bots, "get_enabled_bots", None)
        if not callable(getter):
            self._enabled_bots_refreshed_at = now
            return

        try:
            bots = await getter()
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("Failed to refresh enabled bots | err=%s", exc)
            self._enabled_bots_refreshed_at = now
            return

        for bot in bots:
            self._bot_cache[bot.id] = bot
        self._enabled_bots_refreshed_at = now

    async def _get_trading(self, bot: BotConfig) -> TradingClient:
        cached = self._trading_cache.get(bot.id)
        if cached is not None:
            return cached
        async with self._trading_lock:
            cached = self._trading_cache.get(bot.id)
            if cached is not None:
                return cached
            trading = await self._trading_factory(bot)
            self._trading_cache[bot.id] = trading
            return trading

    async def _fetch_order(
        self,
        trading: TradingClient,
        symbol: str,
        order_id: Optional[int],
    ) -> Optional[dict]:
        if not order_id:
            return None
        try:
            return await trading.get_order(symbol, int(order_id))
        except Exception as exc:
            log.warning(
                "Order lookup failed | symbol=%s order_id=%s err=%s",
                symbol,
                order_id,
                exc,
            )
            return None

    async def _handle_pending(
        self,
        bot: BotConfig,
        state: OrderState,
        trading: TradingClient,
        *,
        allow_transition: bool = False,
    ) -> None:
        info = await self._fetch_order(trading, state.symbol, state.order_id)
        if info is None:
            log.info(
                "Pending order missing on exchange, marking cancelled | bot_id=%s",
                bot.id,
            )
            state.mark(OrderStatus.CANCELLED)
            await self._orders.save_state(state)
            if self._metrics:
                self._metrics.inc_order_monitor_event("pending_missing_cancelled")
            return

        status = str(info.get("status", "")).upper()
        executed_qty = _to_decimal(
            _get_any(info, "executed_qty", "executedQty", "cum_qty", "cumQty")
        )
        # If any quantity was executed, treat it as a fill even if the order was later canceled/expired.
        if executed_qty > 0 and (
            _is_exchange_filled(status) or not _is_exchange_open(status)
        ):
            await self._on_entry_filled(bot, state, trading, info)
        elif not _is_exchange_open(status) and allow_transition:
            log.info(
                "Pending order not open (status=%s), marking cancelled | bot_id=%s",
                status,
                bot.id,
            )
            state.mark(OrderStatus.CANCELLED)
            await self._orders.save_state(state)
            if self._metrics:
                self._metrics.inc_order_monitor_event("pending_not_open_cancelled")

    async def _handle_active(
        self,
        bot: BotConfig,
        state: OrderState,
        trading: TradingClient,
    ) -> None:
        await self._ensure_position(bot, state, trading)

        # If we *think* we have a position (in-memory), but the exchange reports none,
        # the position was likely closed externally (manual close, liquidation, etc).
        if (
            self._positions.get_position(bot.id) is not None
            and state.status in (OrderStatus.ARMED, OrderStatus.FILLED)
            and not await self._exchange_has_open_position(trading, state.symbol)
        ):
            await self._handle_external_position_close(bot, state, trading)
            return

        # If the position is gone but protective orders remain, clean them up.
        if (
            self._positions.get_position(bot.id) is None
            and state.status in (OrderStatus.ARMED, OrderStatus.FILLED)
            and (state.take_profit_order_id or state.stop_order_id)
        ):
            if not await self._exchange_has_open_position(trading, state.symbol):
                if await self._cleanup_orphan_exits(bot, state, trading):
                    return
        for label, oid in (
            ("take_profit", state.take_profit_order_id),
            ("stop", state.stop_order_id),
        ):
            info = await self._fetch_order(trading, state.symbol, oid)
            if info is None:
                continue
            status = str(info.get("status", ""))
            if _is_exchange_filled(status):
                reason = "tp_hit" if label == "take_profit" else "sl_hit"
                await self._finalize_trade(
                    bot, state, trading, reason, filled_order=info, filled_label=label
                )
                return  # exit after handling a fill to avoid double-processing

    async def _ensure_position(
        self,
        bot: BotConfig,
        state: OrderState,
        trading: TradingClient,
    ) -> None:
        if self._positions.get_position(bot.id):
            return
        if state.status not in (OrderStatus.FILLED, OrderStatus.ARMED):
            return

        if state.filled_quantity <= 0 or state.avg_fill_price is None:
            info = await self._fetch_order(trading, state.symbol, state.order_id)
            if info:
                filled_qty = _to_decimal(
                    _get_any(info, "executed_qty", "executedQty", "cum_qty", "cumQty")
                )
                fill_price = _to_decimal(
                    _get_any(info, "avg_price", "avgPrice", "price"),
                    state.trigger_price,
                )
                if filled_qty > 0:
                    state.quantity = filled_qty
                    state.record_fill(quantity=filled_qty, price=fill_price)
                    await self._orders.save_state(state)

        try:
            allow_pyramiding = getattr(bot, "allow_pyramiding", False)
            await self._positions.open_position(
                bot.id,
                state,
                allow_pyramiding=allow_pyramiding,
                tp_r_multiple=getattr(bot, "tp_r_multiple", None),
            )
            if state.status == OrderStatus.FILLED:
                state.mark(OrderStatus.ARMED)
                await self._orders.save_state(state)
            return None
        except WorkerException as exc:
            log.warning("Failed to rehydrate position | bot_id=%s err=%s", bot.id, exc)
            return None

    async def _cleanup_orphan_exits(
        self,
        bot: BotConfig,
        state: OrderState,
        trading: TradingClient,
    ) -> bool:
        """Cancel lingering TP/SL orders when we should have no position."""
        cancelled_any = False
        for label, oid in (
            ("take_profit", state.take_profit_order_id),
            ("stop", state.stop_order_id),
        ):
            if not oid:
                continue
            try:
                await trading.cancel_order(state.symbol, int(oid))
                cancelled_any = True
                log.info(
                    "Orphan exit order cancelled | bot_id=%s symbol=%s order_id=%s label=%s",
                    bot.id,
                    state.symbol,
                    oid,
                    label,
                )
            except Exception as exc:  # pragma: no cover - defensive cleanup
                if _is_order_not_found_error(exc):
                    cancelled_any = True
                    log.info(
                        "Orphan exit order already absent | bot_id=%s symbol=%s order_id=%s label=%s",
                        bot.id,
                        state.symbol,
                        oid,
                        label,
                    )
                    continue
                log.warning(
                    "Failed to cancel orphan %s order | bot_id=%s symbol=%s order_id=%s err=%s",
                    label,
                    bot.id,
                    state.symbol,
                    oid,
                    exc,
                )

        if cancelled_any:
            state.take_profit_order_id = None
            state.stop_order_id = None
            if state.status in (OrderStatus.ARMED, OrderStatus.FILLED):
                state.mark(OrderStatus.CANCELLED)
            else:
                state.touch()
            await self._orders.save_state(state)
        return cancelled_any

    async def _handle_external_position_close(
        self,
        bot: BotConfig,
        state: OrderState,
        trading: TradingClient,
    ) -> None:
        """
        Handle a position that disappeared on the exchange while we still had it in memory.

        Best-effort cancels exit orders and clears local position to avoid accumulating
        stale TP/SL orders and blocking future ARM signals.
        """
        await self._cleanup_orphan_exits(bot, state, trading)
        await self._positions.close_position(bot.id, "exchange_position_closed")
        log.info(
            "Exchange position closed; local position cleared | bot_id=%s symbol=%s",
            bot.id,
            state.symbol,
        )
        if self._metrics:
            self._metrics.inc_order_monitor_event("exchange_position_closed")

    async def _exchange_has_open_position(
        self, trading: TradingClient, symbol: str
    ) -> bool:
        """
        Best-effort exchange position check.

        If the trading adapter supports ``has_open_position()``, use it to avoid
        cancelling protective orders when a position still exists (e.g., after a restart
        when in-memory positions are empty). On failures, assume a position may exist
        to avoid unsafe cancellations.
        """
        checker = getattr(trading, "has_open_position", None)
        if not callable(checker):
            return False
        try:
            return bool(await checker(symbol))
        except Exception as exc:  # pragma: no cover - defensive / network errors
            log.warning(
                "Open position check failed; skipping cleanup | symbol=%s err=%s",
                symbol,
                exc,
            )
            return True

    async def _cleanup_tagged_exits(
        self, bot: BotConfig, trading: TradingClient
    ) -> None:
        """
        Cancel any open orders on the exchange whose clientOrderId is tagged
        with this bot's prefix. This catches lingering TP/SL from previous runs
        even if state was lost.
        """
        prefix = _bot_client_prefix(bot.id)
        if await self._exchange_has_open_position(trading, bot.symbol):
            return
        try:
            open_orders = await trading.list_open_orders(bot.symbol)
        except Exception as exc:  # pragma: no cover - defensive
            log.warning(
                "Prefix cleanup skipped (list_open_orders failed) | bot_id=%s err=%s",
                bot.id,
                exc,
            )
            return

        cancelled = 0
        for order in open_orders:
            client_id = str(_get_any(order, "client_order_id", "clientOrderId") or "")
            if not client_id.startswith(prefix):
                continue
            reduce_only = _to_bool(_get_any(order, "reduce_only", "reduceOnly"))
            if reduce_only is False:
                continue
            oid = _get_any(order, "order_id", "orderId")
            if oid in (None, ""):
                continue
            try:
                await trading.cancel_order(bot.symbol, int(oid))
                cancelled += 1
                log.info(
                    "Cancelled tagged exchange order | bot_id=%s symbol=%s order_id=%s client_id=%s",
                    bot.id,
                    bot.symbol,
                    oid,
                    client_id,
                )
            except Exception as exc:  # pragma: no cover - defensive
                log.warning(
                    "Failed to cancel tagged exchange order | bot_id=%s symbol=%s order_id=%s err=%s",
                    bot.id,
                    bot.symbol,
                    oid,
                    exc,
                )

        if cancelled:
            log.info(
                "Tagged exit cleanup complete | bot_id=%s cancelled=%d",
                bot.id,
                cancelled,
            )

    async def _failsafe_close_position(
        self,
        bot: BotConfig,
        state: OrderState,
        trading: TradingClient,
        *,
        quantity: Decimal,
        reason: str,
    ) -> None:
        closer = getattr(trading, "close_position_market", None)
        if not callable(closer):
            log.error(
                "Failsafe close unavailable | bot_id=%s symbol=%s reason=%s",
                bot.id,
                bot.symbol,
                reason,
            )
            return

        exit_side = exit_side_for(state.side)
        try:
            await closer(bot.symbol, exit_side, quantity)
            log.error(
                "Failsafe position close submitted | bot_id=%s symbol=%s side=%s qty=%s reason=%s",
                bot.id,
                bot.symbol,
                exit_side.value,
                str(quantity),
                reason,
            )
        except Exception as exc:  # pragma: no cover - network/exchange errors
            log.error(
                "Failsafe close failed | bot_id=%s symbol=%s qty=%s reason=%s err=%s",
                bot.id,
                bot.symbol,
                str(quantity),
                reason,
                exc,
            )
            return

        await self._positions.close_position(bot.id, "failsafe_close")
        state.mark(OrderStatus.CANCELLED)
        await self._orders.save_state(state)

    async def _recover_protective_orders(
        self, bot: BotConfig, state: OrderState, trading: TradingClient
    ) -> None:
        if state.stop_price <= 0:
            log.warning(
                "Recovery skipped (invalid stop) | bot_id=%s symbol=%s stop=%s",
                bot.id,
                state.symbol,
                str(state.stop_price),
            )
            return

        if state.filled_quantity <= 0 or state.avg_fill_price is None:
            info = await self._fetch_order(trading, state.symbol, state.order_id)
            if info:
                filled_qty = _to_decimal(
                    _get_any(info, "executed_qty", "executedQty", "cum_qty", "cumQty")
                )
                fill_price = _to_decimal(
                    _get_any(info, "avg_price", "avgPrice", "price"),
                    state.trigger_price,
                )
                if filled_qty > 0:
                    state.quantity = filled_qty
                    state.record_fill(quantity=filled_qty, price=fill_price)

        qty = state.filled_quantity if state.filled_quantity > 0 else state.quantity
        if qty <= 0:
            log.warning(
                "Recovery skipped (unknown qty) | bot_id=%s symbol=%s",
                bot.id,
                state.symbol,
            )
            return

        allow_pyramiding = getattr(bot, "allow_pyramiding", False)
        tp_r_multiple = getattr(bot, "tp_r_multiple", None)
        position = self._positions.get_position(bot.id)
        if position is None:
            try:
                position = await self._positions.open_position(
                    bot.id,
                    replace(state, status=OrderStatus.FILLED),
                    allow_pyramiding=allow_pyramiding,
                    tp_r_multiple=tp_r_multiple,
                )
            except WorkerException as exc:
                log.warning(
                    "Recovery failed to rehydrate position | bot_id=%s symbol=%s err=%s",
                    bot.id,
                    state.symbol,
                    exc,
                )
                return

        prefix = _bot_client_prefix(bot.id)
        try:
            open_orders = await trading.list_open_orders(bot.symbol)
        except Exception as exc:  # pragma: no cover - defensive
            log.warning(
                "Recovery list_open_orders failed | bot_id=%s symbol=%s err=%s",
                bot.id,
                bot.symbol,
                exc,
            )
            open_orders = []

        extra_order_ids: list[int] = []
        for order in open_orders:
            client_id = str(_get_any(order, "client_order_id", "clientOrderId") or "")
            if not client_id.startswith(prefix):
                continue
            reduce_only = _to_bool(_get_any(order, "reduce_only", "reduceOnly"))
            if reduce_only is False:
                continue
            oid = _to_int_or_none(_get_any(order, "order_id", "orderId"))
            if oid is None:
                continue
            order_type = str(order.get("type") or "").upper()
            if order_type == "STOP_MARKET":
                if state.stop_order_id is None:
                    state.stop_order_id = oid
                else:
                    extra_order_ids.append(oid)
            elif order_type == "TAKE_PROFIT_MARKET":
                if state.take_profit_order_id is None:
                    state.take_profit_order_id = oid
                else:
                    extra_order_ids.append(oid)

        for oid in extra_order_ids:
            try:
                await trading.cancel_order(bot.symbol, int(oid))
                log.info(
                    "Recovery cancelled extra tagged exit | bot_id=%s symbol=%s order_id=%s",
                    bot.id,
                    bot.symbol,
                    oid,
                )
            except Exception:
                continue

        creator = getattr(trading, "create_stop_market_order", None)
        if not callable(creator):
            log.warning(
                "Recovery skipped (trading cannot create exits) | bot_id=%s symbol=%s",
                bot.id,
                bot.symbol,
            )
            return

        exit_side = exit_side_for(state.side)
        if state.stop_order_id is None:
            stop_client_id = _bot_exit_client_id(bot.id, "sl")
            try:
                resp = await creator(
                    symbol=bot.symbol,
                    side=exit_side,
                    quantity=qty,
                    stop_price=state.stop_price,
                    reduce_only=True,
                    order_type="STOP_MARKET",
                    time_in_force="GTE_GTC",
                    new_client_order_id=stop_client_id,
                )
                state.stop_order_id = _to_int_or_none(
                    _get_any(resp, "order_id", "orderId")
                )
            except Exception as exc:  # pragma: no cover - defensive / exchange errors
                if _is_immediate_trigger_error(exc):
                    await self._failsafe_close_position(
                        bot,
                        state,
                        trading,
                        quantity=qty,
                        reason="recovery_stop_immediate_trigger",
                    )
                    return
                log.warning(
                    "Recovery stop create failed | bot_id=%s symbol=%s err=%s",
                    bot.id,
                    bot.symbol,
                    exc,
                )
                return

            if state.stop_order_id is None:
                log.warning(
                    "Recovery stop returned no id; closing position | bot_id=%s symbol=%s",
                    bot.id,
                    bot.symbol,
                )
                await self._failsafe_close_position(
                    bot,
                    state,
                    trading,
                    quantity=qty,
                    reason="recovery_stop_missing_id",
                )
                return

        if state.take_profit_order_id is None:
            tp_client_id = _bot_exit_client_id(bot.id, "tp")
            try:
                resp = await creator(
                    symbol=bot.symbol,
                    side=exit_side,
                    quantity=qty,
                    stop_price=position.take_profit,
                    reduce_only=True,
                    order_type="TAKE_PROFIT_MARKET",
                    time_in_force="GTE_GTC",
                    new_client_order_id=tp_client_id,
                )
                state.take_profit_order_id = _to_int_or_none(
                    _get_any(resp, "order_id", "orderId")
                )
            except Exception as exc:  # pragma: no cover - defensive / exchange errors
                if _is_immediate_trigger_error(exc):
                    await self._failsafe_close_position(
                        bot,
                        state,
                        trading,
                        quantity=qty,
                        reason="recovery_tp_immediate_trigger",
                    )
                    return
                log.warning(
                    "Recovery take-profit create failed | bot_id=%s symbol=%s err=%s",
                    bot.id,
                    bot.symbol,
                    exc,
                )
                return

            if state.take_profit_order_id is None:
                log.warning(
                    "Recovery tp returned no id; closing position | bot_id=%s symbol=%s",
                    bot.id,
                    bot.symbol,
                )
                await self._failsafe_close_position(
                    bot,
                    state,
                    trading,
                    quantity=qty,
                    reason="recovery_tp_missing_id",
                )
                return

        state.mark(OrderStatus.ARMED)
        await self._orders.save_state(state)
        log.warning(
            "Recovered missing exits for open position | bot_id=%s symbol=%s stop_id=%s tp_id=%s",
            bot.id,
            bot.symbol,
            state.stop_order_id,
            state.take_profit_order_id,
        )

    async def _on_entry_filled(
        self,
        bot: BotConfig,
        state: OrderState,
        trading: TradingClient,
        info: dict,
    ) -> None:
        filled_qty = _to_decimal(
            _get_any(
                info,
                "executed_qty",
                "executedQty",
                "cum_qty",
                "cumQty",
                "orig_qty",
                "origQty",
            )
        )
        fill_price = _to_decimal(
            _get_any(info, "avg_price", "avgPrice", "price"),
            state.trigger_price,
        )
        if filled_qty <= 0:
            filled_qty = state.quantity
        state.quantity = filled_qty
        state.record_fill(
            quantity=filled_qty, price=fill_price, fill_time=datetime.now(timezone.utc)
        )
        state.exit_price = fill_price
        try:
            entry = state.avg_fill_price or state.trigger_price
            if state.side == OrderSide.LONG:
                state.realized_pnl = (fill_price - entry) * filled_qty
            else:
                state.realized_pnl = (entry - fill_price) * filled_qty
        except Exception:
            state.realized_pnl = Decimal("0")
        state.mark(OrderStatus.FILLED)
        await self._orders.save_state(state)

        allow_pyramiding = getattr(bot, "allow_pyramiding", False)
        position = await self._positions.open_position(
            bot.id,
            state,
            allow_pyramiding=allow_pyramiding,
            tp_r_multiple=getattr(bot, "tp_r_multiple", None),
        )

        state.mark(OrderStatus.ARMED)
        await self._orders.save_state(state)
        log.info(
            "Order filled and armed | bot_id=%s symbol=%s qty=%s entry=%s tp=%s",
            bot.id,
            state.symbol,
            filled_qty,
            fill_price,
            position.take_profit,
        )

    async def _finalize_trade(
        self,
        bot: BotConfig,
        state: OrderState,
        trading: TradingClient,
        reason: str,
        *,
        filled_order: dict,
        filled_label: str,
    ) -> None:
        other_id = (
            state.stop_order_id
            if filled_label == "take_profit"
            else state.take_profit_order_id
        )
        if other_id:
            other_info = await self._fetch_order(trading, state.symbol, other_id)
            if other_info and not _is_exchange_filled(
                str(other_info.get("status", ""))
            ):
                try:
                    await trading.cancel_order(state.symbol, int(other_id))
                except Exception as exc:
                    log.warning(
                        "Failed to cancel opposing leg | bot_id=%s symbol=%s order_id=%s err=%s",
                        bot.id,
                        state.symbol,
                        other_id,
                        exc,
                    )

        state.mark(OrderStatus.CANCELLED)
        await self._orders.save_state(state)
        await self._positions.close_position(bot.id, reason)
        fill_price = _to_decimal(
            _get_any(
                filled_order,
                "avg_price",
                "avgPrice",
                "price",
                "stop_price",
                "stopPrice",
            )
        )
        state.exit_price = fill_price
        try:
            entry = state.avg_fill_price or state.trigger_price
            if state.side == OrderSide.LONG:
                state.realized_pnl = (fill_price - entry) * state.filled_quantity
            else:
                state.realized_pnl = (entry - fill_price) * state.filled_quantity
        except Exception:
            state.realized_pnl = Decimal("0")
        if self._metrics:
            self._metrics.inc_order_monitor_event(reason)
        log.info(
            "Exit leg filled | bot_id=%s symbol=%s reason=%s price=%s",
            bot.id,
            state.symbol,
            reason,
            fill_price,
        )
