from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Awaitable, Callable, Dict, Iterable, List, Optional, Protocol
from uuid import UUID

from ..domain.enums import OrderStatus
from ..domain.exceptions import WorkerException
from ..domain.models import BotConfig, OrderState
from .position_manager import PositionManager
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


def _is_exchange_filled(status: str) -> bool:
    normalized = status.upper()
    return normalized in {"FILLED", "PARTIALLY_FILLED"}


def _is_exchange_open(status: str) -> bool:
    normalized = status.upper()
    return normalized in {"NEW", "PARTIALLY_FILLED", "PENDING_NEW", "ACCEPTED"}


class BinanceOrderMonitor:
    """Polls Binance for order status changes to drive state transitions."""

    ACTIVE_STATUSES = (OrderStatus.PENDING, OrderStatus.FILLED, OrderStatus.ARMED)

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
        states = await self._orders.list_states_by_statuses(self.ACTIVE_STATUSES)
        for state in states:
            bot = await self._get_bot(state.bot_id)
            if bot is None:
                continue
            trading = await self._get_trading(bot)
            if state.status == OrderStatus.PENDING:
                await self._handle_pending(bot, state, trading)
            else:
                await self._handle_active(bot, state, trading)

    async def _get_bot(self, bot_id: UUID) -> Optional[BotConfig]:
        cached = self._bot_cache.get(bot_id)
        if cached is not None:
            return cached
        bot = await self._bots.get_bot(bot_id)
        if bot is not None:
            self._bot_cache[bot_id] = bot
        return bot

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
        executed_qty = _to_decimal(info.get("executedQty") or info.get("cumQty"))
        if _is_exchange_filled(status) and executed_qty > 0:
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
                filled_qty = _to_decimal(info.get("executedQty") or info.get("cumQty"))
                fill_price = _to_decimal(
                    info.get("avgPrice") or info.get("avg_price") or info.get("price"),
                    state.trigger_price,
                )
                if filled_qty > 0:
                    state.quantity = filled_qty
                    state.record_fill(quantity=filled_qty, price=fill_price)
                    await self._orders.save_state(state)

        try:
            allow_pyramiding = getattr(bot, "allow_pyramiding", False)
            position = await self._positions.open_position(
                bot.id,
                state,
                allow_pyramiding=allow_pyramiding,
            )
            if state.status == OrderStatus.FILLED:
                state.mark(OrderStatus.ARMED)
                await self._orders.save_state(state)
            return position
        except WorkerException as exc:
            log.warning("Failed to rehydrate position | bot_id=%s err=%s", bot.id, exc)
            return None

    async def _on_entry_filled(
        self,
        bot: BotConfig,
        state: OrderState,
        trading: TradingClient,
        info: dict,
    ) -> None:
        filled_qty = _to_decimal(
            info.get("executedQty") or info.get("cumQty") or info.get("origQty")
        )
        fill_price = _to_decimal(
            info.get("avgPrice") or info.get("avg_price") or info.get("price"),
            state.trigger_price,
        )
        if filled_qty <= 0:
            filled_qty = state.quantity
        state.quantity = filled_qty
        state.record_fill(
            quantity=filled_qty, price=fill_price, fill_time=datetime.now(timezone.utc)
        )
        state.mark(OrderStatus.FILLED)
        await self._orders.save_state(state)

        allow_pyramiding = getattr(bot, "allow_pyramiding", False)
        position = await self._positions.open_position(
            bot.id,
            state,
            allow_pyramiding=allow_pyramiding,
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

        state.mark(OrderStatus.CLOSED)
        await self._orders.save_state(state)
        await self._positions.close_position(bot.id, reason)
        fill_price = _to_decimal(
            filled_order.get("avgPrice")
            or filled_order.get("price")
            or filled_order.get("stopPrice")
        )
        if self._metrics:
            self._metrics.inc_order_monitor_event(reason)
        log.info(
            "Exit leg filled | bot_id=%s symbol=%s reason=%s price=%s",
            bot.id,
            state.symbol,
            reason,
            fill_price,
        )
