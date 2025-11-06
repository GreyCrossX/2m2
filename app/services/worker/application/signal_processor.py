from __future__ import annotations

import logging
import inspect
from dataclasses import replace
from decimal import Decimal
from typing import Awaitable, Callable, Dict, Iterable, List, Optional, Protocol
from uuid import UUID

from ..core.logging_utils import ensure_log_context, format_log_context
from ..domain.enums import OrderStatus, OrderSide  # <<< changed
from ..domain.exceptions import InvalidSignalException
from ..domain.models import ArmSignal, BotConfig, DisarmSignal, OrderState, Position

logger = logging.getLogger(__name__)


class SymbolRouter(Protocol):
    """Port for routing/lookup: which bots are subscribed to a (symbol, timeframe)."""
    def get_bot_ids(self, symbol: str, timeframe: str) -> Iterable[UUID]: ...


class BotRepository(Protocol):
    """Port to load BotConfig."""
    async def get_bot(self, bot_id: UUID) -> Optional[BotConfig]: ...


class OrderGateway(Protocol):
    """Port for order state persistence."""

    async def list_pending_order_states(
        self,
        bot_id: UUID,
        symbol: str,
        side: OrderSide,  # <<< changed
        statuses: Optional[Iterable[OrderStatus]] = None,
    ) -> List[OrderState]: ...

    async def save_state(self, state: OrderState) -> None: ...


class OrderExecutorPort(Protocol):
    """Use case that actually creates an order (we'll inject OrderExecutor)."""
    async def execute_order(self, bot: BotConfig, signal: ArmSignal) -> OrderState: ...


class PositionStore(Protocol):
    def get_position(self, bot_id: UUID) -> Position | Awaitable[Optional[Position]] | None: ...

    def get_positions(
        self, bot_id: UUID
    ) -> Iterable[Position] | Awaitable[Iterable[Position]] | None:
        ...


class TradingPort(Protocol):
    async def cancel_order(self, symbol: str, order_id: int) -> None: ...


class SignalProcessor:
    """Orchestrates ARM/DISARM handling across subscribed bots."""

    def __init__(
        self,
        router: SymbolRouter,
        bot_repository: BotRepository,
        order_executor: OrderExecutorPort,
        order_gateway: OrderGateway,
        trading_factory: Callable[[BotConfig], Awaitable[TradingPort]],
        position_store: Optional[PositionStore] = None,
    ) -> None:
        self._router = router
        self._bots = bot_repository
        self._executor = order_executor
        self._orders = order_gateway
        self._trading_factory = trading_factory
        self._positions = position_store
        logger.info("SignalProcessor initialized")

    async def _get_positions(self, bot_id: UUID) -> List[Position]:
        if self._positions is None:
            return []

        getter = getattr(self._positions, "get_positions", None)
        positions: Iterable[Position] | Position | None

        if getter is not None:
            positions = getter(bot_id)
        else:
            positions = self._positions.get_position(bot_id)

        if inspect.isawaitable(positions):  # type: ignore[truthy-bool]
            positions = await positions  # type: ignore[misc]

        if positions is None:
            return []

        if isinstance(positions, Position):
            return [positions]

        return list(positions)

    async def process_arm_signal(
        self,
        signal: ArmSignal,
        message_id: str,
        log_context: Optional[Dict[str, str]] = None,
    ) -> List[OrderState]:
        """Handle ARM signals by placing orders for every subscribed bot."""
        if signal.symbol != signal.symbol.upper():
            logger.error("Signal symbol not uppercased | symbol=%s", signal.symbol)
            raise InvalidSignalException("Signal symbol must be uppercased.")

        # Signals are frozen dataclasses -> attach msg_id via replace
        sig = replace(signal, signal_msg_id=message_id)

        context = ensure_log_context(
            log_context,
            symbol=sig.symbol,
            tf=sig.timeframe,
            type=sig.type.value,
            msg_id=message_id,
            prev_side=log_context.get("prev_side") if log_context else "-",
        )
        logger.info(
            "Processing ARM signal | side=%s trigger=%s stop=%s | %s",
            sig.side.value,
            sig.trigger,
            sig.stop,
            format_log_context(context),
        )

        results: List[OrderState] = []
        bot_ids = list(self._router.get_bot_ids(sig.symbol, sig.timeframe))

        logger.info("Found %d bot(s) subscribed | %s", len(bot_ids), format_log_context(context))

        if not bot_ids:
            logger.warning("No bots subscribed - signal will not be processed | %s", format_log_context(context))
            return results

        for i, bot_id in enumerate(bot_ids, 1):
            bot_context = ensure_log_context(context, bot_id=str(bot_id))
            log_ctx = format_log_context(bot_context)
            logger.debug("Processing bot %d/%d | bot_id=%s | %s", i, len(bot_ids), bot_id, log_ctx)

            bot = await self._bots.get_bot(bot_id)
            if bot is None:
                logger.warning("Bot not found in database | bot_id=%s | %s", bot_id, log_ctx)
                continue

            if not bot.enabled:
                logger.info("Skipping disabled bot %s | %s", bot_id, log_ctx)
                continue

            logger.info(
                "Checking side whitelist | bot_id=%s bot_whitelist=%s signal_side=%s | %s",
                bot_id,
                bot.side_whitelist.value,
                sig.side.value,
                log_ctx,
            )

            if not bot.allows_side(sig.side):
                logger.info(
                    "Bot side whitelist blocked signal | bot_id=%s whitelist=%s signal_side=%s | %s",
                    bot_id,
                    bot.side_whitelist.value,
                    sig.side.value,
                    log_ctx,
                )
                state = OrderState(
                    bot_id=bot.id,
                    signal_id=message_id,
                    status=OrderStatus.SKIPPED_WHITELIST,
                    side=sig.side,
                    symbol=sig.symbol,
                    trigger_price=sig.trigger,
                    stop_price=sig.stop,
                    quantity=Decimal("0"),
                )
                logger.debug("Saving SKIPPED_WHITELIST state | bot_id=%s | %s", bot_id, log_ctx)
                await self._orders.save_state(state)
                results.append(state)
                continue

            allow_pyramiding = getattr(bot, "allow_pyramiding", False)
            if not allow_pyramiding:
                positions = await self._get_positions(bot.id)
                open_position = next((pos for pos in positions if pos.quantity > 0), None)
                if open_position is not None:
                    logger.info(
                        "Active position open, skipping new ARM | bot_id=%s qty=%s side=%s | %s",
                        bot_id,
                        open_position.quantity,
                        open_position.side.value,
                        log_ctx,
                    )
                    continue

                active_states = await self._orders.list_pending_order_states(
                    bot.id,
                    sig.symbol,
                    sig.side,
                    statuses=(OrderStatus.PENDING, OrderStatus.ARMED, OrderStatus.FILLED),
                )
                if active_states:
                    logger.info(
                        "Active trade detected, skipping new ARM | bot_id=%s active=%s | %s",
                        bot_id,
                        len(active_states),
                        log_ctx,
                    )
                    continue

            logger.info(
                "Executing order | bot_id=%s side=%s trigger=%s | %s",
                bot_id,
                sig.side.value,
                sig.trigger,
                log_ctx,
            )

            try:
                state = await self._executor.execute_order(bot, sig)
                state.signal_id = message_id
                state.touch()

                logger.info(
                    "Order executed | bot_id=%s status=%s qty=%s order_id=%s | %s",
                    bot_id,
                    state.status.value,
                    state.quantity,
                    state.order_id or "N/A",
                    log_ctx,
                )

                logger.debug(
                    "Saving order state | bot_id=%s status=%s | %s",
                    bot_id,
                    state.status.value,
                    log_ctx,
                )
                await self._orders.save_state(state)
                results.append(state)
            except Exception as e:
                logger.error(
                    "Order execution failed | bot_id=%s symbol=%s err=%s | %s",
                    bot_id,
                    sig.symbol,
                    e,
                    log_ctx,
                    exc_info=True,
                )
                continue

        logger.info(
            "ARM signal processing complete | bots_processed=%d results=%d | %s",
            len(bot_ids), len(results), format_log_context(context),
        )

        status_counts: Dict[str, int] = {}
        for r in results:
            status_counts[r.status.value] = status_counts.get(r.status.value, 0) + 1
        if status_counts:
            logger.info(
                "Result summary | %s | %s",
                " | ".join(f"{k}={v}" for k, v in status_counts.items()),
                format_log_context(context),
            )

        return results

    async def process_disarm_signal(
        self,
        signal: DisarmSignal,
        message_id: str,
        log_context: Optional[Dict[str, str]] = None,
    ) -> List[str]:
        """Cancel pending orders matching the DISARM semantics."""
        sig = replace(signal, signal_msg_id=message_id)

        context = ensure_log_context(
            log_context,
            symbol=sig.symbol,
            tf=sig.timeframe,
            type=sig.type.value,
            msg_id=message_id,
            prev_side=sig.prev_side.value,
        )
        logger.info("Processing DISARM signal | reason=%s | %s", sig.reason, format_log_context(context))

        cancelled: List[str] = []
        bot_ids = list(self._router.get_bot_ids(sig.symbol, sig.timeframe))

        logger.info("Found %d bot(s) subscribed | %s", len(bot_ids), format_log_context(context))

        if not bot_ids:
            logger.warning("No bots subscribed - DISARM has no effect | %s", format_log_context(context))
            return cancelled

        total_matching = 0
        for i, bot_id in enumerate(bot_ids, 1):
            bot_context = ensure_log_context(context, bot_id=str(bot_id))
            log_ctx = format_log_context(bot_context)
            logger.debug(
                "Processing bot %d/%d for cancellation | bot_id=%s | %s",
                i,
                len(bot_ids),
                bot_id,
                log_ctx,
            )

            bot = await self._bots.get_bot(bot_id)
            if bot is None:
                logger.warning("Bot not found in database | bot_id=%s | %s", bot_id, log_ctx)
                continue

            if not bot.enabled:
                logger.info("Bot disabled, skipping | bot_id=%s | %s", bot_id, log_ctx)
                continue

            logger.debug("Fetching pending orders | bot_id=%s symbol=%s", bot_id, sig.symbol)
            pendings = await self._orders.list_pending_order_states(
                bot_id,
                sig.symbol,
                sig.prev_side,  # OrderSide
            )

            logger.info(
                "Found %d pending order(s) | bot_id=%s | %s",
                len(pendings), bot_id, log_ctx,
            )

            for j, state in enumerate(pendings, 1):
                logger.debug(
                    "Checking pending order %d/%d | bot_id=%s order_id=%s side=%s status=%s",
                    j, len(pendings), bot_id, state.order_id or "N/A",
                    state.side.value, state.status.value,
                )

                if state.side != sig.prev_side:
                    logger.debug(
                        "Order side mismatch | order_side=%s disarm_prev_side=%s - skipping",
                        state.side.value, sig.prev_side.value,
                    )
                    continue

                if state.status not in (OrderStatus.PENDING, OrderStatus.ARMED):
                    logger.debug("Order not active | status=%s - skipping", state.status.value)
                    continue

                if not (state.order_id or state.stop_order_id or state.take_profit_order_id):
                    logger.warning("Pending order has no exchange ids | bot_id=%s state_id=%s", bot_id, state.id)
                    continue

                trading = await self._trading_factory(bot)
                cancel_targets = [
                    ("entry", state.order_id),
                    ("stop", state.stop_order_id),
                    ("take_profit", state.take_profit_order_id),
                ]

                cancelled_any = False
                for label, oid in cancel_targets:
                    if not oid:
                        continue
                    total_matching += 1
                    logger.info(
                        "Attempting to cancel %s order | bot_id=%s order_id=%s symbol=%s | %s",
                        label,
                        bot_id,
                        oid,
                        state.symbol,
                        log_ctx,
                    )
                    try:
                        await trading.cancel_order(symbol=state.symbol, order_id=int(oid))
                        cancelled.append(str(oid))
                        cancelled_any = True
                        logger.info(
                            "%s order cancelled | bot_id=%s order_id=%s | %s",
                            label.capitalize(),
                            bot_id,
                            oid,
                            log_ctx,
                        )
                    except Exception as exc:
                        logger.warning(
                            "Cancel %s failed | bot_id=%s order_id=%s disarm_reason=%s err=%s",
                            label,
                            bot.id,
                            oid,
                            sig.reason,
                            exc,
                        )

                if cancelled_any:
                    logger.debug("Marking order as CANCELLED | bot_id=%s state_id=%s", bot_id, state.id)
                    state.mark(OrderStatus.CANCELLED)
                    await self._orders.save_state(state)

        logger.info(
            "DISARM complete | found=%d cancelled=%d msg_id=%s | %s",
            total_matching, len(cancelled), message_id, format_log_context(context),
        )

        if cancelled:
            logger.info("Cancelled order IDs: %s | %s", ", ".join(cancelled), format_log_context(context))

        return cancelled
