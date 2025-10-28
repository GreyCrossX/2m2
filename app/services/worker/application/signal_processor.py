from __future__ import annotations

import logging
from decimal import Decimal
from typing import Awaitable, Callable, Dict, Iterable, List, Optional, Protocol
from uuid import UUID

from ..core.logging_utils import ensure_log_context, format_log_context
from ..domain.enums import OrderStatus, Side
from ..domain.exceptions import InvalidSignalException
from ..domain.models import ArmSignal, BotConfig, DisarmSignal, OrderState

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
        side: Side,
    ) -> List[OrderState]: ...

    async def save_state(self, state: OrderState) -> None: ...


class OrderExecutorPort(Protocol):
    """Use case that actually creates an order (we'll inject OrderExecutor)."""

    async def execute_order(self, bot: BotConfig, signal: ArmSignal) -> OrderState: ...


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
    ) -> None:
        self._router = router
        self._bots = bot_repository
        self._executor = order_executor
        self._orders = order_gateway
        self._trading_factory = trading_factory
        logger.info("SignalProcessor initialized")

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

        signal.signal_msg_id = message_id
        context = ensure_log_context(
            log_context,
            symbol=signal.symbol,
            tf=signal.timeframe,
            type=signal.type.value,
            msg_id=message_id,
            prev_side=log_context.get("prev_side") if log_context else "-",
        )
        logger.info(
            "Processing ARM signal | side=%s trigger=%s stop=%s | %s",
            signal.side.value,
            signal.trigger,
            signal.stop,
            format_log_context(context),
        )

        results: List[OrderState] = []
        bot_ids = list(self._router.get_bot_ids(signal.symbol, signal.timeframe))

        logger.info(
            "Found %d bot(s) subscribed | %s",
            len(bot_ids),
            format_log_context(context),
        )

        if not bot_ids:
            logger.warning(
                "No bots subscribed - signal will not be processed | %s",
                format_log_context(context),
            )
            return results

        for i, bot_id in enumerate(bot_ids, 1):
            logger.debug("Processing bot %d/%d | bot_id=%s", i, len(bot_ids), bot_id)

            bot = await self._bots.get_bot(bot_id)
            if bot is None:
                logger.warning("Bot not found in database | bot_id=%s", bot_id)
                continue

            if not bot.enabled:
                logger.info(
                    "Bot disabled, skipping | bot_id=%s | %s",
                    bot_id,
                    format_log_context(context),
                )
                continue

            logger.info(
                "Checking side whitelist | bot_id=%s bot_whitelist=%s signal_side=%s | %s",
                bot_id,
                bot.side_whitelist.value,
                signal.side.value,
                format_log_context(context),
            )

            if not bot.allows_side(signal.side):
                logger.info(
                    "Bot side whitelist blocked signal | bot_id=%s whitelist=%s signal_side=%s | %s",
                    bot_id,
                    bot.side_whitelist.value,
                    signal.side.value,
                    format_log_context(context),
                )
                state = OrderState(
                    bot_id=bot.id,
                    signal_id=message_id,
                    status=OrderStatus.SKIPPED_WHITELIST,
                    side=signal.side,
                    symbol=signal.symbol,
                    trigger_price=signal.trigger,
                    stop_price=signal.stop,
                    quantity=Decimal("0"),
                )
                logger.debug("Saving SKIPPED_WHITELIST state | bot_id=%s", bot_id)
                await self._orders.save_state(state)
                results.append(state)
                continue

            logger.info(
                "Executing order | bot_id=%s side=%s trigger=%s | %s",
                bot_id,
                signal.side.value,
                signal.trigger,
                format_log_context(context),
            )

            try:
                state = await self._executor.execute_order(bot, signal)
                state.signal_id = message_id
                state.touch()

                logger.info(
                    "Order executed | bot_id=%s status=%s qty=%s order_id=%s | %s",
                    bot_id,
                    state.status.value,
                    state.quantity,
                    state.order_id or "N/A",
                    format_log_context(context),
                )

                logger.debug("Saving order state | bot_id=%s status=%s", bot_id, state.status.value)
                await self._orders.save_state(state)
                results.append(state)
            except Exception as e:
                logger.error(
                    "Order execution failed | bot_id=%s symbol=%s err=%s",
                    bot_id,
                    signal.symbol,
                    e,
                    exc_info=True,
                )
                continue

        logger.info(
            "ARM signal processing complete | bots_processed=%d results=%d | %s",
            len(bot_ids),
            len(results),
            format_log_context(context),
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
        signal.signal_msg_id = message_id
        context = ensure_log_context(
            log_context,
            symbol=signal.symbol,
            tf=signal.timeframe,
            type=signal.type.value,
            msg_id=message_id,
            prev_side=signal.prev_side.value,
        )
        logger.info(
            "Processing DISARM signal | reason=%s | %s",
            signal.reason,
            format_log_context(context),
        )

        cancelled: List[str] = []
        bot_ids = list(self._router.get_bot_ids(signal.symbol, signal.timeframe))

        logger.info(
            "Found %d bot(s) subscribed | %s",
            len(bot_ids),
            format_log_context(context),
        )

        if not bot_ids:
            logger.warning(
                "No bots subscribed - DISARM has no effect | %s",
                format_log_context(context),
            )
            return cancelled

        total_matching = 0
        for i, bot_id in enumerate(bot_ids, 1):
            logger.debug("Processing bot %d/%d for cancellation | bot_id=%s", i, len(bot_ids), bot_id)

            bot = await self._bots.get_bot(bot_id)
            if bot is None:
                logger.warning("Bot not found in database | bot_id=%s", bot_id)
                continue

            if not bot.enabled:
                logger.info(
                    "Bot disabled, skipping | bot_id=%s | %s",
                    bot_id,
                    format_log_context(context),
                )
                continue

            logger.debug("Fetching pending orders | bot_id=%s symbol=%s", bot_id, signal.symbol)
            pendings = await self._orders.list_pending_order_states(
                bot_id,
                signal.symbol,
                signal.prev_side,
            )

            logger.info(
                "Found %d pending order(s) | bot_id=%s | %s",
                len(pendings),
                bot_id,
                format_log_context(context),
            )

            for j, state in enumerate(pendings, 1):
                logger.debug(
                    "Checking pending order %d/%d | bot_id=%s order_id=%s side=%s status=%s",
                    j,
                    len(pendings),
                    bot_id,
                    state.order_id or "N/A",
                    state.side.value,
                    state.status.value,
                )

                if state.side != signal.prev_side:
                    logger.debug(
                        "Order side mismatch | order_side=%s disarm_prev_side=%s - skipping",
                        state.side.value,
                        signal.prev_side.value,
                    )
                    continue

                if state.status not in (OrderStatus.PENDING, OrderStatus.ARMED):
                    logger.debug("Order not active | status=%s - skipping", state.status.value)
                    continue

                if not state.order_id:
                    logger.warning(
                        "Pending order has no order_id | bot_id=%s state_id=%s",
                        bot_id,
                        state.id,
                    )
                    continue

                total_matching += 1
                logger.info(
                    "Attempting to cancel order | bot_id=%s order_id=%s symbol=%s | %s",
                    bot_id,
                    state.order_id,
                    state.symbol,
                    format_log_context(context),
                )

                try:
                    trading = await self._trading_factory(bot)
                    await trading.cancel_order(symbol=state.symbol, order_id=int(state.order_id))
                    logger.info(
                        "Order cancelled successfully | bot_id=%s order_id=%s | %s",
                        bot_id,
                        state.order_id,
                        format_log_context(context),
                    )
                except Exception as exc:
                    logger.warning(
                        "Cancel order failed | bot_id=%s order_id=%s disarm_reason=%s err=%s",
                        bot.id,
                        state.order_id,
                        signal.reason,
                        exc,
                    )
                    continue

                logger.debug("Marking order as CANCELLED | bot_id=%s order_id=%s", bot_id, state.order_id)
                state.mark(OrderStatus.CANCELLED)
                await self._orders.save_state(state)
                cancelled.append(str(state.order_id))

        logger.info(
            "DISARM complete | found=%d cancelled=%d msg_id=%s | %s",
            total_matching,
            len(cancelled),
            message_id,
            format_log_context(context),
        )

        if cancelled:
            logger.info(
                "Cancelled order IDs: %s | %s",
                ", ".join(cancelled),
                format_log_context(context),
            )

        return cancelled
