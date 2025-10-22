from __future__ import annotations

import logging
from decimal import Decimal
from typing import Awaitable, Callable, Iterable, List, Optional, Protocol
from uuid import UUID

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

    async def list_pending_order_states(self, bot_id: UUID, symbol: str) -> List[OrderState]: ...
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

    async def process_arm_signal(self, signal: ArmSignal, message_id: str) -> List[OrderState]:
        """Handle ARM signals by placing orders for every subscribed bot."""
        if signal.symbol != signal.symbol.upper():
            raise InvalidSignalException("Signal symbol must be uppercased.")

        results: List[OrderState] = []
        bot_ids = list(self._router.get_bot_ids(signal.symbol, signal.timeframe))

        for bot_id in bot_ids:
            bot = await self._bots.get_bot(bot_id)
            if bot is None or not bot.enabled:
                continue

            if not bot.allows_side(signal.side):
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
                await self._orders.save_state(state)
                results.append(state)
                continue

            state = await self._executor.execute_order(bot, signal)
            state.signal_id = message_id
            state.touch()
            await self._orders.save_state(state)
            results.append(state)

        return results

    async def process_disarm_signal(self, signal: DisarmSignal, message_id: str) -> List[str]:
        """Cancel pending orders matching the DISARM semantics."""
        cancelled: List[str] = []
        bot_ids = list(self._router.get_bot_ids(signal.symbol, signal.timeframe))

        for bot_id in bot_ids:
            bot = await self._bots.get_bot(bot_id)
            if bot is None or not bot.enabled:
                continue

            pendings = await self._orders.list_pending_order_states(bot_id, signal.symbol)
            for state in pendings:
                if state.side != signal.prev_side or state.status != OrderStatus.PENDING:
                    continue
                if not state.order_id:
                    continue

                try:
                    trading = await self._trading_factory(bot)
                    await trading.cancel_order(symbol=state.symbol, order_id=int(state.order_id))
                except Exception as exc:  # pragma: no cover - best effort, surfaces via logs
                    logger.warning(
                        "Cancel order failed | bot=%s order_id=%s disarm_reason=%s err=%s",
                        bot.id,
                        state.order_id,
                        signal.reason,
                        exc,
                    )
                    continue

                state.mark(OrderStatus.CANCELLED)
                await self._orders.save_state(state)
                cancelled.append(str(state.order_id))

        return cancelled
