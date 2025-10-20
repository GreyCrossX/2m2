from __future__ import annotations

from typing import List, Protocol, Iterable, Optional
from uuid import UUID

from decimal import Decimal

from ..domain.models import ArmSignal, DisarmSignal, BotConfig, OrderState
from ..domain.enums import Side, OrderStatus
from ..domain.exceptions import InvalidSignalException


# --------- Ports (to be implemented by Core/Infra) ---------

class SymbolRouter(Protocol):
    """Port for routing/lookup: which bots are subscribed to a (symbol, timeframe)."""
    async def get_bot_ids(self, symbol: str, timeframe: str) -> Iterable[UUID]: ...


class BotRepository(Protocol):
    """Port to load BotConfig."""
    async def get_bot(self, bot_id: UUID) -> Optional[BotConfig]: ...


class OrderGateway(Protocol):
    """
    Port for order state persistence and cancellation surfacing.
    Infra should back this with DB + Binance trading client.
    """
    async def list_pending_order_states(self, bot_id: UUID, symbol: str) -> List[OrderState]: ...
    async def cancel_order(self, state: OrderState, reason: str) -> bool: ...
    async def save_state(self, state: OrderState) -> None: ...


class OrderExecutorPort(Protocol):
    """Use case that actually creates an order (we'll inject OrderExecutor)."""
    async def execute_order(self, bot: BotConfig, signal: ArmSignal) -> OrderState: ...


# --------- Use Case ---------

class SignalProcessor:
    """
    Orchestrates ARM/DISARM handling across subscribed bots.
    Produces OrderState records but leaves persistence to OrderGateway.
    """

    def __init__(
        self,
        router: SymbolRouter,
        bot_repository: BotRepository,
        order_executor: OrderExecutorPort,
        order_gateway: OrderGateway,
    ) -> None:
        self._router = router
        self._bots = bot_repository
        self._executor = order_executor
        self._orders = order_gateway

    async def process_arm_signal(
        self,
        signal: ArmSignal,
        message_id: str
    ) -> List[OrderState]:
        """
        1) Discover subscribed bots via router
        2) Load BotConfig and filter (enabled, side_whitelist)
        3) Execute order (balance, sizing, place order)
        4) Persist states via OrderGateway
        5) Return list of resulting OrderState (pending/failed/skipped)
        """
        if signal.symbol != signal.symbol.upper():
            # Defensive: domain already uppercases, but keep strictness here.
            raise InvalidSignalException("Signal symbol must be uppercased.")

        out: List[OrderState] = []
        signal_msg_id = message_id

        for_bot_ids = await self._router.get_bot_ids(signal.symbol, signal.timeframe)
        bot_ids: List[UUID] = list(for_bot_ids) if not isinstance(for_bot_ids, list) else for_bot_ids

        for bot_id in bot_ids:
            bot = await self._bots.get_bot(bot_id)
            if bot is None:
                continue
            if not bot.enabled:
                continue
            if not bot.allows_side(signal.side):
                # Persist a skipped_whitelist state for observability
                state = OrderState(
                    bot_id=bot.id,
                    signal_id=signal_msg_id,
                    status=OrderStatus.SKIPPED_WHITELIST,
                    side=signal.side,
                    symbol=signal.symbol,
                    trigger_price=signal.trigger,
                    stop_price=signal.stop,
                    quantity=Decimal("0"),
                )
                await self._orders.save_state(state)
                out.append(state)
                continue

            # Execute the order path
            state = await self._executor.execute_order(bot, signal)
            state.signal_id = signal_msg_id  # ensure linkage if executor set it earlier or not
            await self._orders.save_state(state)
            out.append(state)

        return out

    async def process_disarm_signal(
        self,
        signal: DisarmSignal,
        message_id: str
    ) -> List[str]:
        """
        1) Discover subscribed bots
        2) For each, load pending OrderState and cancel them
        3) Persist new CANCELLED states
        4) Return list of cancelled order_ids (as strings)
        """
        cancelled: List[str] = []

        for_bot_ids = await self._router.get_bot_ids(signal.symbol, signal.timeframe)
        bot_ids: List[UUID] = list(for_bot_ids) if not isinstance(for_bot_ids, list) else for_bot_ids

        for bot_id in bot_ids:
            bot = await self._bots.get_bot(bot_id)
            if bot is None or not bot.enabled:
                continue

            # Cancel only if the pending order matches previous side intent.
            pendings = await self._orders.list_pending_order_states(bot_id, signal.symbol)
            for st in pendings:
                if st.side == signal.prev_side and st.status == OrderStatus.PENDING:
                    ok = await self._orders.cancel_order(st, reason=f"DISARM:{signal.reason}")
                    if ok:
                        cancelled.append(str(st.order_id or ""))

        return cancelled