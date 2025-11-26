from __future__ import annotations

import logging
from decimal import Decimal
from typing import Optional

from ..application.order_executor import OrderExecutor
from ..domain.enums import OrderStatus
from ..domain.models import ArmSignal, DisarmSignal, OrderState
from ...infrastructure.binance.binance_trading import BinanceTrading


logger = logging.getLogger(__name__)


class BotWorker:
    """
    Handles order lifecycle for a single bot instance.
    This class keeps its own transient state; persistence/logging should be done
    by higher layers (e.g., Application SignalProcessor or an OrderGateway).

    Typical flow on ARM:
      - execute order (balance validation + limit order)
      - update internal state
      - (optional) place protective stop

    Typical flow on DISARM:
      - cancel pending order(s) if any
      - clear state
    """

    def __init__(
        self,
        bot,  # BotConfig
        order_executor: OrderExecutor,
        position_manager,  # Deprecated dependency retained for compatibility
        binance_trading: BinanceTrading,
    ):
        self._bot = bot
        self._order_executor = order_executor
        self._trading = binance_trading
        self._state: Optional[OrderState] = None

    async def handle_arm_signal(self, signal: ArmSignal, message_id: str) -> OrderState:
        # If side not allowed, mark as SKIPPED_WHITELIST (usually filtered earlier).
        if not self._bot.allows_side(signal.side):
            st = OrderState(
                bot_id=self._bot.id,
                signal_id=message_id,
                order_id=None,
                status=OrderStatus.SKIPPED_WHITELIST,
                side=signal.side,
                symbol=signal.symbol,
                trigger_price=signal.trigger,
                stop_price=signal.stop,
                quantity=Decimal("0"),
                filled_quantity=Decimal("0"),
            )
            self._state = st
            return st

        # Execute entry (balance validation, leverage, limit order)
        st = await self._order_executor.execute_order(self._bot, signal)
        # attach message id if needed by caller afterwards
        self._state = st

        return st

    async def handle_disarm_signal(self, signal: DisarmSignal, message_id: str) -> None:
        if not self._state:
            return

        st = self._state
        # If we have a pending entry order, try to cancel it.
        if st.status in (OrderStatus.PENDING, OrderStatus.ARMED) and st.order_id:
            try:
                await self._trading.cancel_order(
                    symbol=st.symbol, order_id=int(st.order_id)
                )
                st.status = OrderStatus.CANCELLED
            except Exception:
                # Cancellation failure will be logged upstream; we leave state untouched.
                pass

        # Keep state (caller may persist), then it can be cleared upstream if desired
        self._state = st
