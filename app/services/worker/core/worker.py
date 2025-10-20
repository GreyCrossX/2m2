from __future__ import annotations

from decimal import Decimal
from typing import Optional

from ..application.order_executor import OrderExecutor
from ..application.position_manager import PositionManager
from ..domain.enums import OrderStatus, Side
from ..domain.models import ArmSignal, DisarmSignal, OrderState
from ..infrastructure.binance.trading import BinanceTrading


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
        position_manager: PositionManager,
        binance_trading: BinanceTrading,
    ):
        self._bot = bot
        self._order_executor = order_executor
        self._position_manager = position_manager
        self._trading = binance_trading
        self._state: Optional[OrderState] = None

    async def handle_arm_signal(self, signal: ArmSignal, message_id: str) -> OrderState:
        # If side not allowed, mark as SKIPPED_WHITELIST (usually filtered earlier).
        if not (self._bot.side_whitelist == Side.BOTH or self._bot.side_whitelist == signal.side):
            # Let OrderExecutor set status when it returns; here we just avoid sending an order.
            st = OrderState(
                bot_id=self._bot.id,
                signal_id=message_id,
                order_id=None,
                status=OrderStatus.SKIPPED_WHITELIST,
                side=signal.side,
                symbol=signal.sym,
                trigger_price=signal.trigger,
                stop_price=signal.stop,
                quantity=Decimal("0"),
                created_at=signal.created_at,
                updated_at=signal.created_at,
            )
            self._state = st
            return st

        # Execute entry (balance validation, leverage, limit order)
        st = await self._order_executor.execute_order(self._bot, signal)
        self._state = st

        # Place protective stop (best-effort, only if quantity > 0 and order placed)
        try:
            if st.quantity > 0 and st.order_id:
                await self._trading.create_stop_market_order(
                    symbol=st.symbol,
                    side=st.side,
                    quantity=st.quantity,
                    stop_price=st.stop_price,
                )
        except Exception:
            # Stop placement failures should be logged by caller; we keep state as-is.
            pass

        return st

    async def handle_disarm_signal(self, signal: DisarmSignal, message_id: str) -> None:
        if not self._state:
            return

        st = self._state
        # If we have a pending entry order, try to cancel it.
        if st.status in (OrderStatus.PENDING, OrderStatus.ARMED) and st.order_id:
            try:
                await self._trading.cancel_order(symbol=st.symbol, order_id=int(st.order_id))
                st.status = OrderStatus.CANCELLED
            except Exception:
                # Cancellation failure will be logged upstream; we leave state untouched.
                pass

        # Clear after DISARM; caller may persist state before clearing.
        self._state = st

    def get_state(self) -> Optional[OrderState]:
        return self._state

    async def _check_order_fill(self) -> None:
        """
        Optional polling-based fill detection (if you don’t consume order updates).
        Caller is responsible for scheduling this periodically.
        """
        st = self._state
        if not st or not st.order_id or st.status != OrderStatus.PENDING:
            return
        try:
            status = await self._trading.get_order_status(symbol=st.symbol, order_id=int(st.order_id))
            if status == "FILLED":
                st.status = OrderStatus.FILLED
                await self._position_manager.open_position(self._bot.id, st)
                self._state = st
        except Exception:
            # Swallow—caller will decide about retries/logging.
            pass
