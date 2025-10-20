from __future__ import annotations

from decimal import Decimal
from typing import Dict, Optional, Protocol
from uuid import UUID

from ..domain.models import Position, OrderState
from ..domain.enums import OrderStatus, Side
from ..domain.exceptions import WorkerException


# --------- Ports (to be implemented by Infra) ---------

class BinanceClient(Protocol):
    async def close_position_market(self, symbol: str, side: str, quantity: Decimal) -> dict: ...
    async def get_mark_price(self, symbol: str) -> Decimal: ...
    async def get_open_position_qty(self, symbol: str) -> Decimal: ...


# --------- Use Case ---------

class PositionManager:
    """
    In-memory position tracker with helpers to open/close and to sync from Binance.
    Infra (websocket/userData stream) should call into this when fills occur, or you
    can call `open_position` from an order-fill handler in core.
    """

    def __init__(
        self,
        binance_client: BinanceClient,
        *,
        tp_r_multiple: Decimal = Decimal("1.5"),
    ) -> None:
        self._bx = binance_client
        self._positions: Dict[UUID, Position] = {}
        self._tp_r = tp_r_multiple

    async def open_position(
        self,
        bot_id: UUID,
        order_state: OrderState,
    ) -> Position:
        """
        Create a position entry after a PENDING order becomes FILLED.
        We compute a simple R-multiple take-profit using (entry - stop) distance.
        """
        if order_state.status != OrderStatus.FILLED:
            raise WorkerException("open_position requires FILLED order state.")
        if order_state.quantity <= 0:
            raise WorkerException("open_position requires positive quantity.")

        entry = order_state.trigger_price
        stop = order_state.stop_price
        if entry <= 0 or stop <= 0:
            raise WorkerException("Invalid entry/stop prices for position.")

        distance = (entry - stop) if order_state.side == Side.LONG else (stop - entry)
        if distance <= 0:
            # If stop is not on the protective side, keep a minimal band
            distance = abs(entry - stop)

        if order_state.side == Side.LONG:
            take_profit = entry + self._tp_r * distance
        else:
            take_profit = entry - self._tp_r * distance

        pos = Position(
            bot_id=bot_id,
            symbol=order_state.symbol,
            side=order_state.side,
            entry_price=entry,
            quantity=order_state.quantity,
            stop_loss=stop,
            take_profit=take_profit,
        )
        self._positions[bot_id] = pos
        return pos

    async def close_position(
        self,
        bot_id: UUID,
        reason: str,
    ) -> None:
        """
        Close at market using current qty and side inversion (reduce-only by infra).
        """
        pos = self._positions.get(bot_id)
        if not pos:
            return
        side_str = "SELL" if pos.side == Side.LONG else "BUY"
        qty = pos.quantity
        if qty > 0:
            await self._bx.close_position_market(pos.symbol, side_str, qty)
        self._positions.pop(bot_id, None)

    def get_position(self, bot_id: UUID) -> Optional[Position]:
        return self._positions.get(bot_id)

    async def sync_positions_from_binance(self) -> None:
        """
        Optional: reconcile from exchange (useful on restarts).
        Infra must define mapping bot_id -> symbol/side externally if needed.
        This method is intentionally minimal; implement richer sync in core/infra.
        """
        # Intentionally left as a hook. Example flow if you track symbol per bot:
        # for bot_id, pos in list(self._positions.items()):
        #     on_exchange_qty = await self._bx.get_open_position_qty(pos.symbol)
        #     if on_exchange_qty == 0:
        #         self._positions.pop(bot_id, None)
        return
