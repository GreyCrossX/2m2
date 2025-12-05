from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from typing import Dict, List, Optional
from uuid import UUID

from ..domain.enums import OrderSide, OrderStatus
from ..domain.exceptions import WorkerException
from ..domain.models import OrderState, Position


class PositionManager:
    """In-memory position tracker supporting pyramiding and rehydration."""

    def __init__(self, *, tp_r_multiple: Decimal = Decimal("1.5")) -> None:
        self._tp_r = tp_r_multiple
        self._positions: Dict[UUID, Position] = {}
        self._layers: Dict[UUID, List[Position]] = {}

    def _compute_take_profit(
        self,
        side: OrderSide,
        entry: Decimal,
        stop: Decimal,
        *,
        tp_r_multiple: Decimal | None = None,
    ) -> Decimal:
        distance = (entry - stop) if side == OrderSide.LONG else (stop - entry)
        if distance <= 0:
            distance = abs(entry - stop)
        r = tp_r_multiple if tp_r_multiple is not None else self._tp_r
        if side == OrderSide.LONG:
            return entry + r * distance
        return entry - r * distance

    async def open_position(
        self,
        bot_id: UUID,
        order_state: OrderState,
        *,
        allow_pyramiding: bool = False,
        tp_r_multiple: Decimal | None = None,
    ) -> Position:
        if order_state.status not in (OrderStatus.FILLED, OrderStatus.ARMED):
            raise WorkerException("open_position requires FILLED/ARMED order state.")

        entry = order_state.avg_fill_price or order_state.trigger_price
        qty = order_state.filled_quantity or order_state.quantity
        stop = order_state.stop_price

        if entry <= 0 or stop <= 0:
            raise WorkerException("Invalid entry/stop prices for position.")
        if qty <= 0:
            raise WorkerException("open_position requires positive quantity.")

        take_profit = self._compute_take_profit(
            order_state.side, entry, stop, tp_r_multiple=tp_r_multiple
        )
        layer = Position(
            bot_id=bot_id,
            symbol=order_state.symbol,
            side=order_state.side,
            entry_price=entry,
            quantity=qty,
            stop_loss=stop,
            take_profit=take_profit,
        )

        existing = self._positions.get(bot_id)
        if existing and allow_pyramiding and existing.side == order_state.side:
            total_qty = existing.quantity + qty
            if total_qty <= 0:
                raise WorkerException(
                    "Pyramiding update resulted in non-positive quantity."
                )
            weighted_entry = (
                (existing.entry_price * existing.quantity) + (entry * qty)
            ) / total_qty
            existing.entry_price = weighted_entry
            existing.quantity = total_qty
            existing.stop_loss = stop
            existing.take_profit = self._compute_take_profit(
                order_state.side, weighted_entry, stop, tp_r_multiple=tp_r_multiple
            )
            self._layers.setdefault(bot_id, []).append(layer)
            return existing

        self._positions[bot_id] = layer
        self._layers[bot_id] = [layer]
        return layer

    async def close_position(self, bot_id: UUID, reason: str) -> None:
        self._positions.pop(bot_id, None)
        self._layers.pop(bot_id, None)

    def set_position(self, bot_id: UUID, position: Position) -> None:
        self._positions[bot_id] = position
        self._layers[bot_id] = [replace(position)]

    def get_position(self, bot_id: UUID) -> Optional[Position]:
        return self._positions.get(bot_id)

    def get_positions(self, bot_id: UUID) -> List[Position]:
        layers = self._layers.get(bot_id)
        if layers:
            return list(layers)
        aggregate = self._positions.get(bot_id)
        return [aggregate] if aggregate else []
