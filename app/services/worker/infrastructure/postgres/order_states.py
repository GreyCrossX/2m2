from __future__ import annotations

from decimal import Decimal
from typing import List, Optional, Sequence
from uuid import UUID

from sqlalchemy import insert, select, update, and_
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models.order_states import OrderStateRecord

# Domain
from ...domain.enums import OrderStatus, OrderSide
from ...domain.models import OrderState


# ------------------ mappers ------------------


def _from_domain(s: OrderState) -> dict:
    """
    Domain -> ORM payload.
    Uses enum .value (strings) because DB columns are backed by postgres enums.
    """
    return dict(
        id=s.id,
        bot_id=s.bot_id,
        signal_id=s.signal_id,
        order_id=s.order_id,
        stop_order_id=s.stop_order_id,
        take_profit_order_id=s.take_profit_order_id,
        status=s.status.value,
        side=s.side.value,
        symbol=s.symbol,
        trigger_price=Decimal(s.trigger_price),
        stop_price=Decimal(s.stop_price),
        quantity=Decimal(s.quantity),
        filled_quantity=Decimal(s.filled_quantity or 0),
        avg_fill_price=Decimal(s.avg_fill_price)
        if s.avg_fill_price is not None
        else None,
        last_fill_at=s.last_fill_at,
        created_at=s.created_at,
        updated_at=s.updated_at,
    )


def _coerce_order_status(v) -> OrderStatus:
    if isinstance(v, OrderStatus):
        return v
    raw = getattr(v, "value", v)
    return OrderStatus(str(raw))


def _coerce_order_side(v) -> OrderSide:
    if isinstance(v, OrderSide):
        return v
    raw = getattr(v, "value", v)
    return OrderSide(str(raw))


def _to_domain(r: OrderStateRecord) -> OrderState:
    """
    ORM row -> Domain.
    Accept both DB enum objects and plain strings.
    """
    return OrderState(
        id=r.id,
        bot_id=r.bot_id,
        signal_id=r.signal_id,
        order_id=r.order_id,
        stop_order_id=r.stop_order_id,
        take_profit_order_id=r.take_profit_order_id,
        status=_coerce_order_status(r.status),
        side=_coerce_order_side(r.side),
        symbol=r.symbol,
        trigger_price=Decimal(r.trigger_price),
        stop_price=Decimal(r.stop_price),
        quantity=Decimal(r.quantity),
        filled_quantity=Decimal(getattr(r, "filled_quantity", 0) or 0),
        avg_fill_price=(
            Decimal(r.avg_fill_price)
            if getattr(r, "avg_fill_price", None) is not None
            else None
        ),
        last_fill_at=getattr(r, "last_fill_at", None),
        created_at=r.created_at,
        updated_at=r.updated_at,
    )


# ------------------ gateway ------------------


class OrderGateway:
    """Persistence adapter for OrderState backed by PostgreSQL."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self._session_factory = session_factory

    async def save_state(self, state: OrderState) -> None:
        """
        Upsert by (bot_id, signal_id).
        Uses a two-step exists/update to keep it portable; if you prefer
        a single statement, switch to PostgreSQL ON CONFLICT in the ORM model.
        """
        payload = _from_domain(state)
        async with self._session_factory() as session:
            async with session.begin():
                exists_stmt = select(OrderStateRecord.id).where(
                    OrderStateRecord.bot_id == state.bot_id,
                    OrderStateRecord.signal_id == state.signal_id,
                )
                res = await session.execute(exists_stmt)
                existing_id = res.scalars().first()

                if existing_id:
                    await session.execute(
                        update(OrderStateRecord)
                        .where(
                            OrderStateRecord.bot_id == state.bot_id,
                            OrderStateRecord.signal_id == state.signal_id,
                        )
                        .values(**payload)
                    )
                else:
                    await session.execute(insert(OrderStateRecord).values(**payload))

    async def list_pending_order_states(
        self,
        bot_id: UUID,
        symbol: str,
        side: OrderSide,
        statuses: Optional[Sequence[OrderStatus]] = None,
    ) -> List[OrderState]:
        """
        Return active orders for (bot, symbol, side).
        IMPORTANT: ignore rows with NULL order_id to prevent dangling "pending" without an exchange id.
        Default statuses = (PENDING, ARMED).
        """
        active_statuses = tuple(statuses or (OrderStatus.PENDING, OrderStatus.ARMED))
        async with self._session_factory() as session:
            stmt = select(OrderStateRecord).where(
                and_(
                    OrderStateRecord.bot_id == bot_id,
                    OrderStateRecord.symbol == symbol,
                    OrderStateRecord.side == side.value,
                    OrderStateRecord.status.in_([s.value for s in active_statuses]),
                    OrderStateRecord.order_id.isnot(None),  # << key change
                )
            )
            res = await session.execute(stmt)
            return [_to_domain(r) for r in res.scalars().all()]

    async def list_states_by_statuses(
        self,
        statuses: Sequence[OrderStatus],
    ) -> List[OrderState]:
        async with self._session_factory() as session:
            stmt = (
                select(OrderStateRecord)
                .where(OrderStateRecord.status.in_([s.value for s in statuses]))
                .order_by(OrderStateRecord.updated_at.desc())
            )
            res = await session.execute(stmt)
            return [_to_domain(r) for r in res.scalars().all()]

    async def get_latest(self, bot_id: UUID) -> Optional[OrderState]:
        """
        Get the most recently updated OrderState for this bot (if any).
        """
        async with self._session_factory() as session:
            stmt = (
                select(OrderStateRecord)
                .where(OrderStateRecord.bot_id == bot_id)
                .order_by(OrderStateRecord.updated_at.desc())
                .limit(1)
            )
            res = await session.execute(stmt)
            row = res.scalars().first()
            return _to_domain(row) if row else None
