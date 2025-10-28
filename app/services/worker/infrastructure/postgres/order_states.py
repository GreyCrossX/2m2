from __future__ import annotations

from decimal import Decimal
from typing import List, Optional, Sequence
from uuid import UUID

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models.order_states import OrderStateRecord

from ...domain.enums import OrderStatus, Side
from ...domain.models import OrderState


def _from_domain(s: OrderState) -> dict:
    return dict(
        id=s.id,
        bot_id=s.bot_id,
        signal_id=s.signal_id,
        order_id=s.order_id,
        status=s.status.value,
        side=s.side.value,
        symbol=s.symbol,
        trigger_price=Decimal(s.trigger_price),
        stop_price=Decimal(s.stop_price),
        quantity=Decimal(s.quantity),
        created_at=s.created_at,
        updated_at=s.updated_at,
    )


def _to_domain(r: OrderStateRecord) -> OrderState:
    status_raw = r.status.value if hasattr(r.status, "value") else r.status
    side_raw = r.side.value if hasattr(r.side, "value") else r.side
    status = OrderStatus(str(status_raw))
    side = Side(str(side_raw))
    return OrderState(
        id=r.id,
        bot_id=r.bot_id,
        signal_id=r.signal_id,
        order_id=r.order_id,
        status=status,
        side=side,
        symbol=r.symbol,
        trigger_price=Decimal(r.trigger_price),
        stop_price=Decimal(r.stop_price),
        quantity=Decimal(r.quantity),
        created_at=r.created_at,
        updated_at=r.updated_at,
    )


class OrderGateway:
    """Persistence adapter for OrderState backed by PostgreSQL."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self._session_factory = session_factory

    async def save_state(self, state: OrderState) -> None:
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
        side: Side,
        statuses: Optional[Sequence[OrderStatus]] = None,
    ) -> List[OrderState]:
        async with self._session_factory() as session:
            active_statuses = tuple(statuses or (OrderStatus.PENDING, OrderStatus.ARMED))
            stmt = select(OrderStateRecord).where(
                OrderStateRecord.bot_id == bot_id,
                OrderStateRecord.symbol == symbol,
                OrderStateRecord.side == side.value,
                OrderStateRecord.status.in_([status.value for status in active_statuses]),
            )
            res = await session.execute(stmt)
            return [_to_domain(r) for r in res.scalars().all()]

    async def get_latest(self, bot_id: UUID) -> Optional[OrderState]:
        async with self._session_factory() as session:
            stmt = (
                select(OrderStateRecord)
                .where(OrderStateRecord.bot_id == bot_id)
                .order_by(OrderStateRecord.updated_at.desc())
                .limit(1)
            )
            res = await session.execute(stmt)
            r = res.scalars().first()
            return _to_domain(r) if r else None
