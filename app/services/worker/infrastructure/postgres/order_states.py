from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from decimal import Decimal
from typing import List, Optional
from uuid import UUID

from sqlalchemy import BigInteger, Column, DateTime, Enum as SAEnum, Numeric, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import declarative_base
from sqlalchemy import select, insert, update

from ...domain.enums import OrderStatus, Side
from ...domain.models import OrderState

Base = declarative_base()

OrderStatusEnum = SAEnum(
    OrderStatus.ARMED.value,
    OrderStatus.PENDING.value,
    OrderStatus.FILLED.value,
    OrderStatus.CANCELLED.value,
    OrderStatus.FAILED.value,
    OrderStatus.SKIPPED_LOW_BALANCE.value,
    OrderStatus.SKIPPED_WHITELIST.value,
    name="order_status_enum",
    create_type=False,  # assume created elsewhere; set True if you manage enums here
)

SideEnum = SAEnum(
    Side.LONG.value,
    Side.SHORT.value,
    Side.BOTH.value,
    name="side_enum_worker",
    create_type=False,
)

class OrderStateORM(Base):
    __tablename__ = "order_states"

    id = Column(PGUUID(as_uuid=True), primary_key=True)
    bot_id = Column(PGUUID(as_uuid=True), index=True, nullable=False)
    signal_id = Column(String(64), nullable=False)  # redis message id
    order_id = Column(BigInteger, nullable=True)    # binance order id
    status = Column(OrderStatusEnum, nullable=False)

    side = Column(SAEnum(Side, name="side_whitelist_enum", create_type=False), nullable=False)
    symbol = Column(String(32), index=True, nullable=False)

    trigger_price = Column(Numeric(18, 8), nullable=False)
    stop_price = Column(Numeric(18, 8), nullable=False)
    quantity = Column(Numeric(18, 8), nullable=False)

    created_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint("bot_id", "signal_id", name="uq_orderstate_bot_signal"),
    )


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


def _to_domain(r: OrderStateORM) -> OrderState:
    return OrderState(
        id=r.id,
        bot_id=r.bot_id,
        signal_id=r.signal_id,
        order_id=r.order_id,
        status=OrderStatus(r.status),
        side=Side(r.side),
        symbol=r.symbol,
        trigger_price=Decimal(r.trigger_price),
        stop_price=Decimal(r.stop_price),
        quantity=Decimal(r.quantity),
        created_at=r.created_at,
        updated_at=r.updated_at,
    )


class OrderGateway:
    """Persistence adapter for OrderState."""

    def __init__(self, session: AsyncSession):
        self._session = session

    async def save_state(self, state: OrderState) -> None:
        # Upsert by (bot_id, signal_id)
        exists_stmt = select(OrderStateORM).where(
            OrderStateORM.bot_id == state.bot_id, OrderStateORM.signal_id == state.signal_id
        )
        res = await self._session.execute(exists_stmt)
        row = res.scalars().first()
        if row:
            upd = (
                update(OrderStateORM)
                .where(OrderStateORM.bot_id == state.bot_id, OrderStateORM.signal_id == state.signal_id)
                .values(**_from_domain(state))
            )
            await self._session.execute(upd)
        else:
            ins = insert(OrderStateORM).values(**_from_domain(state))
            await self._session.execute(ins)
        await self._session.commit()

    async def list_pending(self, bot_id: UUID, symbol: str) -> List[OrderState]:
        stmt = select(OrderStateORM).where(
            OrderStateORM.bot_id == bot_id,
            OrderStateORM.symbol == symbol,
            OrderStateORM.status.in_([OrderStatus.PENDING.value, OrderStatus.ARMED.value]),
        )
        res = await self._session.execute(stmt)
        return [_to_domain(r) for r in res.scalars().all()]

    async def get_latest(self, bot_id: UUID) -> Optional[OrderState]:
        stmt = (
            select(OrderStateORM)
            .where(OrderStateORM.bot_id == bot_id)
            .order_by(OrderStateORM.updated_at.desc())
            .limit(1)
        )
        res = await self._session.execute(stmt)
        r = res.scalars().first()
        return _to_domain(r) if r else None
