# app/db/models/order_states.py
from __future__ import annotations

from uuid import uuid4

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Enum as SAEnum,
    ForeignKey,
    Numeric,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PGUUID

from app.db.base import Base

_ORDER_STATUS_VALUES = (
    "armed",
    "pending",
    "filled",
    "cancelled",
    "failed",
    "skipped_low_balance",
    "skipped_whitelist",
)

# NEW: dedicated enum for order side (NO "both" here)
_ORDER_SIDE_VALUES = ("long", "short")


class OrderStateRecord(Base):
    """Persistent order lifecycle state for worker-driven executions."""

    __tablename__ = "order_states"

    id = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    bot_id = Column(
        PGUUID(as_uuid=True),
        ForeignKey("bots.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    # Redis stream message id (not a UUID)
    signal_id = Column(String(64), nullable=False)
    order_id = Column(BigInteger, nullable=True)
    stop_order_id = Column(BigInteger, nullable=True)
    take_profit_order_id = Column(BigInteger, nullable=True)

    status = Column(
        SAEnum(*_ORDER_STATUS_VALUES, name="order_status_enum", create_type=False),
        nullable=False,
    )
    # ✅ use a dedicated enum for order sides
    side = Column(
        SAEnum(*_ORDER_SIDE_VALUES, name="order_side_enum", create_type=False),
        nullable=False,
    )

    symbol = Column(String(32), nullable=False, index=True)

    trigger_price = Column(Numeric(18, 8), nullable=False)
    stop_price = Column(Numeric(18, 8), nullable=False)
    quantity = Column(Numeric(18, 8), nullable=False)

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
        onupdate=text("now()"),
    )

    __table_args__ = (
        UniqueConstraint("bot_id", "signal_id", name="uq_orderstate_bot_signal"),
    )
