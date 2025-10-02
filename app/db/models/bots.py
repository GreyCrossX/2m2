from __future__ import annotations
from uuid import uuid4
from decimal import Decimal
from sqlalchemy import (
    Column,
    String,
    Boolean,
    Integer,
    DateTime,
    ForeignKey,
    Index,
    Numeric,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from sqlalchemy import Enum as SAEnum

from app.db.base import Base

EnvEnum = SAEnum("testnet", "prod", name="env_enum", create_type=True)
SideWhitelistEnum = SAEnum("long", "short", "both", name="side_whitelist_enum", create_type=True)
SideModeEnum = SAEnum("both", "long_only", "short_only", name="side_mode_enum", create_type=True)
BotStatusEnum = SAEnum("active", "paused", "ended", name="bot_status_enum", create_type=True)



class Bot(Base):
    """
    Per-user trading bot settings. Runtime state stays in Redis.
    """
    __tablename__ = "bots"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), index=True)
    cred_id = Column(UUID(as_uuid=True), ForeignKey("api_credentials.id", ondelete="RESTRICT"), index=True)

    symbol = Column(String(32), nullable=False, index=True)
    timeframe = Column(String(8), nullable=False, default="2m") 
    enabled = Column(Boolean, nullable=False, default=True)
    env = Column(EnvEnum, nullable=False, server_default="testnet") 
    side_whitelist = Column(SideWhitelistEnum, nullable=False, default="both", server_default="both")
    side_mode = Column(SideModeEnum, nullable=False, default="both", server_default="both")
    leverage = Column(Integer, nullable=False, default=5)

    status = Column(BotStatusEnum, nullable=False, default="active", server_default="active")

    risk_per_trade = Column(
        Numeric(12, 8),
        nullable=False,
        default=Decimal("0.005"),
        server_default="0.00500000",
    )
    tp_ratio = Column(
        Numeric(8, 4),
        nullable=False,
        default=Decimal("1.5"),
        server_default="1.5000",
    )
    max_qty = Column(Numeric(18, 8), nullable=True)

    # --- sizing (MVP) ---
    use_balance_pct = Column(Boolean, nullable=False, default=True)
    balance_pct = Column(Numeric(6, 4), nullable=False, server_default="0.0500") 
    fixed_notional = Column(Numeric(18, 4), nullable=False, server_default="0.0000") 
    max_position_usdt = Column(Numeric(18, 4), nullable=False, server_default="0.0000") 

    # optional QoL
    nickname = Column(String(64), nullable=False, default="")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    user = relationship("User", back_populates="bots")
    credential = relationship("ApiCredential", back_populates="bots")

    __table_args__ = (
        
        Index("ix_bots_symbol_enabled", "symbol", "enabled"),
        
        Index("ix_bots_user_enabled", "user_id", "enabled"),
    )
