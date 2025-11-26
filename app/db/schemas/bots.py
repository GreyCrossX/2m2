from __future__ import annotations
from datetime import datetime
from uuid import UUID
from decimal import Decimal
from typing import Literal, Optional
from pydantic import BaseModel, Field, field_validator

Env = Literal["testnet", "prod"]
Side = Literal["long", "short", "both"]


class BotBase(BaseModel):
    symbol: str = Field(min_length=1, max_length=32)
    timeframe: Literal["2m"] = "2m"
    enabled: bool = True
    env: Env = "testnet"
    side_whitelist: Side = "both"
    leverage: int = Field(default=5, ge=1, le=125)

    use_balance_pct: bool = True
    balance_pct: Decimal = Field(
        default=Decimal("0.0500"), ge=Decimal("0"), le=Decimal("1")
    )
    fixed_notional: Decimal = Field(default=Decimal("0.0000"), ge=Decimal("0"))
    max_position_usdt: Decimal = Field(default=Decimal("0.0000"), ge=Decimal("0"))

    nickname: str = Field(default="", max_length=64)

    @field_validator("symbol")
    @classmethod
    def symbol_upper(cls, v: str) -> str:
        return v.upper()


class BotCreateRequest(BotBase):
    cred_id: UUID


class BotCreate(BotBase):
    user_id: UUID
    cred_id: UUID


class BotUpdate(BaseModel):
    # all optional for PATCH-style updates
    enabled: Optional[bool] = None
    side_whitelist: Optional[Side] = None
    leverage: Optional[int] = Field(default=None, ge=1, le=125)
    use_balance_pct: Optional[bool] = None
    balance_pct: Optional[Decimal] = Field(
        default=None, ge=Decimal("0"), le=Decimal("1")
    )
    fixed_notional: Optional[Decimal] = Field(default=None, ge=Decimal("0"))
    max_position_usdt: Optional[Decimal] = Field(default=None, ge=Decimal("0"))
    nickname: Optional[str] = Field(default=None, max_length=64)


class BotOut(BotBase):
    id: UUID
    user_id: UUID
    cred_id: UUID
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
