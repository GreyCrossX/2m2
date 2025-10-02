from __future__ import annotations
from datetime import datetime
from uuid import UUID
from decimal import Decimal
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator

Env = Literal["testnet", "prod"]
SideMode = Literal["both", "long_only", "short_only"]
BotStatus = Literal["active", "paused", "ended"]


class BotBase(BaseModel):
    symbol: str = Field(min_length=1, max_length=32)
    timeframe: Literal["2m"] = "2m"
    env: Env = "testnet"
    side_mode: SideMode = "both"
    leverage: int = Field(default=5, ge=1, le=125)
    risk_per_trade: Decimal = Field(
        default=Decimal("0.005"),
        gt=Decimal("0"),
        le=Decimal("1"),
    )
    tp_ratio: Decimal = Field(default=Decimal("1.5"), gt=Decimal("0"))
    max_qty: Optional[Decimal] = Field(default=None, gt=Decimal("0"))
    status: BotStatus = "active"
    nickname: str = Field(default="", max_length=64)

    @field_validator("symbol")
    @classmethod
    def symbol_upper(cls, v: str) -> str:
        return v.upper()

    @field_validator("risk_per_trade", "tp_ratio", mode="before")
    @classmethod
    def ensure_decimal(cls, value):  # type: ignore[override]
        if value is None:
            return value
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))

    @field_validator("max_qty", mode="before")
    @classmethod
    def ensure_decimal_optional(cls, value):  # type: ignore[override]
        if value in (None, "", 0):
            return None if value in (None, "") else Decimal(str(value))
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))


class BotCreate(BotBase):
    user_id: UUID
    cred_id: UUID


class BotUpdate(BaseModel):
    side_mode: Optional[SideMode] = None
    leverage: Optional[int] = Field(default=None, ge=1, le=125)
    risk_per_trade: Optional[Decimal] = Field(default=None, gt=Decimal("0"), le=Decimal("1"))
    tp_ratio: Optional[Decimal] = Field(default=None, gt=Decimal("0"))
    max_qty: Optional[Decimal] = Field(default=None, gt=Decimal("0"))
    status: Optional[BotStatus] = None
    nickname: Optional[str] = Field(default=None, max_length=64)

    @field_validator("risk_per_trade", "tp_ratio", "max_qty", mode="before")
    @classmethod
    def ensure_optional_decimal(cls, value):  # type: ignore[override]
        if value is None:
            return value
        if isinstance(value, Decimal):
            return value
        if value == "":
            return None
        return Decimal(str(value))


class BotOut(BotBase):
    id: UUID
    user_id: UUID
    cred_id: UUID
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
