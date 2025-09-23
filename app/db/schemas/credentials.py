from __future__ import annotations
from datetime import datetime
from uuid import UUID
from decimal import Decimal
from typing import Literal, Optional
from pydantic import BaseModel, Field

Env = Literal["testnet", "prod"]

class ApiCredentialBase(BaseModel):
    env: Env = Field(default="testnet")
    label: str = Field(default="default", max_length=64)

class ApiCredentialCreate(ApiCredentialBase):
    # Plaintext inputs only for creation; you will encrypt server-side.
    api_key: str
    api_secret: str

class ApiCredentialOut(ApiCredentialBase):
    id: UUID
    user_id: UUID
    created_at: datetime

    class Config:
        from_attributes = True  # pydantic v2 ORM mode
