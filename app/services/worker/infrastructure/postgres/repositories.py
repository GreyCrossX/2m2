from __future__ import annotations

from decimal import Decimal
from typing import List, Optional, Tuple
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

# ORM models
from app.db.models.bots import Bot as ORMBot
from app.db.models.credentials import ApiCredential as ORMCred

# Domain
from ...domain.enums import SideWhitelist
from ...domain.models import BotConfig


# ---------- mapping helpers ----------

_WHITELIST_MAP = {
    "long": SideWhitelist.LONG,
    "short": SideWhitelist.SHORT,
    "both": SideWhitelist.BOTH,
}


def _map_side_whitelist(value: object) -> SideWhitelist:
    """
    Accepts DB value as Enum/str and returns SideWhitelist.
    Defaults to BOTH for unknown/None to be defensive.
    """
    if isinstance(value, SideWhitelist):
        return value
    try:
        key = str(value).lower()
        return _WHITELIST_MAP.get(key, SideWhitelist.BOTH)
    except Exception:
        return SideWhitelist.BOTH


def _to_bot_config(row: ORMBot) -> BotConfig:
    """
    Convert ORM Bot row -> Domain BotConfig.
    Ensures:
      - symbol uppercased
      - proper SideWhitelist enum
      - Decimals preserved (0 fallback when None)
    """
    return BotConfig(
        id=row.id,
        user_id=row.user_id,
        cred_id=row.cred_id,
        symbol=(row.symbol or "").upper(),
        timeframe=row.timeframe,
        enabled=bool(row.enabled),
        env=str(row.env),  # "testnet" | "prod"
        side_whitelist=_map_side_whitelist(row.side_whitelist),
        leverage=int(row.leverage),
        use_balance_pct=bool(row.use_balance_pct),
        balance_pct=Decimal(row.balance_pct or 0),
        fixed_notional=Decimal(row.fixed_notional or 0),
        max_position_usdt=Decimal(row.max_position_usdt or 0),
    )


# ---------- Repositories ----------


class BotRepository:
    """Adapter that exposes BotConfig records using a session factory."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self._session_factory = session_factory

    async def get_bot(self, bot_id: UUID) -> Optional[BotConfig]:
        async with self._session_factory() as session:
            stmt = select(ORMBot).where(ORMBot.id == bot_id)
            res = await session.execute(stmt)
            row: Optional[ORMBot] = res.scalars().first()
            return _to_bot_config(row) if row else None

    async def get_enabled_bots(self) -> List[BotConfig]:
        async with self._session_factory() as session:
            stmt = select(ORMBot).where(ORMBot.enabled == True)  # noqa: E712
            res = await session.execute(stmt)
            return [_to_bot_config(row) for row in res.scalars().all()]

    async def get_bot_credentials(self, bot_id: UUID) -> Tuple[BotConfig, str, str]:
        async with self._session_factory() as session:
            stmt = (
                select(ORMBot, ORMCred)
                .join(ORMCred, ORMCred.id == ORMBot.cred_id)
                .where(ORMBot.id == bot_id)
            )
            res = await session.execute(stmt)
            result = res.first()
            if not result:
                raise LookupError(f"Bot {bot_id} not found")
            bot_row, cred_row = result
            api_key, api_secret = cred_row.get_decrypted()
            return _to_bot_config(bot_row), api_key, api_secret


class CredentialRepository:
    """Fetch/decrypt ApiCredential secrets on demand."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self._session_factory = session_factory

    async def get_plaintext_credentials(self, cred_id: UUID) -> Tuple[str, str]:
        async with self._session_factory() as session:
            stmt = select(ORMCred).where(ORMCred.id == cred_id)
            res = await session.execute(stmt)
            cred: Optional[ORMCred] = res.scalars().first()
            if not cred:
                raise LookupError(f"Credential {cred_id} not found")
            api_key, api_secret = cred.get_decrypted()
            return api_key, api_secret
