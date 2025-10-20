from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from typing import List, Optional, Tuple
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

# ORM models (your current schema)
from app.db.models.bots import Bot as ORMBot          # adjust import if your path differs
from app.db.models.credentials import ApiCredential as ORMCred  # adjust import if your path differs

# Domain
from ...domain.models import BotConfig
from ...domain.enums import Side


# ---------- mapping helpers ----------

_SIDE_MAP = {
    "long": Side.LONG,
    "short": Side.SHORT,
    "both": Side.BOTH,
}

def _map_side_whitelist(value: str | Side) -> Side:
    if isinstance(value, Side):
        return value
    try:
        return _SIDE_MAP[str(value).lower()]
    except Exception:
        # Fallback defensively
        return Side.BOTH

def _to_bot_config(row: ORMBot) -> BotConfig:
    """
    Convert ORM Bot row -> Domain BotConfig.
    Ensures:
      - symbol uppercased
      - proper Side enum
      - Decimals preserved
    """
    return BotConfig(
        id=row.id,
        user_id=row.user_id,
        cred_id=row.cred_id,
        symbol=row.symbol.upper(),
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
    """
    Adapter over SQLAlchemy AsyncSession that returns Domain BotConfig objects.
    Also offers a raw accessor with decrypted credentials when needed.
    """
    def __init__(self, session: AsyncSession):
        self._session = session

    # ---- Application-layer port: Optional[BotConfig] by id ----
    async def get_bot(self, bot_id: UUID) -> Optional[BotConfig]:
        stmt = select(ORMBot).where(ORMBot.id == bot_id)
        res = await self._session.execute(stmt)
        row: Optional[ORMBot] = res.scalars().first()
        return _to_bot_config(row) if row else None

    # ---- For router & orchestration: enabled bots ----
    async def get_enabled_bots(self) -> List[BotConfig]:
        stmt = select(ORMBot).where(ORMBot.enabled == True)  # noqa: E712
        res = await self._session.execute(stmt)
        return [_to_bot_config(row) for row in res.scalars().all()]

    # ---- For router filtering by (symbol, timeframe) ----
    async def get_bots_by_symbol(self, symbol: str, timeframe: str) -> List[BotConfig]:
        stmt = (
            select(ORMBot)
            .where(ORMBot.symbol == symbol.upper())
            .where(ORMBot.timeframe == timeframe)
            .where(ORMBot.enabled == True)  # noqa: E712
        )
        res = await self._session.execute(stmt)
        return [_to_bot_config(row) for row in res.scalars().all()]

    # ---- Raw ORM + creds (when you need plaintext for client factories) ----
    async def get_bot_with_credentials_raw(self, bot_id: UUID) -> Tuple[ORMBot, ORMCred, str, str]:
        """
        Return (bot_orm, cred_orm, api_key, api_secret).
        Uses the model's get_decrypted() helper to avoid leaking encryption details here.
        """
        # Bot
        bot_stmt = select(ORMBot).where(ORMBot.id == bot_id)
        bot_res = await self._session.execute(bot_stmt)
        bot: Optional[ORMBot] = bot_res.scalars().first()
        if not bot:
            raise LookupError(f"Bot {bot_id} not found")

        # Credential
        cred_stmt = select(ORMCred).where(ORMCred.id == bot.cred_id)
        cred_res = await self._session.execute(cred_stmt)
        cred: Optional[ORMCred] = cred_res.scalars().first()
        if not cred:
            raise LookupError(f"Credential for bot {bot_id} not found")

        api_key, api_secret = cred.get_decrypted()
        return bot, cred, api_key, api_secret


class CredentialRepository:
    """
    Focused adapter to fetch/decrypt ApiCredential.
    """
    def __init__(self, session: AsyncSession):
        self._session = session

    async def get_credential(self, cred_id: UUID) -> Tuple[ORMCred, str, str]:
        stmt = select(ORMCred).where(ORMCred.id == cred_id)
        res = await self._session.execute(stmt)
        cred: Optional[ORMCred] = res.scalars().first()
        if not cred:
            raise LookupError(f"Credential {cred_id} not found")
        api_key, api_secret = cred.get_decrypted()
        return cred, api_key, api_secret
