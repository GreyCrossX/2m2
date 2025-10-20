from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol, Tuple
from uuid import UUID

from ..domain.models import BotConfig


# --------- Ports (to be implemented by Infra) ---------

class BinanceAccount(Protocol):
    async def fetch_usdt_balance(self, user_id: UUID, env: str) -> Decimal: ...
    async def fetch_used_margin(self, user_id: UUID, env: str) -> Decimal: ...


class BalanceCache(Protocol):
    """Simple in-memory cache with TTL controlled by infra."""
    async def get(self, user_id: UUID, env: str) -> Decimal | None: ...
    async def set(self, user_id: UUID, env: str, value: Decimal) -> None: ...


# --------- Use Case ---------

@dataclass
class BalanceValidator:
    """
    Computes available balance and validates margin requirements.
    Assumes USDT-margined futures.
    """
    binance_account: BinanceAccount
    balance_cache: BalanceCache

    async def validate_balance(
        self,
        bot: BotConfig,
        required_margin: Decimal,
    ) -> Tuple[bool, Decimal]:
        """
        1) Available balance = total_free - used_margin
        2) Check available >= required_margin
        3) Return (ok, available)
        """
        available = await self.get_available_balance(bot.user_id, bot.env)
        ok = available >= required_margin
        return ok, available

    async def get_available_balance(
        self,
        user_id: UUID,
        env: str,
    ) -> Decimal:
        cached = await self.balance_cache.get(user_id, env)
        if cached is not None:
            return cached

        total_free = await self.binance_account.fetch_usdt_balance(user_id, env)
        used_margin = await self.binance_account.fetch_used_margin(user_id, env)

        # Defensive
        if total_free < 0:
            total_free = Decimal("0")
        if used_margin < 0:
            used_margin = Decimal("0")

        available = total_free - used_margin
        if available < 0:
            available = Decimal("0")

        await self.balance_cache.set(user_id, env, available)
        return available
