# app/services/worker/application/balance_validator.py
from __future__ import annotations
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol, Tuple
from uuid import UUID

from ..domain.models import BotConfig


# --------- Ports (to be implemented by Infra) ---------
class BinanceAccount(Protocol):
    """
    Backward-compatible account port.

    Implement EITHER:
      - fetch_um_available_balance(cred_id, env) -> Decimal   (preferred)
    OR BOTH of:
      - fetch_usdt_balance(cred_id, env) -> Decimal           (old)
      - fetch_used_margin(cred_id, env) -> Decimal            (old)
    """
    # New / preferred (from /fapi/v2/account.availableBalance)
    async def fetch_um_available_balance(self, cred_id: UUID, env: str) -> Decimal: ...  # type: ignore[empty-body]

    # Legacy pair (we'll derive available = free - used)
    async def fetch_usdt_balance(self, cred_id: UUID, env: str) -> Decimal: ...         # type: ignore[empty-body]
    async def fetch_used_margin(self, cred_id: UUID, env: str) -> Decimal: ...          # type: ignore[empty-body]


class BalanceCache(Protocol):
    """Cache available balance by credential+env with short TTL."""
    async def get(self, cred_id: UUID, env: str) -> Decimal | None: ...
    async def set(self, cred_id: UUID, env: str, value: Decimal) -> None: ...


# --------- Use Case ---------
@dataclass
class BalanceValidator:
    """
    Uses Binance *availableBalance* if the adapter provides it, otherwise
    falls back to (free - used_margin). Caches per (cred_id, env).
    """
    binance_account: BinanceAccount
    balance_cache: BalanceCache

    async def validate_balance(
        self,
        bot: BotConfig,
        required_margin: Decimal,
        *,
        available_balance: Decimal | None = None,
    ) -> Tuple[bool, Decimal]:
        """Validate that available balance covers ``required_margin``.

        ``available_balance`` allows callers to reuse a recently fetched value
        to avoid duplicate cache/network lookups (the validator will still
        consult its cache if ``None`` is provided).
        """
        available = (
            available_balance
            if available_balance is not None
            else await self.get_available_balance(bot.cred_id, bot.env)
        )
        ok = available >= required_margin
        return ok, available

    async def get_available_balance(
        self,
        cred_id: UUID,
        env: str,
    ) -> Decimal:
        cached = await self.balance_cache.get(cred_id, env)
        if cached is not None:
            return cached

        # Preferred: direct availableBalance from UM futures account endpoint.
        if hasattr(self.binance_account, "fetch_um_available_balance"):
            try:
                available = await self.binance_account.fetch_um_available_balance(cred_id, env)  # type: ignore[attr-defined]
            except Exception:
                # Defensive fallback to legacy pair if the new call fails at runtime
                available = await self._fallback_available(cred_id, env)
        else:
            # Legacy adapter: compute available as free - used_margin
            available = await self._fallback_available(cred_id, env)

        if available < 0:
            available = Decimal("0")

        await self.balance_cache.set(cred_id, env, available)
        return available

    async def _fallback_available(self, cred_id: UUID, env: str) -> Decimal:
        # Legacy behavior: free - used (clamped to >= 0)
        free = Decimal("0")
        used = Decimal("0")
        try:
            free = await self.binance_account.fetch_usdt_balance(cred_id, env)  # type: ignore[attr-defined]
        except Exception:
            pass
        try:
            used = await self.binance_account.fetch_used_margin(cred_id, env)   # type: ignore[attr-defined]
        except Exception:
            pass

        if free < 0:
            free = Decimal("0")
        if used < 0:
            used = Decimal("0")

        avail = free - used
        return avail if avail > 0 else Decimal("0")
