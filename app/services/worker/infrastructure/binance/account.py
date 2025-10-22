from __future__ import annotations

from decimal import Decimal
from typing import Awaitable, Callable
from uuid import UUID

from .client import BinanceClient


class BinanceAccount:
    """
    Provides BalanceValidator port using the new SDK via a client provider.
    """

    def __init__(self, client_provider: Callable[[UUID, str], Awaitable[BinanceClient]]):
        self._client_provider = client_provider

    async def _get_client(self, cred_id: UUID, env: str) -> BinanceClient:
        return await self._client_provider(cred_id, env)

    async def fetch_usdt_balance(self, cred_id: UUID, env: str) -> Decimal:
        client = await self._get_client(cred_id, env)
        balances = await client.balance()
        usdt = next((b for b in balances if str(b.get("asset")) == "USDT"), None)
        if not usdt:
            return Decimal("0")
        # Prefer availableBalance if present; otherwise balance
        val = usdt.get("availableBalance") or usdt.get("balance") or "0"
        return Decimal(str(val))

    async def fetch_used_margin(self, cred_id: UUID, env: str) -> Decimal:
        client = await self._get_client(cred_id, env)
        acct = await client.account()
        init_margin = acct.get("totalInitialMargin")
        if init_margin is not None:
            return Decimal(str(init_margin))

        risks = await client.position_risk()
        used = Decimal("0")
        for r in risks:
            try:
                qty = Decimal(str(r.get("positionAmt", "0")))
                entry = Decimal(str(r.get("entryPrice", "0")))
                lev = Decimal(str(r.get("leverage", "1"))) or Decimal("1")
                notional = abs(qty) * entry
                used += notional / (lev if lev > 0 else Decimal("1"))
            except Exception:
                continue
        return used
