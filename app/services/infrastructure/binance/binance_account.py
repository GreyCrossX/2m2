"""Account helpers built on :class:`BinanceClient`."""
from __future__ import annotations

from decimal import Decimal
from typing import Awaitable, Callable
from uuid import UUID

from .binance_client import BinanceClient


class BinanceAccount:
    """High-level account helper for balance/margin lookups."""

    def __init__(self, client_provider: Callable[[UUID, str], Awaitable[BinanceClient]]):
        self._client_provider = client_provider

    async def _get_client(self, cred_id: UUID, env: str) -> BinanceClient:
        return await self._client_provider(cred_id, env)

    async def fetch_usdt_balance(self, cred_id: UUID, env: str) -> Decimal:
        """Return available USDT balance for the given credentials/env."""

        client = await self._get_client(cred_id, env)
        balances = await client.balance()
        usdt = next((b for b in balances if str(b.get("asset")) == "USDT"), None)
        if not usdt:
            return Decimal("0")
        value = usdt.get("availableBalance") or usdt.get("balance") or "0"
        return Decimal(str(value))

    async def fetch_used_margin(self, cred_id: UUID, env: str) -> Decimal:
        """Return initial margin in use; falls back to manual computation if needed."""

        client = await self._get_client(cred_id, env)
        account_info = await client.account()
        init_margin = account_info.get("totalInitialMargin")
        if init_margin is None:
            init_margin = account_info.get("total_initial_margin")
        if init_margin is not None:
            return Decimal(str(init_margin))

        risks = await client.position_information()
        used = Decimal("0")
        for risk in risks:
            try:
                qty = Decimal(str(risk.get("positionAmt", "0")))
                entry = Decimal(str(risk.get("entryPrice", "0")))
                lev = Decimal(str(risk.get("leverage", "1"))) or Decimal("1")
                notional = abs(qty) * entry
                used += notional / (lev if lev > 0 else Decimal("1"))
            except Exception:  # pragma: no cover - defensive against malformed payloads
                continue
        return used
