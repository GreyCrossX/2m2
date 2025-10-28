"""Account helpers built on :class:`BinanceClient`."""
from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation
from typing import Awaitable, Callable, Any
from uuid import UUID

from .binance_client import BinanceClient

logger = logging.getLogger(__name__)


def _to_decimal_safe(value: Any, *, default: str = "0") -> Decimal:
    """
    Convert possibly-malformed numeric values to Decimal safely.
    Treat None / '' / 'null' / 'NaN' / 'nan' / 'inf' / '-inf' as default.
    """
    s = str(value).strip().lower()
    if s in ("", "none", "null", "nan", "inf", "-inf"):
        return Decimal(default)
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        logger.debug("Decimal parse failed; value=%r -> using default=%s", value, default)
        return Decimal(default)


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

        # balances: [{'asset': 'USDT', 'availableBalance': '...', 'balance': '...'}, ...]
        usdt = next((b for b in balances if str(b.get("asset")) == "USDT"), None)
        if not usdt:
            return Decimal("0")

        raw = usdt.get("availableBalance") or usdt.get("balance") or "0"
        return _to_decimal_safe(raw)

    async def fetch_used_margin(self, cred_id: UUID, env: str) -> Decimal:
        """
        Return initial margin in use. Prefer 'account()' fields, fallback to
        manual aggregation from 'position_information()' if needed.
        """
        client = await self._get_client(cred_id, env)

        # Primary path: account() summary fields
        account_info = await client.account()
        init_margin = account_info.get("totalInitialMargin", account_info.get("total_initial_margin"))
        if init_margin is not None:
            return _to_decimal_safe(init_margin)

        # Fallback path: manual calc from position info
        risks = await client.position_information()
        used = Decimal("0")
        for risk in risks:
            try:
                qty = _to_decimal_safe(risk.get("positionAmt", "0"))
                entry = _to_decimal_safe(risk.get("entryPrice", "0"))
                lev = _to_decimal_safe(risk.get("leverage", "1")) or Decimal("1")
                notional = abs(qty) * entry
                used += notional / (lev if lev > 0 else Decimal("1"))
            except Exception:
                # defensive: skip malformed rows
                logger.debug("Skipping malformed position risk row: %r", risk, exc_info=True)
                continue
        return used
