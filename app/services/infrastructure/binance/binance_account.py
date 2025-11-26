"""Account helpers built on :class:`BinanceClient`."""

from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation
from typing import Awaitable, Callable, Any, Optional
from uuid import UUID

from .binance_client import BinanceClient

logger = logging.getLogger(__name__)


# === Utility Functions ===


def _to_decimal_safe(value: Any, *, default: str = "0") -> Decimal:
    """
    Safely convert a value to Decimal.

    Treats None, '', 'null', 'NaN', 'inf', '-inf' (case-insensitive) as default.
    Logs debug on parse failure.
    """
    if value is None:
        return Decimal(default)

    s = str(value).strip().lower()
    if s in {"", "none", "null", "nan", "inf", "-inf"}:
        return Decimal(default)

    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as e:
        logger.debug(
            "Failed to parse Decimal from %r; using default=%s",
            value,
            default,
            exc_info=e,
        )
        return Decimal(default)


def _find_asset_balance(balances: list[dict], asset: str) -> Optional[dict]:
    """Find balance dict for given asset (case-sensitive match)."""
    return next((b for b in balances if b.get("asset") == asset), None)


# === Core Class ===


class BinanceAccount:
    """
    High-level helper for Binance account operations (balance, margin, available funds).
    """

    def __init__(
        self, client_provider: Callable[[UUID, str], Awaitable[BinanceClient]]
    ):
        self._client_provider = client_provider

    async def _get_client(self, cred_id: UUID, env: str) -> BinanceClient:
        """Resolve BinanceClient instance asynchronously."""
        return await self._client_provider(cred_id, env)

    # --- Public Methods ---

    async def fetch_usdt_balance(self, cred_id: UUID, env: str) -> Decimal:
        """
        Fetch available USDT balance (spot or futures wallet).

        Returns:
            Decimal: Available USDT balance, never negative.
        """
        client = await self._get_client(cred_id, env)
        balances = await client.balance()

        usdt_entry = _find_asset_balance(balances, "USDT")
        if not usdt_entry:
            return Decimal("0")

        raw = (
            usdt_entry.get("availableBalance")
            or usdt_entry.get("withdrawAvailable")
            or usdt_entry.get("balance")
            or "0"
        )
        return _to_decimal_safe(raw)

    async def fetch_used_margin(self, cred_id: UUID, env: str) -> Decimal:
        """
        Calculate total initial margin in use.

        Prefers ``totalInitialMargin`` from ``/fapi/v2/account``.
        Falls back to manual aggregation from position information.

        Returns:
            Decimal: Total used initial margin.
        """
        client = await self._get_client(cred_id, env)

        # Primary: Use account summary
        account_info = await client.account()
        init_margin = account_info.get("totalInitialMargin") or account_info.get(
            "total_initial_margin"
        )
        if init_margin is not None:
            return _to_decimal_safe(init_margin)

        # Fallback: Aggregate from position risks
        return await self._calculate_used_margin_from_positions(client)

    async def _calculate_used_margin_from_positions(
        self, client: BinanceClient
    ) -> Decimal:
        """Manually compute used margin from position information."""
        risks = await client.position_information()
        total_used = Decimal("0")

        for risk in risks:
            try:
                qty = _to_decimal_safe(risk.get("positionAmt", "0"))
                if qty == 0:
                    continue

                entry_price = _to_decimal_safe(risk.get("entryPrice", "0"))
                leverage = _to_decimal_safe(risk.get("leverage", "1")) or Decimal("1")
                notional = abs(qty) * entry_price
                margin_used = notional / (leverage if leverage > 0 else Decimal("1"))
                total_used += margin_used
            except Exception as e:
                logger.debug(
                    "Malformed position risk row skipped: %r", risk, exc_info=e
                )

        return total_used

    async def fetch_um_available_balance(self, cred_id: UUID, env: str) -> Decimal:
        """
        Get available balance for USDT-Margined Futures.

        Priority:
          1. ``availableBalance`` from ``/fapi/v2/account``
          2. USDT ``availableBalance`` from ``/fapi/v2/balance``
          3. ``fetch_usdt_balance() - fetch_used_margin()``

        Returns:
            Decimal: Available margin, clamped to >= 0.
        """
        client = await self._get_client(cred_id, env)

        # 1. Try account-level available balance
        try:
            account_info = await client.account()
            raw_avail = (
                account_info.get("availableBalance")
                or account_info.get("available_balance")
                or account_info.get("totalAvailableBalance")
            )
            if raw_avail is not None:
                avail = _to_decimal_safe(raw_avail)
                return avail if avail > 0 else Decimal("0")
        except Exception as e:
            logger.debug("Failed to get availableBalance from account()", exc_info=e)

        # 2. Try per-asset balance
        try:
            balances = await client.balance()
            usdt = _find_asset_balance(balances, "USDT")
            if usdt:
                raw = (
                    usdt.get("availableBalance")
                    or usdt.get("withdrawAvailable")
                    or usdt.get("balance")
                )
                if raw is not None:
                    avail = _to_decimal_safe(raw)
                    return avail if avail > 0 else Decimal("0")
        except Exception as e:
            logger.debug(
                "Failed to get USDT availableBalance from balance()", exc_info=e
            )

        # 3. Fallback: free - used
        try:
            free = await self.fetch_usdt_balance(cred_id, env)
            used = await self.fetch_used_margin(cred_id, env)
            available = free - used
            return available if available > 0 else Decimal("0")
        except Exception as e:
            logger.warning(
                "All methods failed to compute available balance", exc_info=e
            )
            return Decimal("0")
