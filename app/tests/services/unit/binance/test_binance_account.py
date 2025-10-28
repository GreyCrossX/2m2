from __future__ import annotations

from decimal import Decimal
from typing import Awaitable, Callable
from uuid import UUID, uuid4

import pytest

pytestmark = pytest.mark.unit

from app.services.infrastructure.binance.binance_account import BinanceAccount


class _StubClient:
    def __init__(self, *, balances: list[dict], account_info: dict, positions: list[dict]):
        self._balances = balances
        self._account_info = account_info
        self._positions = positions

    async def balance(self) -> list[dict]:
        return self._balances

    async def account(self) -> dict:
        return self._account_info

    async def position_information(self, symbol: str | None = None) -> list[dict]:
        return self._positions


def _provider_factory(client: _StubClient) -> Callable[[UUID, str], Awaitable[_StubClient]]:
    async def _provider(_: UUID, __: str) -> _StubClient:
        return client

    return _provider


@pytest.fixture()
def cred_id() -> UUID:
    return uuid4()


@pytest.mark.asyncio()
async def test_fetch_usdt_balance_prefers_available(cred_id: UUID) -> None:
    client = _StubClient(
        balances=[{"asset": "USDT", "availableBalance": "12.5", "balance": "99"}],
        account_info={},
        positions=[],
    )
    account = BinanceAccount(_provider_factory(client))
    balance = await account.fetch_usdt_balance(cred_id, "testnet")
    assert balance == Decimal("12.5")


@pytest.mark.asyncio()
async def test_fetch_usdt_balance_handles_missing_asset(cred_id: UUID) -> None:
    client = _StubClient(balances=[], account_info={}, positions=[])
    account = BinanceAccount(_provider_factory(client))
    balance = await account.fetch_usdt_balance(cred_id, "mainnet")
    assert balance == Decimal("0")


@pytest.mark.asyncio()
async def test_fetch_used_margin_prefers_account_field(cred_id: UUID) -> None:
    client = _StubClient(
        balances=[],
        account_info={"totalInitialMargin": "25.75"},
        positions=[],
    )
    account = BinanceAccount(_provider_factory(client))
    used = await account.fetch_used_margin(cred_id, "testnet")
    assert used == Decimal("25.75")


@pytest.mark.asyncio()
async def test_fetch_used_margin_fallbacks_to_positions(cred_id: UUID) -> None:
    client = _StubClient(
        balances=[],
        account_info={},
        positions=[
            {"positionAmt": "1", "entryPrice": "100", "leverage": "10"},
            {"positionAmt": "-2", "entryPrice": "50", "leverage": "5"},
        ],
    )
    account = BinanceAccount(_provider_factory(client))
    used = await account.fetch_used_margin(cred_id, "mainnet")
    assert used == Decimal("30")


@pytest.mark.asyncio()
async def test_fetch_used_margin_supports_alt_field(cred_id: UUID) -> None:
    client = _StubClient(
        balances=[],
        account_info={"total_initial_margin": "12"},
        positions=[],
    )
    account = BinanceAccount(_provider_factory(client))
    used = await account.fetch_used_margin(cred_id, "mainnet")
    assert used == Decimal("12")


@pytest.mark.asyncio()
async def test_fetch_used_margin_skips_bad_positions(cred_id: UUID) -> None:
    client = _StubClient(
        balances=[],
        account_info={},
        positions=[{"positionAmt": "nan", "entryPrice": "abc", "leverage": "0"}],
    )
    account = BinanceAccount(_provider_factory(client))
    used = await account.fetch_used_margin(cred_id, "testnet")
    assert used == Decimal("0")
