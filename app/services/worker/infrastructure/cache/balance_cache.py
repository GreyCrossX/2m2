from __future__ import annotations

import time
from decimal import Decimal
from typing import Dict, Optional, Tuple
from uuid import UUID


class BalanceCache:
    """
    In-memory async-friendly cache with TTL for account balances.
    Implements the async get/set the application port expects.
    """

    def __init__(self, ttl_seconds: int = 30):
        self._cache: Dict[Tuple[UUID, str], Tuple[Decimal, float]] = {}
        self._ttl = ttl_seconds

    async def get(self, cred_id: UUID, env: str) -> Optional[Decimal]:
        key = (cred_id, env)
        item = self._cache.get(key)
        if not item:
            return None
        value, ts = item
        if (time.time() - ts) > self._ttl:
            self._cache.pop(key, None)
            return None
        return value

    async def set(self, cred_id: UUID, env: str, balance: Decimal) -> None:
        key = (cred_id, env)
        self._cache[key] = (balance, time.time())

    async def invalidate(self, cred_id: UUID, env: str) -> None:
        self._cache.pop((cred_id, env), None)
