from __future__ import annotations

import asyncio
from typing import Dict, List, Optional
from uuid import UUID

from ..domain.enums import OrderStatus
from ..domain.models import OrderState


class StateManager:
    """
    Ephemeral in-memory state of latest OrderState per bot.
    Useful for quick lookups / duplicate suppression.
    Not a source of truth; persistence should happen elsewhere.
    """

    def __init__(self):
        self._states: Dict[UUID, OrderState] = {}
        self._lock = asyncio.Lock()

    async def set_state(self, bot_id: UUID, state: OrderState) -> None:
        async with self._lock:
            self._states[bot_id] = state

    async def get_state(self, bot_id: UUID) -> Optional[OrderState]:
        async with self._lock:
            return self._states.get(bot_id)

    async def clear_state(self, bot_id: UUID) -> None:
        async with self._lock:
            self._states.pop(bot_id, None)

    async def get_all_pending_orders(self) -> List[OrderState]:
        async with self._lock:
            return [s for s in self._states.values() if s.status == OrderStatus.PENDING]
