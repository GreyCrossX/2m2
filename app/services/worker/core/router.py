from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Optional, Tuple
from uuid import UUID

from ..domain.enums import Side
from ..domain.models import BotConfig


class SymbolRouter:
    """
    Maintains in-memory subscriptions:
      key (symbol, timeframe) -> [bot_ids]
    Also keeps a local cache of BotConfig for quick filtering.
    """

    def __init__(self):
        self._subscriptions: Dict[Tuple[str, str], List[UUID]] = defaultdict(list)
        self._bot_configs: Dict[UUID, BotConfig] = {}

    async def register_bot(self, bot: BotConfig) -> None:
        """Register a single bot subscription."""
        key = (bot.symbol.upper(), bot.timeframe)
        if bot.id not in self._subscriptions[key]:
            self._subscriptions[key].append(bot.id)
        self._bot_configs[bot.id] = bot

    def get_subscribed_bots(
        self,
        symbol: str,
        timeframe: str,
        side: Side,
    ) -> List[BotConfig]:
        """
        Return bots for (symbol, timeframe) that:
          - are enabled
          - include `side` in side_whitelist (or whitelist == BOTH)
        """
        key = (symbol.upper(), timeframe)
        bot_ids = self._subscriptions.get(key, [])
        out: List[BotConfig] = []
        for bid in bot_ids:
            cfg = self._bot_configs.get(bid)
            if not cfg:
                continue
            if not cfg.enabled:
                continue
            if cfg.side_whitelist == Side.BOTH or cfg.side_whitelist == side:
                out.append(cfg)
        return out

    def reload_subscriptions(self, bots: List[BotConfig]) -> None:
        """
        Rebuild the map from a fresh list of bots.
        Idempotent, cheap, and safe to call periodically.
        """
        subs: Dict[Tuple[str, str], List[UUID]] = defaultdict(list)
        cache: Dict[UUID, BotConfig] = {}
        for b in bots:
            key = (b.symbol.upper(), b.timeframe)
            subs[key].append(b.id)
            cache[b.id] = b
        self._subscriptions = subs
        self._bot_configs = cache

    def get_bot_ids(self, symbol: str, timeframe: str) -> List[UUID]:
        """Return bot IDs subscribed to (symbol, timeframe)."""
        key = (symbol.upper(), timeframe)
        return list(self._subscriptions.get(key, []))

    def get_bot_config(self, bot_id: UUID) -> Optional[BotConfig]:
        return self._bot_configs.get(bot_id)

    def symbols(self) -> List[str]:
        """Return the distinct symbol list currently in the router."""
        return sorted({sym for (sym, _tf) in self._subscriptions.keys()})

    def __len__(self) -> int:
        return sum(len(v) for v in self._subscriptions.values())
