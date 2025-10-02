"""Helpers for managing bot runtime configuration in Redis."""
from __future__ import annotations

from decimal import Decimal
from typing import List, Optional, Set, Tuple, TYPE_CHECKING
import logging

from redis.exceptions import RedisError

from app.config import settings
from app.services.ingestor import redis_io
from app.services.tasks.state import write_bot_config, index_bot, deindex_bot

if TYPE_CHECKING:  # pragma: no cover - only for typing
    from app.db.models.bots import Bot


LOG = logging.getLogger(__name__)

_STREAM_MATCH = "stream:signal|{*}"


def _to_decimal(value: Optional[object]) -> Optional[Decimal]:
    if value is None or value == "":
        return None
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def list_signal_stream_pairs() -> List[Tuple[str, str]]:
    """Return available (symbol, timeframe) pairs discovered in Redis."""
    pairs: Set[Tuple[str, str]] = set()

    try:
        cursor: int = 0
        while True:
            cursor, keys = redis_io.r.scan(cursor=cursor, match=_STREAM_MATCH, count=200)
            for key in keys:
                if not key:
                    continue
                key_str = key.decode("utf-8") if isinstance(key, (bytes, bytearray)) else str(key)
                tag_start = key_str.find("{")
                tag_end = key_str.find("}", tag_start + 1)
                if tag_start == -1 or tag_end == -1:
                    continue
                tag = key_str[tag_start + 1 : tag_end]
                if ":" in tag:
                    sym, tf = tag.split(":", 1)
                else:
                    sym, tf = tag, ""
                pairs.add((sym.upper(), tf))
            if cursor == 0:
                break
    except RedisError as exc:  # pragma: no cover - defensive logging path
        LOG.warning("Failed to scan Redis for signal streams: %s", exc)

    if not pairs:
        for sym, tf in settings.pairs_1m_list():
            pairs.add((sym.upper(), tf))

    return sorted(pairs)


def available_symbols() -> Set[str]:
    return {sym for sym, _ in list_signal_stream_pairs()}


def sync_bot_runtime(bot: "Bot") -> None:
    """Persist the bot's runtime configuration in Redis and update indexes."""
    cfg = {
        "bot_id": str(bot.id),
        "user_id": str(bot.user_id),
        "sym": (bot.symbol or "").upper(),
        "side_mode": (bot.side_mode or "both"),
        "status": (bot.status or "active"),
        "risk_per_trade": _to_decimal(bot.risk_per_trade) or Decimal("0.005"),
        "leverage": _to_decimal(bot.leverage) or Decimal("1"),
        "tp_ratio": _to_decimal(bot.tp_ratio) or Decimal("1.5"),
    }

    max_qty = _to_decimal(getattr(bot, "max_qty", None))
    if max_qty:
        cfg["max_qty"] = max_qty

    write_bot_config(str(bot.id), cfg)

    status = cfg.get("status", "active")
    symbol = cfg.get("sym", "")

    if status == "ended":
        deindex_bot(symbol, str(bot.id))
    else:
        index_bot(symbol, str(bot.id))
