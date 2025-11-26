"""Lightweight Redis-backed bot state helpers (config + index)."""

from __future__ import annotations

import json
from typing import Any, Dict

from app.services.ingestor.redis_io import r


def _cfg_key(bot_id: str) -> str:
    return f"bot:{bot_id}"


def _idx_key(sym: str) -> str:
    return f"idx:bots|{{{sym.upper()}}}"


def write_bot_config(bot_id: str, cfg: Dict[str, Any]) -> None:
    """Persist bot config as a Redis hash."""
    mapping = {k: json.dumps(v) if isinstance(v, (dict, list)) else str(v) for k, v in cfg.items()}
    r.hset(_cfg_key(bot_id), mapping=mapping)


def read_bot_config(bot_id: str) -> Dict[str, str]:
    """Read bot config hash; returns empty dict if missing."""
    return r.hgetall(_cfg_key(bot_id)) or {}


def index_bot(sym: str, bot_id: str) -> None:
    """Add bot to symbol index set."""
    r.sadd(_idx_key(sym), bot_id)

