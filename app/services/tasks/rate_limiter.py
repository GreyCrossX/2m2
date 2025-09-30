"""
Token-bucket rate limiter backed by Redis (atomic via Lua).

Usage
-----
- Global/default config via env:
    RL_ENABLED=1
    RL_DEFAULT_CAPACITY=10        # max burst tokens
    RL_DEFAULT_REFILL_RATE=5      # tokens per second (float ok)
- Optional per-key overrides:
    set_bucket_config(api_key_id, capacity=20, refill_rate=10)

- Check allowance:
    if acquire(api_key_id, cost=1):
        ... do the exchange call ...
    else:
        ... retry/backoff/queue ...

Notes
-----
- All math and state updates are atomic in Lua on the Redis server.
- State per key is stored at: rl:{api_key_id}
- Fields: tokens (float), ts (ms), cap (float), rate (float)
- Idle buckets auto-expire after 1 hour (configurable below).
"""

from __future__ import annotations

import os
import logging
from typing import Tuple

from app.services.ingestor.redis_io import r  # redis-py client

LOG = logging.getLogger("rate_limiter")


def _env_bool(name: str, default: bool = True) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip().lower()
    return v not in ("0", "false", "no", "off", "")


RL_ENABLED = _env_bool("RL_ENABLED", True)
DEFAULT_CAPACITY = float(os.getenv("RL_DEFAULT_CAPACITY", "10"))
DEFAULT_REFILL_RATE = float(os.getenv("RL_DEFAULT_REFILL_RATE", "5"))  # tokens/sec
IDLE_EXPIRE_MS = int(os.getenv("RL_IDLE_EXPIRE_MS", "3600000"))        # 1h


# ──────────────────────────────────────────────────────────────────────────────
# Redis key helpers
# ──────────────────────────────────────────────────────────────────────────────

def _key(api_key_id: str) -> str:
    return f"rl:{api_key_id}"


# ──────────────────────────────────────────────────────────────────────────────
# Atomic Lua script (server time based)
# ──────────────────────────────────────────────────────────────────────────────
# ARGV:
#   1: capacity (float)
#   2: refill_rate tokens/sec (float)
#   3: cost (float)
#   4: idle_expire_ms (int)
#
# Returns: {allowed (1|0), remaining_tokens (float)}
_LUA = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local idle_expire_ms = tonumber(ARGV[4])

-- use Redis server time for consistency
local t = redis.call('TIME')
local now_ms = (t[1] * 1000) + math.floor(t[2] / 1000)

local data = redis.call('HMGET', key, 'tokens', 'ts', 'cap', 'rate')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])

-- bootstrapping
if (not tokens) or (not ts) then
  tokens = capacity
  ts = now_ms
end

-- per-key overrides if present
local cap_override = tonumber(data[3])
local rate_override = tonumber(data[4])
if cap_override then capacity = cap_override end
if rate_override then refill_rate = rate_override end

-- refill based on elapsed time
local delta_ms = now_ms - ts
if delta_ms < 0 then delta_ms = 0 end
local refill = delta_ms * (refill_rate / 1000.0)
tokens = math.min(capacity, tokens + refill)

-- spend tokens if we can
local allowed = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
end

-- persist state and set idle expiry
redis.call('HMSET', key, 'tokens', tokens, 'ts', now_ms, 'cap', capacity, 'rate', refill_rate)
if idle_expire_ms and idle_expire_ms > 0 then
  redis.call('PEXPIRE', key, idle_expire_ms)
end

return {allowed, tokens}
"""


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────

def set_bucket_config(api_key_id: str, *, capacity: float, refill_rate: float) -> None:
    """
    Override capacity/refill for a specific API key.
    """
    try:
        key = _key(api_key_id)
        r.hset(key, mapping={"cap": capacity, "rate": refill_rate})
        r.hsetnx(key, "tokens", capacity)
        r.hsetnx(key, "ts", 0)
    except Exception as e:
        LOG.error("set_bucket_config failed for %s: %s", api_key_id, e)


def get_bucket_config(api_key_id: str) -> Tuple[float, float]:
    """
    Return (capacity, refill_rate) for this key, falling back to defaults.
    """
    try:
        vals = r.hmget(_key(api_key_id), "cap", "rate")
        cap = float(vals[0]) if vals and vals[0] is not None else DEFAULT_CAPACITY
        rate = float(vals[1]) if vals and vals[1] is not None else DEFAULT_REFILL_RATE
        return cap, rate
    except Exception:
        return DEFAULT_CAPACITY, DEFAULT_REFILL_RATE


def acquire_with_remaining(
    api_key_id: str,
    *,
    cost: float = 1.0,
    capacity: float | None = None,
    refill_rate: float | None = None,
) -> Tuple[bool, float]:
    """
    Atomically attempt to spend `cost` tokens.
    Returns (allowed, remaining_tokens).

    - capacity/refill_rate can be provided to avoid an extra HMGET round-trip
      when you already know the desired defaults; per-key overrides still win.
    """
    if not RL_ENABLED:
        return True, float("inf")

    cap = capacity if capacity is not None else DEFAULT_CAPACITY
    rate = refill_rate if refill_rate is not None else DEFAULT_REFILL_RATE

    try:
        resp = r.eval(_LUA, 1, _key(api_key_id), cap, rate, cost, IDLE_EXPIRE_MS)
        allowed = bool(resp[0])
        remaining = float(resp[1])
        return allowed, remaining
    except Exception as e:
        # Fail-closed on mainnet to avoid accidental rate overruns
        LOG.error("rate_limiter acquire failed for %s: %s", api_key_id, e)
        return False, 0.0


def acquire(
    api_key_id: str,
    *,
    cost: float = 1.0,
    capacity: float | None = None,
    refill_rate: float | None = None,
) -> bool:
    """
    Convenience wrapper that returns only the boolean decision.
    """
    allowed, _ = acquire_with_remaining(
        api_key_id, cost=cost, capacity=capacity, refill_rate=refill_rate
    )
    return allowed
