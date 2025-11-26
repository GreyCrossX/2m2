import redis
from typing import Dict, Optional, cast

from app.config import settings

# redis-py stubs sometimes mark Redis methods as returning Awaitable; cast to Redis.
r = cast(
    redis.Redis,
    redis.Redis.from_url(
        settings.REDIS_URL or "redis://redis:6379/0", decode_responses=True
    ),
)


def ping_redis() -> None:
    assert r.ping() is True


def xadd(
    stream: str,
    fields: Dict,
    *,
    maxlen: Optional[int] = None,
    approximate: bool = True,
    id: str | int | None = None,
) -> str:
    """
    Wrapper that supports MAXLEN and explicit IDs (for time-based trimming).
    id can be a str like '1693766400000-0'.
    """
    return str(
        r.xadd(
            stream,
            fields,
            id="*" if id is None else str(id),
            maxlen=maxlen,
            approximate=approximate,
        )
    )


def dedupe_once(key: str, ttl_seconds: int = 7 * 24 * 3600) -> bool:
    ok = r.setnx(key, "1")
    if ok:
        r.expire(key, ttl_seconds)

    return bool(ok)
