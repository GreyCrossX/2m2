from __future__ import annotations
from typing import Mapping
from redis.asyncio import Redis

from ..utils.keys import st_ind, st_signal, snap_ind

class StreamPublisher:
    def __init__(self, r: Redis, sym: str, tf: str, maxlen_ind: int, maxlen_signal: int) -> None:
        self.r = r
        self.sym = sym
        self.tf = tf
        self.maxlen_ind = maxlen_ind
        self.maxlen_signal = maxlen_signal


    async def publish_indicator(self, payload: Mapping[str, str]) -> str:
        key = st_ind(self.sym, self.tf)
        snap = snap_ind(self.sym, self.tf)
        # XADD with MAXLEN ~ to keep series bounded
        msg_id = await self.r.xadd(key, {k: str(v) for k, v in payload.items()}, maxlen=self.maxlen_ind, approximate=True)
        # Keep latest snapshot
        await self.r.hset(snap, mapping={k: str(v) for k, v in payload.items()})
        return msg_id.decode() if isinstance(msg_id, (bytes, bytearray)) else str(msg_id)


    async def publish_signal(self, payload: Mapping[str, str]) -> str:
        key = st_signal(self.sym, self.tf)
        msg_id = await self.r.xadd(key, {k: str(v) for k, v in payload.items()}, maxlen=self.maxlen_signal, approximate=True)
        return msg_id.decode() if isinstance(msg_id, (bytes, bytearray)) else str(msg_id)