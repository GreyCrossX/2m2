from __future__ import annotations
from typing import Any, Mapping, Awaitable, cast
import logging
from redis.asyncio import Redis

from ..utils.keys import st_ind, st_signal, snap_ind

logger = logging.getLogger(__name__)


class StreamPublisher:
    def __init__(
        self, r: Redis, sym: str, tf: str, maxlen_ind: int, maxlen_signal: int
    ) -> None:
        self.r = r
        self.sym = sym
        self.tf = tf
        self.maxlen_ind = maxlen_ind
        self.maxlen_signal = maxlen_signal
        self._ind_count = 0
        self._signal_count = 0
        logger.info(
            "StreamPublisher initialized | sym=%s tf=%s maxlen_ind=%d maxlen_signal=%d",
            sym,
            tf,
            maxlen_ind,
            maxlen_signal,
        )

    async def publish_indicator(self, payload: Mapping[str, str]) -> str:
        key = st_ind(self.sym, self.tf)
        snap = snap_ind(self.sym, self.tf)

        try:
            # XADD with MAXLEN ~ to keep series bounded
            msg_id = await cast(
                Awaitable[Any],
                self.r.xadd(
                    key,
                    {k: str(v) for k, v in payload.items()},
                    maxlen=self.maxlen_ind,
                    approximate=True,
                ),
            )

            # Keep latest snapshot
            await cast(
                Awaitable[Any],
                self.r.hset(snap, mapping={k: str(v) for k, v in payload.items()}),
            )

            self._ind_count += 1
            decoded_id = (
                msg_id.decode()
                if isinstance(msg_id, (bytes, bytearray))
                else str(msg_id)
            )

            logger.debug(
                "Indicator published | key=%s id=%s ts=%s regime=%s count=%d",
                key,
                decoded_id,
                payload.get("ts"),
                payload.get("regime"),
                self._ind_count,
            )

            if self._ind_count % 100 == 0:
                logger.info(
                    "Indicator milestone | sym=%s tf=%s total_published=%d",
                    self.sym,
                    self.tf,
                    self._ind_count,
            )

            return decoded_id

        except Exception as e:
            logger.error(
                "Failed to publish indicator | key=%s error=%s payload=%s",
                key,
                e,
                payload,
                exc_info=True,
            )
            raise

    async def publish_signal(self, payload: Mapping[str, str]) -> str:
        key = st_signal(self.sym, self.tf)

        try:
            msg_id = await cast(
                Awaitable[Any],
                self.r.xadd(
                    key,
                    {k: str(v) for k, v in payload.items()},
                    maxlen=self.maxlen_signal,
                    approximate=True,
                ),
            )

            self._signal_count += 1
            decoded_id = (
                msg_id.decode()
                if isinstance(msg_id, (bytes, bytearray))
                else str(msg_id)
            )

            logger.info(
                "Signal published | key=%s id=%s type=%s side=%s ts=%s count=%d",
                key,
                decoded_id,
                payload.get("type"),
                payload.get("side") or payload.get("prev_side"),
                payload.get("ts"),
                self._signal_count,
            )

            return decoded_id

        except Exception as e:
            logger.error(
                "Failed to publish signal | key=%s error=%s payload=%s",
                key,
                e,
                payload,
                exc_info=True,
            )
            raise
