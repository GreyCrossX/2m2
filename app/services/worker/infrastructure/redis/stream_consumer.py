from __future__ import annotations

import asyncio
import contextlib
import json
from datetime import datetime, timezone
from typing import AsyncIterator, Dict, List, Mapping, Optional, Tuple

from redis.asyncio import Redis

from .keys import stream_signal, offset_key


class SignalStreamConsumer:
    """
    Consume Calc service signals from Redis streams and yield (symbol, timeframe, payload, msg_id).

    Features
    --------
    - Supports XREAD (simple fan-out) and Consumer Group (competing consumers) modes
    - Persists offsets per stream in XREAD mode for clean restarts
    - Skips stale signals using a catch-up threshold (ms) to avoid acting on old data
    - Robust parser: supports single-field {"json": "..."} or flat K/V (bytes) payloads

    Typical Flow
    ------------
    binance WS -> redis ingestor -> calc service -> (this) worker poller -> bots -> binance order(s)

    Usage
    -----
        redis = Redis.from_url(cfg.redis_url, decode_responses=False)

        consumer = SignalStreamConsumer(
            redis=redis,
            symbols=cfg.symbols,
            timeframe=cfg.timeframe,
            block_ms=cfg.stream_block_ms,
            start_from_latest=True,       # follow-the-head in prod
            use_consumer_group=False,     # True if scaling horizontally
            consumer_group=f"cg:worker:signal|{cfg.timeframe}",
            consumer_name="worker-1",
            persist_offsets=True,
            catchup_threshold_ms=cfg.catchup_threshold_ms,
        )

        async for sym, tf, payload, msg_id in consumer.consume():
            ...

    Notes
    -----
    - When use_consumer_group=False (XREAD), we store last delivered msg id in key:
          worker:offset:signal|{SYMBOL:TF}
      so the worker can resume after restarts.
    - When use_consumer_group=True, we create (if missing) a single group on each stream
      and ACK after successfully yielding to the caller.
    """

    def __init__(
        self,
        redis: Redis,
        symbols: List[str],
        timeframe: str,
        *,
        block_ms: int = 15_000,
        start_from_latest: bool = True,
        # Consumer Group mode:
        use_consumer_group: bool = False,
        consumer_group: Optional[str] = None,
        consumer_name: Optional[str] = None,
        # XREAD offsets:
        persist_offsets: bool = True,
        # Staleness protection:
        catchup_threshold_ms: Optional[int] = None,
    ) -> None:
        self._redis = redis
        self._symbols = [s.upper() for s in symbols]
        self._tf = timeframe
        self._block_ms = block_ms
        self._start_from_latest = start_from_latest

        self._use_cg = use_consumer_group
        # Use a single group name across all streams (simplifies XREADGROUP calls)
        self._group = consumer_group or f"cg:worker:signal|{timeframe}"
        self._consumer = consumer_name or "worker"

        self._persist_offsets = persist_offsets and not use_consumer_group
        self._catchup_ms = catchup_threshold_ms

        # Internal state:
        # XREAD: stream_key -> last_id
        self._streams_last_id: Dict[str, str] = {}
        # Calculated stream keys
        self._stream_keys: List[str] = [stream_signal(sym, self._tf) for sym in self._symbols]

    # ---------------------------------------------------------------------
    # Public API
    # ---------------------------------------------------------------------

    async def consume(self) -> AsyncIterator[Tuple[str, str, Dict, str]]:
        """
        Yields (symbol, timeframe, payload, message_id).

        In Consumer Group mode, messages are ACKed after yielding (best-effort).
        In XREAD mode, last offsets are persisted (if enabled) as they are consumed.
        """
        await self._prepare()

        if self._use_cg:
            async for item in self._consume_cg():
                yield item
        else:
            async for item in self._consume_xread():
                yield item

    # ---------------------------------------------------------------------
    # Setup
    # ---------------------------------------------------------------------

    async def _prepare(self) -> None:
        if self._use_cg:
            # Ensure each stream has the consumer group (create if missing)
            for sk in self._stream_keys:
                await self._ensure_group(sk, self._group, start_latest=self._start_from_latest)
        else:
            # Load or initialize offsets per stream
            for sk in self._stream_keys:
                if self._persist_offsets:
                    last_id = await self._load_offset(sk)
                else:
                    last_id = None
                if not last_id:
                    last_id = "$" if self._start_from_latest else "0-0"
                self._streams_last_id[sk] = last_id

    async def _ensure_group(self, stream_key: str, group: str, *, start_latest: bool) -> None:
        # Create consumer group if it doesn't exist
        with contextlib.suppress(Exception):
            start_id = "$" if start_latest else "0-0"
            await self._redis.xgroup_create(
                name=stream_key,
                groupname=group,
                id=start_id,
                mkstream=True,
            )

    # ---------------------------------------------------------------------
    # XREAD mode
    # ---------------------------------------------------------------------

    async def _consume_xread(self) -> AsyncIterator[Tuple[str, str, Dict, str]]:
        streams = list(self._streams_last_id.items())  # [(key, last_id), ...]
        while True:
            try:
                result = await self._redis.xread(streams=streams, block=self._block_ms)
                # result: List[Tuple[stream_key, List[Tuple[msg_id, fields_dict]]]]
                if not result:
                    continue

                for stream_key, entries in result:
                    for msg_id, fields in entries:
                        payload = self._parse(fields)
                        # Staleness check (uses payload["ts"] in epoch ms if present)
                        if self._is_stale(payload, self._catchup_ms):
                            # Advance offset & persist, but skip delivering the message
                            self._streams_last_id[stream_key] = msg_id
                            if self._persist_offsets:
                                await self._save_offset(stream_key, msg_id)
                            continue

                        sym, tf = self._split_stream_key(stream_key)
                        # Advance offset & persist
                        self._streams_last_id[stream_key] = msg_id
                        if self._persist_offsets:
                            await self._save_offset(stream_key, msg_id)

                        yield sym, tf, payload, msg_id

                # Refresh tuple with updated last ids
                streams = list(self._streams_last_id.items())

            except asyncio.CancelledError:
                raise
            except Exception:
                # Backoff lightly on transient errors
                await asyncio.sleep(0.5)

    # ---------------------------------------------------------------------
    # Consumer Group mode
    # ---------------------------------------------------------------------

    async def _consume_cg(self) -> AsyncIterator[Tuple[str, str, Dict, str]]:
        """
        Uses XREADGROUP against all tracked streams with the same consumer group.
        ACKs after yielding (best-effort).
        """
        while True:
            try:
                # Build streams dict as {stream_key: '>'} for new messages
                streams = {sk: ">" for sk in self._stream_keys}
                result = await self._redis.xreadgroup(
                    groupname=self._group,
                    consumername=self._consumer,
                    streams=streams,
                    block=self._block_ms,
                )
                if not result:
                    continue

                for stream_key, entries in result:
                    for msg_id, fields in entries:
                        payload = self._parse(fields)

                        # Staleness check: ACK and skip if stale
                        if self._is_stale(payload, self._catchup_ms):
                            with contextlib.suppress(Exception):
                                await self._redis.xack(stream_key, self._group, msg_id)
                            continue

                        sym, tf = self._split_stream_key(stream_key)
                        yield sym, tf, payload, msg_id

                        # ACK after yielding (best-effort)
                        with contextlib.suppress(Exception):
                            await self._redis.xack(stream_key, self._group, msg_id)

            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(0.5)

    # ---------------------------------------------------------------------
    # Offsets (XREAD mode only)
    # ---------------------------------------------------------------------

    async def _load_offset(self, stream_key: str) -> Optional[str]:
        sym, tf = self._split_stream_key(stream_key)
        k = offset_key(sym, tf)
        val = await self._redis.get(k)
        if val is None:
            return None
        if isinstance(val, (bytes, bytearray)):
            try:
                return val.decode()
            except Exception:
                return None
        return str(val)

    async def _save_offset(self, stream_key: str, msg_id: str) -> None:
        sym, tf = self._split_stream_key(stream_key)
        k = offset_key(sym, tf)
        await self._redis.set(k, msg_id)

    # ---------------------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------------------

    @staticmethod
    def _parse(fields: Mapping[bytes, bytes]) -> Dict:
        """
        Supports both:
          1) {"json": b'{"type": "...", ...}'}
          2) flat fields: {b"type": b"arm", b"sym": b"BTCUSDT", ...}
        """
        decoded = {k.decode(): v.decode() for k, v in fields.items()}
        if "json" in decoded:
            with contextlib.suppress(Exception):
                return json.loads(decoded["json"])
        return decoded

    @staticmethod
    def _split_stream_key(stream_key: str) -> Tuple[str, str]:
        """
        stream:signal|{BTCUSDT:2m} -> ("BTCUSDT", "2m")
        """
        try:
            inner = stream_key.split("{", 1)[1].split("}", 1)[0]
            sym, tf = inner.split(":", 1)
            return sym, tf
        except Exception:
            return "", ""

    @staticmethod
    def _is_stale(payload: Dict, threshold_ms: Optional[int]) -> bool:
        """
        Returns True if payload.ts (epoch ms) is older than threshold_ms.
        If threshold_ms is None or ts missing/unparseable, returns False.
        """
        if not threshold_ms:
            return False
        try:
            ts_ms = int(str(payload.get("ts")))
            dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
            age_ms = (datetime.now(timezone.utc) - dt).total_seconds() * 1000.0
            return age_ms > threshold_ms
        except Exception:
            return False
