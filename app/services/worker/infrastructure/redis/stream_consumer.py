from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from datetime import datetime, timezone
from typing import AsyncIterator, Dict, List, Mapping, Optional, Tuple

from redis.asyncio import Redis

from .keys import stream_signal, offset_key


logger = logging.getLogger(__name__)


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
        logger.info("Preparing stream consumer...")
        logger.info("  Mode: %s", "Consumer Group" if self._use_cg else "XREAD")
        logger.info("  Streams: %s", ", ".join(self._stream_keys))
        logger.info("  Start from: %s", "latest ($)" if self._start_from_latest else "beginning (0-0)")
        logger.info("  Block timeout: %dms", self._block_ms)
        logger.info("  Persist offsets: %s", self._persist_offsets)
        logger.info("  Catchup threshold: %sms", self._catchup_ms or "disabled")
        logger.info("  Redis decode_responses: %s", getattr(self._redis, 'decode_responses', 'unknown'))
        
        if self._use_cg:
            # Ensure each stream has the consumer group (create if missing)
            logger.info("Setting up consumer group: %s", self._group)
            for sk in self._stream_keys:
                logger.debug("Ensuring group exists for stream: %s", sk)
                await self._ensure_group(sk, self._group, start_latest=self._start_from_latest)
            logger.info("Consumer group setup complete")
        else:
            # Load or initialize offsets per stream
            logger.info("Loading stream offsets...")
            for sk in self._stream_keys:
                if self._persist_offsets:
                    last_id = await self._load_offset(sk)
                    if last_id:
                        logger.info("  %s: resuming from %s", sk, last_id)
                else:
                    last_id = None
                if not last_id:
                    last_id = "$" if self._start_from_latest else "0-0"
                    logger.info("  %s: starting from %s", sk, last_id)
                
                # Convert to bytes if Redis is not decoding responses
                if not getattr(self._redis, 'decode_responses', True):
                    logger.debug("Converting stream key to bytes: %s", sk)
                    self._streams_last_id[sk.encode()] = last_id.encode()
                else:
                    self._streams_last_id[sk] = last_id
                    
            logger.info("Stream offset initialization complete")
            logger.debug("Initialized streams_last_id: %s", dict(self._streams_last_id))
        
        logger.info("Stream consumer preparation complete")

    async def _ensure_group(self, stream_key: str, group: str, *, start_latest: bool) -> None:
        # Create consumer group if it doesn't exist
        try:
            start_id = "$" if start_latest else "0-0"
            await self._redis.xgroup_create(
                name=stream_key,
                groupname=group,
                id=start_id,
                mkstream=True,
            )
            logger.info("Created consumer group | stream=%s group=%s", stream_key, group)
        except Exception as e:
            # Group likely already exists (BUSYGROUP error is normal)
            logger.debug("Consumer group setup | stream=%s group=%s err=%s", stream_key, group, e)

    # ---------------------------------------------------------------------
    # XREAD mode
    # ---------------------------------------------------------------------

    async def _consume_xread(self) -> AsyncIterator[Tuple[str, str, Dict, str]]:
        logger.info("Starting XREAD consume loop | streams=%d", len(self._streams_last_id))
        logger.debug("Initial offsets: %s", dict(self._streams_last_id))
        
        iteration = 0
        while True:
            iteration += 1
            try:
                if iteration == 1 or iteration % 100 == 0:
                    logger.debug("XREAD iteration %d | blocking for %dms... | offsets=%s", 
                               iteration, self._block_ms, dict(self._streams_last_id))
                
                # redis-py xread expects a dict: {stream_key: last_id, ...}
                if not self._streams_last_id:
                    logger.error("No streams configured for XREAD | dict_len=%d", len(self._streams_last_id))
                    await asyncio.sleep(1)
                    continue
                
                result = await self._redis.xread(streams=self._streams_last_id, block=self._block_ms)
                # result: List[Tuple[stream_key, List[Tuple[msg_id, fields_dict]]]]
                if not result:
                    if iteration % 10 == 0:
                        logger.debug("No messages after %d iterations (normal during idle periods)", iteration)
                    continue

                total_entries = sum(len(entries) for _, entries in result)
                logger.debug("XREAD returned %d stream(s) with %d total entries", len(result), total_entries)

                for stream_key, entries in result:
                    logger.debug("Processing %d entries from stream: %s", len(entries), stream_key)
                    
                    for msg_id, fields in entries:
                        payload = self._parse(fields)
                        # Staleness check (uses payload["ts"] in epoch ms if present)
                        if self._is_stale(payload, self._catchup_ms):
                            # Advance offset & persist, but skip delivering the message
                            logger.debug("Skipping stale message | stream=%s msg_id=%s", stream_key, msg_id)
                            self._streams_last_id[stream_key] = msg_id
                            if self._persist_offsets:
                                await self._save_offset(stream_key, msg_id)
                            continue

                        sym, tf = self._split_stream_key(stream_key)
                        # Advance offset & persist
                        self._streams_last_id[stream_key] = msg_id
                        if self._persist_offsets:
                            await self._save_offset(stream_key, msg_id)

                        logger.debug("Yielding message | stream=%s msg_id=%s type=%s", 
                                   stream_key, msg_id, payload.get('type', 'unknown'))
                        yield sym, tf, payload, msg_id

            except asyncio.CancelledError:
                logger.info("XREAD consume loop cancelled")
                raise
            except Exception as e:
                logger.error("XREAD error (will retry after 0.5s) | err=%s", e, exc_info=True)
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
        logger.info("Starting XREADGROUP consume loop | group=%s consumer=%s streams=%d", 
                   self._group, self._consumer, len(self._stream_keys))
        
        iteration = 0
        while True:
            iteration += 1
            try:
                if iteration == 1 or iteration % 100 == 0:
                    logger.debug("XREADGROUP iteration %d | blocking for %dms...", iteration, self._block_ms)
                
                # Build streams dict as {stream_key: '>'} for new messages
                streams = {sk: ">" for sk in self._stream_keys}
                result = await self._redis.xreadgroup(
                    groupname=self._group,
                    consumername=self._consumer,
                    streams=streams,
                    block=self._block_ms,
                )
                if not result:
                    if iteration % 10 == 0:
                        logger.debug("No messages after %d iterations (normal during idle periods)", iteration)
                    continue

                total_entries = sum(len(entries) for _, entries in result)
                logger.debug("XREADGROUP returned %d stream(s) with %d total entries", len(result), total_entries)

                for stream_key, entries in result:
                    logger.debug("Processing %d entries from stream: %s", len(entries), stream_key)
                    
                    for msg_id, fields in entries:
                        payload = self._parse(fields)

                        # Staleness check: ACK and skip if stale
                        if self._is_stale(payload, self._catchup_ms):
                            logger.debug("Skipping stale message (ACKing) | stream=%s msg_id=%s", stream_key, msg_id)
                            with contextlib.suppress(Exception):
                                await self._redis.xack(stream_key, self._group, msg_id)
                            continue

                        sym, tf = self._split_stream_key(stream_key)
                        
                        logger.debug("Yielding message | stream=%s msg_id=%s type=%s", 
                                   stream_key, msg_id, payload.get('type', 'unknown'))
                        yield sym, tf, payload, msg_id

                        # ACK after yielding (best-effort)
                        with contextlib.suppress(Exception):
                            await self._redis.xack(stream_key, self._group, msg_id)
                            logger.debug("ACKed message | stream=%s msg_id=%s", stream_key, msg_id)

            except asyncio.CancelledError:
                logger.info("XREADGROUP consume loop cancelled")
                raise
            except Exception as e:
                logger.error("XREADGROUP error (will retry after 0.5s) | err=%s", e, exc_info=True)
                await asyncio.sleep(0.5)

    # ---------------------------------------------------------------------
    # Offsets (XREAD mode only)
    # ---------------------------------------------------------------------

    async def _load_offset(self, stream_key: str) -> Optional[str]:
        sym, tf = self._split_stream_key(stream_key)
        k = offset_key(sym, tf)
        try:
            val = await self._redis.get(k)
            if val is None:
                logger.debug("No stored offset found | key=%s", k)
                return None
            if isinstance(val, (bytes, bytearray)):
                try:
                    decoded = val.decode()
                    logger.debug("Loaded offset | key=%s offset=%s", k, decoded)
                    return decoded
                except Exception as e:
                    logger.warning("Failed to decode offset | key=%s err=%s", k, e)
                    return None
            result = str(val)
            logger.debug("Loaded offset | key=%s offset=%s", k, result)
            return result
        except Exception as e:
            logger.error("Error loading offset | key=%s err=%s", k, e)
            return None

    async def _save_offset(self, stream_key: str, msg_id: str) -> None:
        sym, tf = self._split_stream_key(stream_key)
        k = offset_key(sym, tf)
        try:
            await self._redis.set(k, msg_id)
            logger.debug("Saved offset | key=%s offset=%s", k, msg_id)
        except Exception as e:
            logger.warning("Failed to save offset | key=%s offset=%s err=%s", k, msg_id, e)

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
            is_stale = age_ms > threshold_ms
            if is_stale:
                logger.debug("Message is stale | age_ms=%.1f threshold_ms=%d", age_ms, threshold_ms)
            return is_stale
        except Exception as e:
            logger.debug("Could not determine staleness | err=%s", e)
            return False