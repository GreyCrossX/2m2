from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Tuple,
    cast,
)

from redis.asyncio import Redis

from .keys import stream_signal, offset_key

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class StreamMessage:
    stream_key: str
    symbol: str
    timeframe: str
    payload: Dict
    message_id: str


class SignalStreamConsumer:
    """
    Consume Calc service signals from Redis streams and yield StreamMessage objects.

    Modes
    -----
    - XREAD (fan-out): persists per-stream offsets (optional) and resumes cleanly after restarts.
    - Consumer Group (competing consumers): XREADGROUP reading with group creation if needed.

    Extras
    ------
    - Catch-up protection: skip stale messages older than `catchup_threshold_ms`.
    - Robust parser: supports {"json": "..."} or flat K/V.
    - Dedupe: optional short-term SETNX-based guard per (stream, msg_id).
    - High-level API: `consume_and_handle(handler)` → calls handler and ACKs on success (CG mode).
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
        self._group = consumer_group or f"cg:worker:signal|{timeframe}"
        self._consumer = consumer_name or "worker"

        self._persist_offsets = persist_offsets and not use_consumer_group
        self._catchup_ms = catchup_threshold_ms

        # Internal state:
        # XREAD: stream_key -> last_id
        self._streams_last_id: Dict[str, str] = {}
        # Calculated stream keys
        self._stream_keys: List[str] = [
            stream_signal(sym, self._tf) for sym in self._symbols
        ]
        # Track last ACKed id per stream for observability
        self._last_ack_by_stream: Dict[str, str] = {}

    # ---------------------------------------------------------------------
    # Public APIs
    # ---------------------------------------------------------------------

    async def consume(self) -> AsyncIterator[StreamMessage]:
        """
        Low-level generator API:
          - CG mode: caller is responsible for calling `ack(...)` on success.
          - XREAD mode: offsets are persisted automatically (if enabled).
        """
        await self._prepare()

        if self._use_cg:
            async for item in self._consume_cg():
                yield item
        else:
            async for item in self._consume_xread():
                yield item

    async def consume_and_handle(
        self,
        handler: Callable[[StreamMessage], Awaitable[None]],
        *,
        dedupe_ttl_sec: int = 300,
    ) -> None:
        """
        High-level API:
          - CG mode: read → (optional dedupe) → handler(msg) → XACK on success only.
          - XREAD mode: read → handler(msg) (no XACK concept).
        """
        await self._prepare()

        if self._use_cg:
            async for msg in self._consume_cg():
                # Short-term dedupe (best effort). If this fails, treat as new.
                if not await self._dedupe_mark(
                    msg.stream_key, msg.message_id, ttl_sec=dedupe_ttl_sec
                ):
                    logger.info(
                        "Duplicate message skipped | stream=%s msg_id=%s",
                        msg.stream_key,
                        msg.message_id,
                    )
                    with contextlib.suppress(Exception):
                        # Best-effort XACK in case it was previously processed.
                        await self.ack(msg.stream_key, msg.message_id)
                    continue

                try:
                    await handler(msg)
                except Exception as e:
                    # No ACK on error; allow retry/DLQ policies to take over.
                    logger.error(
                        "Handler error (no ACK) | stream=%s msg_id=%s err=%s",
                        msg.stream_key,
                        msg.message_id,
                        e,
                        exc_info=True,
                    )
                    continue

                # Success → ACK
                acked = await self.ack(msg.stream_key, msg.message_id)
                if not acked:
                    logger.warning(
                        "XACK returned 0 | stream=%s msg_id=%s",
                        msg.stream_key,
                        msg.message_id,
                    )
        else:
            async for msg in self._consume_xread():
                try:
                    await handler(msg)
                except Exception as e:
                    logger.error(
                        "Handler error (XREAD mode) | stream=%s msg_id=%s err=%s",
                        msg.stream_key,
                        msg.message_id,
                        e,
                        exc_info=True,
                    )
                    continue

    # ---------------------------------------------------------------------
    # Setup
    # ---------------------------------------------------------------------

    async def _prepare(self) -> None:
        logger.info("Preparing stream consumer...")
        logger.info("  Mode: %s", "Consumer Group" if self._use_cg else "XREAD")
        logger.info("  Streams: %s", ", ".join(self._stream_keys))
        logger.info(
            "  Start from: %s",
            "latest ($)" if self._start_from_latest else "beginning (0-0)",
        )
        logger.info("  Block timeout: %dms", self._block_ms)
        logger.info("  Persist offsets: %s", self._persist_offsets)
        logger.info("  Catchup threshold: %sms", self._catchup_ms or "disabled")
        logger.info(
            "  Redis decode_responses: %s",
            getattr(self._redis, "decode_responses", "unknown"),
        )

        if self._use_cg:
            logger.info("Setting up consumer group: %s", self._group)
            for sk in self._stream_keys:
                logger.debug("Ensuring group exists for stream: %s", sk)
                await self._ensure_group(
                    sk, self._group, start_latest=self._start_from_latest
                )
            logger.info("Consumer group setup complete")
        else:
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

                # Always store offsets as strings for typing consistency
                self._streams_last_id[str(sk)] = str(last_id)

            logger.info("Stream offset initialization complete")
            logger.debug("Initialized streams_last_id: %s", dict(self._streams_last_id))

        logger.info("Stream consumer preparation complete")

    async def _ensure_group(
        self, stream_key: str, group: str, *, start_latest: bool
    ) -> None:
        # Create group if it doesn't exist
        try:
            start_id = "$" if start_latest else "0-0"
            await self._redis.xgroup_create(
                name=stream_key,
                groupname=group,
                id=start_id,
                mkstream=True,
            )
            logger.info(
                "Created consumer group | stream=%s group=%s", stream_key, group
            )
        except Exception as e:
            # BUSYGROUP or benign errors
            logger.debug(
                "Consumer group setup | stream=%s group=%s err=%s", stream_key, group, e
            )

    # ---------------------------------------------------------------------
    # XREAD mode
    # ---------------------------------------------------------------------

    async def _consume_xread(self) -> AsyncIterator[StreamMessage]:
        logger.info(
            "Starting XREAD consume loop | streams=%d", len(self._streams_last_id)
        )
        logger.debug("Initial offsets: %s", dict(self._streams_last_id))

        iteration = 0
        while True:
            iteration += 1
            try:
                if iteration == 1 or iteration % 100 == 0:
                    logger.debug(
                        "XREAD iteration %d | blocking for %dms... | offsets=%s",
                        iteration,
                        self._block_ms,
                        dict(self._streams_last_id),
                    )

                if not self._streams_last_id:
                    logger.error(
                        "No streams configured for XREAD | dict_len=%d",
                        len(self._streams_last_id),
                    )
                    await asyncio.sleep(1)
                    continue

                # redis-py xread expects: {stream_key: last_id, ...}
                result = await self._redis.xread(
                    streams=cast(Any, dict(self._streams_last_id)),
                    block=self._block_ms,
                )
                # result: List[Tuple[stream_key, List[Tuple[msg_id, fields_dict]]]]

                if not result:
                    if iteration % 10 == 0:
                        logger.debug(
                            "No messages after %d iterations (idle ok)", iteration
                        )
                    continue

                total_entries = sum(len(entries) for _, entries in result)
                logger.debug(
                    "XREAD returned %d stream(s) with %d total entries",
                    len(result),
                    total_entries,
                )

                for raw_stream_key, entries in result:
                    stream_key = self._normalize_stream_key(raw_stream_key)
                    logger.debug(
                        "Processing %d entries from stream: %s",
                        len(entries),
                        stream_key,
                    )

                    for raw_msg_id, fields in entries:
                        msg_id = self._normalize_msg_id(raw_msg_id)
                        payload = self._parse(fields)

                        # Staleness check
                        if self._is_stale(payload, self._catchup_ms):
                            logger.debug(
                                "Skipping stale message | stream=%s msg_id=%s",
                                stream_key,
                                msg_id,
                            )
                            self._update_last_id(raw_stream_key, msg_id)
                            if self._persist_offsets:
                                await self._save_offset(stream_key, msg_id)
                            continue

                        sym, tf = self._split_stream_key(stream_key)

                        # Advance offset & persist BEFORE yielding (at-least-once semantics)
                        self._update_last_id(raw_stream_key, msg_id)
                        if self._persist_offsets:
                            await self._save_offset(stream_key, msg_id)

                        logger.debug(
                            "Yielding message | stream=%s msg_id=%s type=%s",
                            stream_key,
                            msg_id,
                            payload.get("type", "unknown"),
                        )
                        yield StreamMessage(
                            stream_key=stream_key,
                            symbol=sym,
                            timeframe=tf,
                            payload=payload,
                            message_id=msg_id,
                        )

            except asyncio.CancelledError:
                logger.info("XREAD consume loop cancelled")
                raise
            except Exception as e:
                logger.error("XREAD error (retry in 0.5s) | err=%s", e, exc_info=True)
                await asyncio.sleep(0.5)

    # ---------------------------------------------------------------------
    # Consumer Group mode
    # ---------------------------------------------------------------------

    async def _consume_cg(self) -> AsyncIterator[StreamMessage]:
        """
        Uses XREADGROUP against all tracked streams with the same consumer group.
        """
        logger.info(
            "Starting XREADGROUP consume loop | group=%s consumer=%s streams=%d",
            self._group,
            self._consumer,
            len(self._stream_keys),
        )

        iteration = 0
        while True:
            iteration += 1
            try:
                if iteration == 1 or iteration % 100 == 0:
                    logger.debug(
                        "XREADGROUP iteration %d | blocking for %dms...",
                        iteration,
                        self._block_ms,
                    )

                # IMPORTANT: use '>' for new messages
                streams = {sk: ">" for sk in self._stream_keys}
                result = await self._redis.xreadgroup(
                    groupname=self._group,
                    consumername=self._consumer,
                    streams=cast(Any, streams),
                    block=self._block_ms,
                )
                if not result:
                    if iteration % 10 == 0:
                        logger.debug(
                            "No messages after %d iterations (idle ok)", iteration
                        )
                    continue

                total_entries = sum(len(entries) for _, entries in result)
                logger.debug(
                    "XREADGROUP returned %d stream(s) with %d total entries",
                    len(result),
                    total_entries,
                )

                for raw_stream_key, entries in result:
                    stream_key = self._normalize_stream_key(raw_stream_key)
                    logger.debug(
                        "Processing %d entries from stream: %s",
                        len(entries),
                        stream_key,
                    )

                    for raw_msg_id, fields in entries:
                        msg_id = self._normalize_msg_id(raw_msg_id)
                        payload = self._parse(fields)

                        # Stale → ACK and skip
                        if self._is_stale(payload, self._catchup_ms):
                            logger.debug(
                                "Skipping stale (ACKing) | stream=%s msg_id=%s",
                                stream_key,
                                msg_id,
                            )
                            with contextlib.suppress(Exception):
                                acked = await self._redis.xack(
                                    stream_key, self._group, msg_id
                                )
                                if acked:
                                    self._last_ack_by_stream[stream_key] = msg_id
                            continue

                        sym, tf = self._split_stream_key(stream_key)

                        logger.debug(
                            "Yielding message | stream=%s msg_id=%s type=%s",
                            stream_key,
                            msg_id,
                            payload.get("type", "unknown"),
                        )
                        yield StreamMessage(
                            stream_key=stream_key,
                            symbol=sym,
                            timeframe=tf,
                            payload=payload,
                            message_id=msg_id,
                        )

            except asyncio.CancelledError:
                logger.info("XREADGROUP consume loop cancelled")
                raise
            except Exception as e:
                logger.error(
                    "XREADGROUP error (retry in 0.5s) | err=%s", e, exc_info=True
                )
                await asyncio.sleep(0.5)

    async def ack(self, stream_key: str, msg_id: str) -> bool:
        """ACK a message for the configured consumer group (no-op in XREAD mode)."""
        if not self._use_cg:
            return True
        try:
            acked = await self._redis.xack(stream_key, self._group, msg_id)
        except Exception as exc:
            logger.error(
                "XACK error | stream=%s msg_id=%s err=%s",
                stream_key,
                msg_id,
                exc,
                exc_info=True,
            )
            return False
        if acked:
            self._last_ack_by_stream[stream_key] = msg_id
            logger.debug("ACKed message | stream=%s msg_id=%s", stream_key, msg_id)
            return True
        logger.debug("XACK returned 0 | stream=%s msg_id=%s", stream_key, msg_id)
        return False

    async def cleanup_stale_pending(
        self, idle_ms: int = 300_000, limit: int = 50
    ) -> Dict[str, int]:
        """Best-effort cleanup of stale pending entries for all tracked streams."""
        cleaned: Dict[str, int] = {}
        if not self._use_cg:
            return cleaned

        for stream_key in self._stream_keys:
            try:
                pending = await self._redis.xpending_range(
                    stream_key,
                    self._group,
                    min="-",
                    max="+",
                    count=limit,
                    idle=idle_ms,
                )
            except Exception as exc:
                logger.debug(
                    "XPENDING cleanup skipped | stream=%s err=%s", stream_key, exc
                )
                continue

            if not pending:
                continue

            stale_ids = [
                entry["message_id"]
                for entry in pending
                if entry.get("idle", 0) >= idle_ms
            ]
            if not stale_ids:
                continue

            try:
                acked = await self._redis.xack(stream_key, self._group, *stale_ids)
                if acked:
                    cleaned[stream_key] = acked
                    self._last_ack_by_stream[stream_key] = stale_ids[-1]
                    logger.info(
                        "Cleaned %d stale pending | stream=%s last_id=%s",
                        acked,
                        stream_key,
                        stale_ids[-1],
                    )
            except Exception as exc:
                logger.warning(
                    "Failed to clean pending | stream=%s err=%s", stream_key, exc
                )

        return cleaned

    def last_ack(self, stream_key: str) -> Optional[str]:
        """Return the last ACKed id (if any) for the given stream."""
        return self._last_ack_by_stream.get(stream_key)

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
                with contextlib.suppress(Exception):
                    decoded = val.decode()
                    logger.debug("Loaded offset | key=%s offset=%s", k, decoded)
                    return decoded
                logger.warning("Failed to decode offset | key=%s", k)
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
            logger.warning(
                "Failed to save offset | key=%s offset=%s err=%s", k, msg_id, e
            )

    # ---------------------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------------------

    @staticmethod
    def _parse(fields: Mapping[bytes, bytes]) -> Dict:
        """
        Supports:
          1) {"json": b'{"type": "...", ...}'}
          2) flat fields: {b"type": b"arm", b"sym": b"BTCUSDT", ...}
        """
        decoded = {k.decode(): v.decode() for k, v in fields.items()}
        if "json" in decoded:
            with contextlib.suppress(Exception):
                return json.loads(decoded["json"])
        return decoded

    def _normalize_stream_key(self, stream_key: str | bytes) -> str:
        if isinstance(stream_key, (bytes, bytearray)):
            with contextlib.suppress(Exception):
                return stream_key.decode()
            return str(stream_key)
        return str(stream_key)

    @staticmethod
    def _normalize_msg_id(msg_id: str | bytes) -> str:
        if isinstance(msg_id, (bytes, bytearray)):
            with contextlib.suppress(Exception):
                return msg_id.decode()
            return str(msg_id)
        return str(msg_id)

    def _update_last_id(self, stream_key: str | bytes, msg_id: str) -> None:
        key = (
            stream_key
            if isinstance(stream_key, str) and stream_key in self._streams_last_id
            else self._normalize_stream_key(stream_key)
        )
        self._streams_last_id[key] = msg_id

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
                logger.debug(
                    "Message is stale | age_ms=%.1f threshold_ms=%d",
                    age_ms,
                    threshold_ms,
                )
            return is_stale
        except Exception as e:
            logger.debug("Could not determine staleness | err=%s", e)
            return False

    async def _dedupe_mark(
        self, stream_key: str, msg_id: str, ttl_sec: int = 300
    ) -> bool:
        """Best-effort dedupe using SETNX + EX per (stream, msg_id)."""
        try:
            key = f"dedupe:{stream_key}:{msg_id}"
            created = await self._redis.setnx(key, "1")
            if created:
                with contextlib.suppress(Exception):
                    await self._redis.expire(key, ttl_sec)
            return bool(created)
        except Exception as e:
            logger.debug("dedupe_mark failed open (treat as new) | err=%s", e)
            return True
