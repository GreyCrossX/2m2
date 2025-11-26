from __future__ import annotations
import asyncio
import json
import logging
from typing import AsyncIterator, Optional

from redis.asyncio import Redis

from ..models import Candle
from ..utils.keys import st_market

logger = logging.getLogger(__name__)


class StreamConsumer:
    """Asynchronous consumer for Redis streams yielding Candle objects."""

    def __init__(
        self,
        redis_client: Redis,
        symbol: str,
        timeframe: str,
        block_timeout_ms: int = 15000,
    ) -> None:
        self.redis_client = redis_client
        self.symbol = symbol
        self.timeframe = timeframe
        self.block_timeout_ms = block_timeout_ms
        self._last_id: str = "0-0"
        logger.info(
            "StreamConsumer initialized | symbol=%s timeframe=%s block_timeout=%dms",
            symbol,
            timeframe,
            block_timeout_ms,
        )

    async def resume_from(self, last_id: Optional[str]) -> None:
        """Resume reading the stream from a specific stream ID."""
        if last_id:
            if not isinstance(last_id, str) or not last_id.replace("-", "").isdigit():
                logger.error("Invalid stream ID format | last_id=%s", last_id)
                raise ValueError("Invalid stream ID format")
            self._last_id = last_id
            logger.info(
                "Resuming stream | symbol=%s timeframe=%s from_id=%s",
                self.symbol,
                self.timeframe,
                last_id,
            )
        else:
            logger.info(
                "Starting stream from beginning | symbol=%s timeframe=%s",
                self.symbol,
                self.timeframe,
            )

    async def candles(self) -> AsyncIterator[Candle]:
        """Yield Candle objects from the Redis stream."""
        stream_key = st_market(self.symbol, self.timeframe)
        logger.info(
            "Starting candle stream | key=%s starting_from=%s",
            stream_key,
            self._last_id,
        )

        candle_count = 0
        batch_count = 0

        while True:
            try:
                # Read from the Redis stream
                logger.debug(
                    "Reading stream | key=%s last_id=%s timeout=%dms",
                    stream_key,
                    self._last_id,
                    self.block_timeout_ms,
                )

                response = await self.redis_client.xread(
                    {stream_key: self._last_id}, block=self.block_timeout_ms, count=1000
                )

                # Handle empty response
                if not response:
                    logger.debug(
                        "No new messages (timeout) | key=%s last_id=%s",
                        stream_key,
                        self._last_id,
                    )
                    # Yield control to other tasks
                    await asyncio.sleep(0)
                    continue

                # Redis xread returns a list of [stream_key, entries]
                # Format: [[b'stream:key', [(b'id', {b'field': b'value'})]]]
                logger.debug(
                    "Response type: %s, length: %d",
                    type(response).__name__,
                    len(response),
                )

                # Find our stream in the response
                stream_data = None
                for item in response:
                    # item is [stream_key_bytes, entries_list]
                    if len(item) >= 2:
                        key_bytes = item[0]
                        key_str = (
                            key_bytes.decode()
                            if isinstance(key_bytes, (bytes, bytearray))
                            else str(key_bytes)
                        )
                        if key_str == stream_key:
                            stream_data = item[1]
                            break

                if stream_data is None:
                    logger.warning(
                        "Stream key not found in response | expected=%s", stream_key
                    )
                    continue

                entries = stream_data
                batch_count += 1
                logger.debug(
                    "Received batch | key=%s batch_num=%d entries=%d",
                    stream_key,
                    batch_count,
                    len(entries),
                )

                # Process stream entries
                for entry_id, fields in entries:
                    # Decode entry_id if it's bytes
                    self._last_id = (
                        entry_id.decode()
                        if isinstance(entry_id, (bytes, bytearray))
                        else entry_id
                    )

                    # Decode fields (keys and values) if they are bytes
                    message_data = {
                        k.decode()
                        if isinstance(k, (bytes, bytearray))
                        else k: v.decode() if isinstance(v, (bytes, bytearray)) else v
                        for k, v in fields.items()
                    }

                    # Handle nested JSON in 'data' field
                    if "data" in message_data:
                        try:
                            if isinstance(message_data["data"], str):
                                message_data = json.loads(message_data["data"])
                                logger.debug(
                                    "Parsed nested JSON data | entry_id=%s",
                                    self._last_id,
                                )
                        except json.JSONDecodeError as e:
                            logger.warning(
                                "Failed to parse JSON in 'data' field | entry_id=%s error=%s",
                                self._last_id,
                                e,
                            )
                            continue

                    try:
                        candle = Candle.from_msg(message_data)
                        candle_count += 1
                        logger.debug(
                            "Candle parsed | entry_id=%s ts=%d sym=%s close=%s count=%d",
                            self._last_id,
                            candle.ts,
                            candle.sym,
                            candle.close,
                            candle_count,
                        )
                        yield candle
                    except Exception as e:
                        logger.error(
                            "Failed to create Candle from message | entry_id=%s error=%s data=%s",
                            self._last_id,
                            e,
                            message_data,
                        )
                        continue

            except asyncio.CancelledError:
                logger.info(
                    "Stream consumer cancelled | key=%s candles_processed=%d batches=%d",
                    stream_key,
                    candle_count,
                    batch_count,
                )
                raise
            except Exception as e:
                logger.error(
                    "Error reading from stream | key=%s last_id=%s error=%s",
                    stream_key,
                    self._last_id,
                    e,
                    exc_info=True,
                )
                raise RuntimeError(f"Stream read failed: {e}")
