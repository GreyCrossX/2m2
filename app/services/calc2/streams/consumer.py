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
    """Asynchronous consumer for Redis streams yielding Candle objects.

    This class reads messages from a Redis stream for a given market symbol and timeframe,
    converting them into Candle objects. It supports resuming from a specific stream ID
    and handles byte-to-string decoding for Redis responses.
    """

    def __init__(self, redis_client: Redis, symbol: str, timeframe: str, block_timeout_ms: int = 15000) -> None:
        """Initialize the StreamConsumer.

        Args:
            redis_client: Redis client instance for async operations.
            symbol: Market symbol (e.g., 'BTCUSD').
            timeframe: Timeframe for the stream (e.g., '1m').
            block_timeout_ms: Timeout in milliseconds for blocking xread calls (default: 15000).
        """
        self.redis_client = redis_client
        self.symbol = symbol
        self.timeframe = timeframe
        self.block_timeout_ms = block_timeout_ms
        self._last_id: str = "0-0"

    async def resume_from(self, last_id: Optional[str]) -> None:
        """Resume reading the stream from a specific stream ID.

        Args:
            last_id: The stream ID to resume from. If None, starts from the beginning ('0-0').

        Raises:
            ValueError: If last_id is invalid (e.g., does not match Redis stream ID format).
        """
        if last_id:
            # Basic validation for stream ID format (e.g., 'ms-seq')
            if not isinstance(last_id, str) or not last_id.replace("-", "").isdigit():
                raise ValueError("Invalid stream ID format")
            self._last_id = last_id

    async def candles(self) -> AsyncIterator[Candle]:
        """Yield Candle objects from the Redis stream.

        Reads messages from the Redis stream for the specified symbol and timeframe,
        decodes them, and converts them to Candle objects. If a message contains a 'data'
        field with nested JSON, it attempts to parse it.

        Yields:
            Candle: A Candle object parsed from the stream message.

        Raises:
            RuntimeError: If an unexpected error occurs while reading the stream.
        """
        stream_key = st_market(self.symbol, self.timeframe)
        while True:
            try:
                # Read from the Redis stream
                response = await self.redis_client.xread(
                    {stream_key: self._last_id}, block=self.block_timeout_ms, count=1000
                )
                # Handle empty response
                if not response or stream_key not in response:
                    continue

                # Process stream entries
                for entry_id, fields in response[stream_key]:
                    # Decode entry_id if it's bytes
                    self._last_id = (
                        entry_id.decode() if isinstance(entry_id, (bytes, bytearray)) else entry_id
                    )
                    # Decode fields (keys and values) if they are bytes
                    message_data = {
                        k.decode() if isinstance(k, (bytes, bytearray)) else k:
                        v.decode() if isinstance(v, (bytes, bytearray)) else v
                        for k, v in fields.items()
                    }
                    # Handle nested JSON in 'data' field
                    if "data" in message_data:
                        try:
                            if isinstance(message_data["data"], str):
                                message_data = json.loads(message_data["data"])
                        except json.JSONDecodeError as e:
                            logger.warning(f"Failed to parse JSON in 'data' field: {e}")
                            continue  # Skip invalid JSON messages
                    try:
                        yield Candle.from_msg(message_data)
                    except Exception as e:
                        logger.error(f"Failed to create Candle from message: {e}")
                        continue  # Skip invalid messages
            except asyncio.CancelledError:
                logger.info("Stream consumer cancelled")
                raise
            except Exception as e:
                logger.error(f"Error reading from stream {stream_key}: {e}")
                raise RuntimeError(f"Stream read failed: {e}")