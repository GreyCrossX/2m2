from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Dict, Optional

from redis.asyncio import Redis

from ..application.signal_processor import SignalProcessor
from ..core.logging_utils import ensure_log_context, format_log_context
from ..domain.enums import SignalType
from ..domain.exceptions import InvalidSignalException
from ..domain.models import ArmSignal, DisarmSignal
from ..infrastructure.metrics import WorkerMetrics
from ..infrastructure.postgres.repositories import BotRepository
from ..infrastructure.redis.stream_consumer import SignalStreamConsumer, StreamMessage
from .router import SymbolRouter

logger = logging.getLogger(__name__)


class WorkerPoller:
    """Consume Redis stream signals and delegate to the SignalProcessor."""

    def __init__(
        self,
        config,
        stream_consumer: SignalStreamConsumer,
        signal_processor: SignalProcessor,
        router: SymbolRouter,
        bot_repository: BotRepository,
        *,
        redis: Redis,
        metrics: WorkerMetrics,
        router_refresh_seconds: Optional[int] = None,
        dedupe_ttl_seconds: int = 300,
    ) -> None:
        self._cfg = config
        self._consumer = stream_consumer
        self._sp = signal_processor
        self._router = router
        self._bots = bot_repository
        self._redis = redis
        self._metrics = metrics
        self._running = False
        self._reload_task: Optional[asyncio.Task] = None
        self._router_refresh_seconds = router_refresh_seconds or getattr(config, "router_refresh_seconds", 60)
        self._dedupe_ttl = max(dedupe_ttl_seconds, 1)
        self._router_fp: Optional[int] = None

    async def start(self) -> None:
        """Load bots, warm the router, and begin the consume loop."""
        self._running = True
        logger.info("Fetching enabled bots from database...")
        await self._reload_bots()

        # Clean stale pendings so the group is healthy before starting
        with contextlib.suppress(Exception):
            await self._consumer.cleanup_stale_pending(idle_ms=self._dedupe_ttl * 1000, limit=100)

        logger.info(
            "Starting background router refresher (interval=%ds)...",
            self._router_refresh_seconds,
        )
        self._reload_task = asyncio.create_task(
            self._reload_bots_periodically(),
            name="router_refresher",
        )

        logger.info(
            "WorkerPoller started | subs=%d symbols=%s",
            len(self._router),
            ",".join(self._router.symbols()),
        )

        logger.info("Beginning signal consumption loop...")

        # High-level loop: handler + ACK-on-success handled by the consumer (CG mode)
        await self._consumer.consume_and_handle(self._handle_message, dedupe_ttl_sec=self._dedupe_ttl)

    async def stop(self) -> None:
        """Graceful shutdown for background tasks."""
        logger.info("Stopping WorkerPoller...")
        self._running = False
        if self._reload_task and not self._reload_task.done():
            logger.info("Cancelling router refresher task...")
            self._reload_task.cancel()
            with contextlib.suppress(Exception):
                await self._reload_task
            logger.info("Router refresher task cancelled")
        logger.info("WorkerPoller stopped")

    # ---------------------------------------------------------------------
    # Handler invoked by the consumer
    # ---------------------------------------------------------------------

    async def _handle_message(self, message: StreamMessage) -> None:
        """
        Parse and route a single StreamMessage.
        - On success: return normally (consumer will XACK in CG mode).
        - On error: raise (consumer will NOT ack).
        """
        log_context = self._build_log_context(message)
        logger.info("Received signal | %s", format_log_context(log_context))

        updated_context = await self._process_signal(message, log_context)
        logger.info(
            "Signal processed successfully | sym=%s tf=%s type=%s msg_id=%s prev_side=%s",
            updated_context.get("symbol", "-"),
            updated_context.get("tf", "-"),
            updated_context.get("type", "-"),
            updated_context.get("msg_id", "-"),
            updated_context.get("prev_side", "-"),
        )
        self._metrics.inc_processed(status="success")

    # ---------------------------------------------------------------------
    # Core processing
    # ---------------------------------------------------------------------

    async def _process_signal(
        self,
        message: StreamMessage,
        log_context: Dict[str, str],
    ) -> Dict[str, str]:
        """Parse the payload and invoke the appropriate SignalProcessor handler."""
        payload = message.payload
        msg_id = message.message_id
        msg_type = str(payload.get("type", "")).lower()

        logger.debug("Parsing signal | %s", format_log_context(log_context))

        if msg_type == SignalType.ARM.value:
            try:
                signal = ArmSignal.from_stream(payload)
                log_context = ensure_log_context(log_context, type=signal.type.value)
                logger.info(
                    "Parsed ARM signal | side=%s trigger=%s stop=%s | %s",
                    signal.side.value,
                    signal.trigger,
                    signal.stop,
                    format_log_context(log_context),
                )
            except Exception as exc:
                self._metrics.inc_processed(status="error")
                logger.error("Failed to parse ARM signal | err=%s | %s", exc, format_log_context(log_context))
                raise InvalidSignalException(f"Invalid ARM signal: {exc}") from exc

            await self._sp.process_arm_signal(signal, msg_id, log_context)
            logger.debug("Processed ARM | %s", format_log_context(log_context))
            return log_context

        if msg_type == SignalType.DISARM.value:
            try:
                signal = DisarmSignal.from_stream(payload)
                log_context = ensure_log_context(
                    log_context,
                    type=signal.type.value,
                    prev_side=signal.prev_side.value,
                )
                logger.info(
                    "Parsed DISARM signal | prev_side=%s reason=%s | %s",
                    signal.prev_side.value,
                    signal.reason,
                    format_log_context(log_context),
                )
            except Exception as exc:
                self._metrics.inc_processed(status="error")
                logger.error("Failed to parse DISARM signal | err=%s | %s", exc, format_log_context(log_context))
                raise InvalidSignalException(f"Invalid DISARM signal: {exc}") from exc

            await self._sp.process_disarm_signal(signal, msg_id, log_context)
            logger.debug("Processed DISARM | %s", format_log_context(log_context))
            return log_context

        self._metrics.inc_processed(status="error")
        logger.error("Unknown signal type | type=%s data=%s", msg_type, payload)
        raise InvalidSignalException(f"Unknown signal type '{msg_type}'")

    # ---------------------------------------------------------------------
    # Router management
    # ---------------------------------------------------------------------

    async def _reload_bots(self) -> None:
        """Pull fresh enabled bots and rebuild router mappings if changed."""
        logger.info("Fetching enabled bots from database...")
        bots = await self._bots.get_enabled_bots()
        logger.info("Found %d enabled bot(s)", len(bots))

        if bots:
            for bot in bots:
                logger.info(
                    "  Bot: id=%s user=%s symbol=%s tf=%s side=%s leverage=%dx env=%s",
                    bot.id, bot.user_id, bot.symbol, bot.timeframe,
                    bot.side_whitelist.value, bot.leverage, bot.env,
                )
        else:
            logger.warning("No enabled bots found in database")

        fingerprint = hash(tuple(sorted((str(b.id), b.symbol.upper(), b.timeframe) for b in bots)))
        if fingerprint != self._router_fp:
            logger.info("Rebuilding router subscriptions...")
            self._router.reload_subscriptions(bots)
            self._router_fp = fingerprint
            symbols_str = ",".join(sorted({b.symbol.upper() for b in bots})) if bots else "-"
            logger.info("Router rebuilt | subs=%d symbols=%s", len(self._router), symbols_str)
        else:
            logger.debug("Router fingerprint unchanged | subs=%d", len(self._router))

    async def _reload_bots_periodically(self) -> None:
        """Periodically refresh router subscriptions to reflect DB changes."""
        logger.info("Router refresher started (interval=%ds)", self._router_refresh_seconds)
        try:
            while self._running:
                await asyncio.sleep(self._router_refresh_seconds)
                try:
                    logger.debug("Performing scheduled router refresh...")
                    await self._reload_bots()
                except Exception as exc:
                    logger.error("Router refresh error | err=%s", exc, exc_info=True)
        except asyncio.CancelledError:
            logger.info("Router refresher cancelled")

    # ---------------------------------------------------------------------
    # Logging helpers
    # ---------------------------------------------------------------------

    def _build_log_context(self, message: StreamMessage) -> Dict[str, str]:
        payload_type = str(message.payload.get("type", "")).lower()
        prev_side = str(message.payload.get("prev_side", "-") or "-")
        return {
            "symbol": message.symbol,
            "tf": message.timeframe,
            "type": payload_type,
            "msg_id": message.message_id,
            "prev_side": prev_side,
        }
