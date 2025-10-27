from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Dict, Optional

from ..application.signal_processor import SignalProcessor
from ..domain.enums import SignalType
from ..domain.exceptions import InvalidSignalException
from ..domain.models import ArmSignal, DisarmSignal, BotConfig
from ..infrastructure.postgres.repositories import BotRepository
from ..infrastructure.redis.stream_consumer import SignalStreamConsumer
from .router import SymbolRouter


logger = logging.getLogger(__name__)


class WorkerPoller:
    """
    Main loop:
      - Load enabled bots and build router
      - Start consuming Calc signals from Redis
      - Parse ARM/DISARM and hand off to SignalProcessor
      - Periodically reload bots
    """

    def __init__(
        self,
        config,  # Config object (expects: timeframe, router_refresh_seconds (opt))
        stream_consumer: SignalStreamConsumer,
        signal_processor: SignalProcessor,
        router: SymbolRouter,
        bot_repository: BotRepository,
        *,
        router_refresh_seconds: Optional[int] = None,
    ):
        self._cfg = config
        self._consumer = stream_consumer
        self._sp = signal_processor
        self._router = router
        self._bots = bot_repository
        self._running = False
        self._reload_task: Optional[asyncio.Task] = None
        self._router_refresh_seconds = router_refresh_seconds or getattr(config, "router_refresh_seconds", 60)

    async def start(self) -> None:
        """
        1) Load enabled bots
        2) Build router subscriptions
        3) Start background router refresher
        4) Start consuming signals
        """
        self._running = True
        logger.info("Loading enabled bots from database...")
        await self._reload_bots()

        # background refresher
        logger.info("Starting background router refresher (interval=%ds)...", self._router_refresh_seconds)
        self._reload_task = asyncio.create_task(self._reload_bots_periodically(), name="router_refresher")

        logger.info("WorkerPoller started | bots=%d symbols=%s",
                    len(self._router), ",".join(self._router.symbols()))
        
        logger.info("Beginning signal consumption loop...")
        signal_count = 0
        
        # main consume loop
        async for sym, tf, payload, msg_id in self._consumer.consume():
            if not self._running:
                logger.info("Poller stopped flag set, breaking consume loop")
                break
            
            signal_count += 1
            logger.info("Received signal #%d | sym=%s tf=%s msg_id=%s type=%s", 
                    signal_count, sym, tf, msg_id, payload.get('type', 'unknown'))
            
            try:
                await self._process_signal(sym, tf, payload, msg_id)
                logger.info("Signal #%d processed successfully | sym=%s msg_id=%s", 
                        signal_count, sym, msg_id)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("Signal processing error | sym=%s tf=%s msg_id=%s err=%s",
                             sym, tf, msg_id, e, exc_info=True)

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

    async def _process_signal(
        self,
        symbol: str,
        timeframe: str,
        signal_data: Dict,
        message_id: str,
    ) -> None:
        """
        Parse & delegate to SignalProcessor.
        """
        t = str(signal_data.get("type", "")).lower()
        logger.debug("Parsing signal | type=%s sym=%s tf=%s msg_id=%s", t, symbol, timeframe, message_id)
        
        if t == SignalType.ARM.value:
            try:
                signal = ArmSignal.from_stream(signal_data)
                logger.info("Parsed ARM signal | sym=%s side=%s trigger=%s stop=%s", 
                          signal.symbol, signal.side.value, signal.trigger, signal.stop)
            except Exception as e:
                logger.error("Failed to parse ARM signal | data=%s err=%s", signal_data, e)
                raise InvalidSignalException(f"Invalid ARM signal: {e}") from e
            
            await self._sp.process_arm_signal(signal, message_id)
            logger.debug("Processed ARM | sym=%s tf=%s msg=%s", symbol, timeframe, message_id)

        elif t == SignalType.DISARM.value:
            try:
                signal = DisarmSignal.from_stream(signal_data)
                logger.info("Parsed DISARM signal | sym=%s prev_side=%s reason=%s", 
                          signal.symbol, signal.prev_side.value, signal.reason)
            except Exception as e:
                logger.error("Failed to parse DISARM signal | data=%s err=%s", signal_data, e)
                raise InvalidSignalException(f"Invalid DISARM signal: {e}") from e
            
            await self._sp.process_disarm_signal(signal, message_id)
            logger.debug("Processed DISARM | sym=%s tf=%s msg=%s", symbol, timeframe, message_id)

        else:
            logger.error("Unknown signal type | type=%s data=%s", t, signal_data)
            raise InvalidSignalException(f"Unknown signal type '{t}'")

    async def _reload_bots(self) -> None:
        """
        Pull fresh enabled bots and rebuild router mappings.
        """
        logger.info("Fetching enabled bots from database...")
        bots = await self._bots.get_enabled_bots()
        logger.info("Found %d enabled bot(s)", len(bots))
        
        if bots:
            for bot in bots:
                logger.info("  Bot: id=%s user=%s symbol=%s tf=%s side=%s leverage=%dx env=%s",
                        bot.id, bot.user_id, bot.symbol, bot.timeframe, 
                        bot.side_whitelist.value, bot.leverage, bot.env)
        else:
            logger.warning("No enabled bots found in database")
        
        # BotRepository returns Domain BotConfig (per our infra impl)
        logger.info("Rebuilding router subscriptions...")
        self._router.reload_subscriptions(bots)
        logger.info("Router rebuilt | subs=%d symbols=%s",
                    len(self._router), ",".join(self._router.symbols()))

    async def _reload_bots_periodically(self) -> None:
        """
        Periodically refresh router subscriptions to reflect DB changes.
        """
        logger.info("Router refresher started (interval=%ds)", self._router_refresh_seconds)
        try:
            while self._running:
                await asyncio.sleep(self._router_refresh_seconds)
                try:
                    logger.debug("Performing scheduled router refresh...")
                    await self._reload_bots()
                except Exception as e:
                    logger.error("Router refresh error | err=%s", e, exc_info=True)
        except asyncio.CancelledError:
            logger.info("Router refresher cancelled")