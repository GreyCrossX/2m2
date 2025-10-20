from __future__ import annotations

import asyncio
import logging
from typing import Dict, Optional
import contextlib

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
        await self._reload_bots()

        # background refresher
        self._reload_task = asyncio.create_task(self._reload_bots_periodically(), name="router_refresher")

        logger.info("WorkerPoller started | bots=%d symbols=%s",
                    len(self._router), ",".join(self._router.symbols()))

        # main consume loop
        async for sym, tf, payload, msg_id in self._consumer.consume():
            if not self._running:
                break
            try:
                await self._process_signal(sym, tf, payload, msg_id)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("Signal processing error | sym=%s tf=%s msg_id=%s err=%s",
                             sym, tf, msg_id, e, exc_info=True)

    async def stop(self) -> None:
        """Graceful shutdown for background tasks."""
        self._running = False
        if self._reload_task and not self._reload_task.done():
            self._reload_task.cancel()
            with contextlib.suppress(Exception):
                await self._reload_task

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
        if t == SignalType.ARM.value:
            try:
                signal = ArmSignal.from_stream(signal_data)
            except Exception as e:
                raise InvalidSignalException(f"Invalid ARM signal: {e}") from e
            await self._sp.process_arm_signal(signal, message_id)
            logger.debug("Processed ARM | sym=%s tf=%s msg=%s", symbol, timeframe, message_id)

        elif t == SignalType.DISARM.value:
            try:
                signal = DisarmSignal.from_stream(signal_data)
            except Exception as e:
                raise InvalidSignalException(f"Invalid DISARM signal: {e}") from e
            await self._sp.process_disarm_signal(signal, message_id)
            logger.debug("Processed DISARM | sym=%s tf=%s msg=%s", symbol, timeframe, message_id)

        else:
            raise InvalidSignalException(f"Unknown signal type '{t}'")

    async def _reload_bots(self) -> None:
        """
        Pull fresh enabled bots and rebuild router mappings.
        """
        bots = await self._bots.get_enabled_bots()
        # BotRepository returns Domain BotConfig (per our infra impl)
        self._router.reload_subscriptions(bots)
        logger.info("Router rebuilt | subs=%d symbols=%s",
                    len(self._router), ",".join(self._router.symbols()))

    async def _reload_bots_periodically(self) -> None:
        """
        Periodically refresh router subscriptions to reflect DB changes.
        """
        try:
            while self._running:
                await asyncio.sleep(self._router_refresh_seconds)
                try:
                    await self._reload_bots()
                except Exception as e:
                    logger.error("Router refresh error | err=%s", e, exc_info=True)
        except asyncio.CancelledError:
            pass
