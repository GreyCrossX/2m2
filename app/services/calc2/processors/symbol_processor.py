from __future__ import annotations
import asyncio
import logging
import random
from decimal import Decimal
from typing import Optional


from redis.asyncio import Redis


from ..config import Config
from ..models import IndicatorState
from ..indicators.calculator import IndicatorCandle
from ..indicators.tracker import IndicatorTracker
from ..regime.detector import RegimeDetector
from ..signals.generator import SignalGenerator
from ..streams.consumer import StreamConsumer
from ..streams.publisher import StreamPublisher

log = logging.getLogger(__name__)

class SymbolProcessor:
    def __init__(self, cfg: Config, r: Redis, sym: str) -> None:
        self.cfg = cfg
        self.r = r
        self.sym = sym
        self.tf = cfg.timeframe
        self.consumer = StreamConsumer(r, sym, self.tf, cfg.stream_block_ms)
        self.publisher = StreamPublisher(r, sym, self.tf, cfg.stream_maxlen_ind, cfg.stream_maxlen_signal)
        self.calc = IndicatorCandle(cfg.ma20_window, cfg.ma200_window)
        self.tracker = IndicatorTracker()
        self.regime = RegimeDetector()
        self.signals = SignalGenerator(tick_size=Decimal(str(cfg.tick_size)))
        self._last_id: Optional[str] = None

    async def run(self) -> None:
        backoff = self.cfg.backoff_min_s
        while True:
            try:
                await self.consumer.resume_from(self._last_id)
                async for c in self.consumer.candles():
                    self.tracker.on_candle(c)
                    ma20, ma200, ind_ts, ind_high, ind_low = self.calc.on_candle(c)
                    regime = self.regime.decide(ma20, ma200, c.close)

                    ind_state = IndicatorState (
                        v="1",
                        sym=c.sym,
                        tf=c.tf,
                        ts=c.ts,
                        open=c.open,
                        high=c.high,
                        low=c.low,
                        close=c.close,
                        color=c.color,
                        ma20=ma20,
                        ma200=ma200,
                        regime=regime,
                        ind_ts=ind_ts,
                        ind_high=ind_high,
                        ind_low=ind_low,
                    )

                    ind_id = await self.publisher.publish_indicator(ind_state.to_stream_map())
                    self._last_id = ind_id

                    sig = self.signals.maybe_signal(
                        sym=c.sym,
                        tf=c.tf,
                        now_ts=c.ts,
                        regime=regime,
                        ind_ts=ind_ts,
                        ind_high=ind_high,
                        ind_low=ind_low,
                    )
                    if sig is not None:
                        await self.publisher.publish_signal(sig.to_stream_map())

                backoff = self.cfg.backoff_min_s # reset on healthy loop

            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.exception("%s processor crashed: %s", self.sym, e)
                await asyncio.sleep(backoff + random.random())
                backoff = min(backoff * 2, self.cfg.backoff_max_s)