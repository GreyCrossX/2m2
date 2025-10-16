from __future__ import annotations
import asyncio
import logging
import random
import time
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

logger = logging.getLogger(__name__)

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
        self._candle_count = 0
        self._signal_count = 0
        
        # Catchup mode tracking
        self._catchup_mode = True
        self._last_signal: Optional[object] = None
        self._catchup_threshold_ms = cfg.catchup_threshold_ms
        
        logger.info("SymbolProcessor initialized | sym=%s tf=%s ma20=%d ma200=%d tick_size=%s", 
                   sym, self.tf, cfg.ma20_window, cfg.ma200_window, cfg.tick_size)

    def _is_caught_up(self, candle_ts: int) -> bool:
        """Check if we're processing near-realtime data."""
        now_ms = int(time.time() * 1000)
        age_ms = now_ms - candle_ts
        return age_ms < self._catchup_threshold_ms

    async def run(self) -> None:
        backoff = self.cfg.backoff_min_s
        retry_count = 0
        
        logger.info("Starting processor loop | sym=%s tf=%s", self.sym, self.tf)
        
        while True:
            try:
                await self.consumer.resume_from(self._last_id)
                logger.info("Processor running | sym=%s tf=%s resume_from=%s catchup_mode=%s", 
                           self.sym, self.tf, self._last_id or "start", self._catchup_mode)
                
                async for c in self.consumer.candles():
                    self._candle_count += 1
                    
                    # Check if we've caught up to live data
                    if self._catchup_mode and self._is_caught_up(c.ts):
                        logger.info("Catchup complete | sym=%s candles_processed=%d transitioning_to_live_mode", 
                                   self.sym, self._candle_count)
                        
                        # Emit the last signal from catchup if we have one
                        if self._last_signal is not None:
                            await self.publisher.publish_signal(self._last_signal.to_stream_map())
                            self._signal_count += 1
                            logger.info("Emitted final catchup signal | sym=%s type=%s count=%d", 
                                       self.sym, getattr(self._last_signal, 'type', 'unknown'), 
                                       self._signal_count)
                            self._last_signal = None
                        
                        self._catchup_mode = False
                    
                    if self._catchup_mode:
                        logger.debug("Processing historical candle | sym=%s ts=%d count=%d", 
                                   self.sym, c.ts, self._candle_count)
                    else:
                        logger.debug("Processing live candle | sym=%s ts=%d close=%s count=%d", 
                                   self.sym, c.ts, c.close, self._candle_count)
                    
                    # Track candle color
                    self.tracker.on_candle(c)
                    
                    # Calculate indicators
                    ma20, ma200, ind_ts, ind_high, ind_low = self.calc.on_candle(c)
                    
                    # Detect regime
                    regime = self.regime.decide(ma20, ma200, c.close)
                    
                    # Create indicator state
                    ind_state = IndicatorState(
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
                    
                    # Always publish indicator state (needed for regime tracking)
                    ind_id = await self.publisher.publish_indicator(ind_state.to_stream_map())
                    self._last_id = ind_id
                    
                    # Generate signal
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
                        if self._catchup_mode:
                            # In catchup mode: just store the latest signal, don't publish
                            self._last_signal = sig
                            logger.debug("Buffered signal in catchup | sym=%s type=%s side=%s ts=%d", 
                                       self.sym, sig.type, 
                                       getattr(sig, 'side', None) or getattr(sig, 'prev_side', None),
                                       c.ts)
                        else:
                            # In live mode: publish immediately
                            self._signal_count += 1
                            await self.publisher.publish_signal(sig.to_stream_map())
                            logger.info("Signal generated and published | sym=%s type=%s ts=%d total_signals=%d", 
                                       self.sym, sig.type, c.ts, self._signal_count)
                    
                    # Log milestone
                    if self._candle_count % 100 == 0:
                        logger.info("Processing milestone | sym=%s candles=%d signals=%d ma20_ready=%s ma200_ready=%s mode=%s", 
                                   self.sym, self._candle_count, self._signal_count, 
                                   ma20 is not None, ma200 is not None,
                                   "catchup" if self._catchup_mode else "live")
                
                # Reset backoff on healthy loop completion
                backoff = self.cfg.backoff_min_s
                if retry_count > 0:
                    logger.info("Processor recovered | sym=%s retry_count=%d", self.sym, retry_count)
                    retry_count = 0

            except asyncio.CancelledError:
                logger.info("Processor cancelled | sym=%s candles=%d signals=%d", 
                           self.sym, self._candle_count, self._signal_count)
                raise
            except Exception as e:
                retry_count += 1
                sleep_time = backoff + random.random()
                logger.error("Processor crashed | sym=%s error=%s retry=%d backoff=%.2fs candles=%d", 
                           self.sym, e, retry_count, sleep_time, self._candle_count, exc_info=True)
                await asyncio.sleep(sleep_time)
                backoff = min(backoff * 2, self.cfg.backoff_max_s)
                logger.info("Processor restarting | sym=%s new_backoff=%.2fs", self.sym, backoff)