from __future__ import annotations
import asyncio
import logging
import random
import time
from decimal import Decimal
from typing import Optional

from redis.asyncio import Redis

from ..config import Config
from ..models import IndicatorState, Candle
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
        self.publisher = StreamPublisher(
            r, sym, self.tf, cfg.stream_maxlen_ind, cfg.stream_maxlen_signal
        )
        self.calc = IndicatorCandle(cfg.ma20_window, cfg.ma200_window)
        self.tracker = IndicatorTracker()
        self.regime = RegimeDetector()
        self.signals = SignalGenerator(tick_size=Decimal(str(cfg.tick_size)))
        self._last_id: Optional[str] = None
        self._candle_count = 0
        self._signal_count = 0

        # Close price tracking
        self._last_red: Optional[Candle] = None
        self._last_green: Optional[Candle] = None
        self._last_signal: Optional[object] = None

        # Catchup mode tracking
        self._catchup_mode = True
        self._catchup_threshold_ms = cfg.catchup_threshold_ms

        # Heartbeat tracking for monitoring
        self._last_heartbeat = 0
        self._heartbeat_interval_s = 60  # Log heartbeat every 60 seconds in live mode

        logger.info(
            "SymbolProcessor initialized | sym=%s tf=%s ma20=%d ma200=%d tick_size=%s",
            sym,
            self.tf,
            cfg.ma20_window,
            cfg.ma200_window,
            cfg.tick_size,
        )

    def _update_color_trackers(self, c: Candle) -> None:
        """Track last red/green CLOSED candles"""
        # Doji leaves state unchanged
        if c.close < c.open:
            self._last_red = c
        elif c.close > c.open:
            self._last_green = c

    def _is_caught_up(self, candle_ts: int) -> bool:
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
                logger.info(
                    "Processor running | sym=%s tf=%s resume_from=%s catchup_mode=%s",
                    self.sym,
                    self.tf,
                    self._last_id or "start",
                    self._catchup_mode,
                )

                async for c in self.consumer.candles():
                    self._candle_count += 1

                    await self._handle_catchup_transition(c.ts)

                    # Track candle color + any other indicator tracking
                    self._update_color_trackers(c)
                    self.tracker.on_candle(c)

                    # Calculate MAs (we'll pick indicator candle from last red/green below)
                    (
                        ma20,
                        ma200,
                        _ind_ts_ignored,
                        _ind_high_ignored,
                        _ind_low_ignored,
                    ) = self.calc.on_candle(c)

                    # Choose closes for detector (fallback to current close)
                    close_for_long = (
                        self._last_red.close if self._last_red else c.close
                    )  # last RED close
                    close_for_short = (
                        self._last_green.close if self._last_green else c.close
                    )  # last GREEN close

                    # Detect regime
                    regime = self.regime.decide(
                        ma20=ma20,
                        ma200=ma200,
                        close_for_long=close_for_long,
                        close_for_short=close_for_short,
                    )

                    # Choose indicator candle (aligns ind_* with regime context)
                    if ma20 is not None and ma200 is not None and ma20 > ma200:
                        ind_candle = self._last_red or c
                    elif ma20 is not None and ma200 is not None and ma20 < ma200:
                        ind_candle = self._last_green or c
                    else:
                        ind_candle = c

                    ind_ts = ind_candle.ts
                    ind_high = ind_candle.high
                    ind_low = ind_candle.low

                    # ---- DEBUG snapshot (unchanged except we keep it near signals)
                    if logger.isEnabledFor(logging.DEBUG):
                        last_red_ts = self._last_red.ts if self._last_red else None
                        last_green_ts = (
                            self._last_green.ts if self._last_green else None
                        )
                        logger.debug(
                            "Regime eval | sym=%s ts=%d ma20=%s ma200=%s close_for_long=%s close_for_short=%s "
                            "regime=%s ind_ts=%s ind_high=%s ind_low=%s last_red_ts=%s last_green_ts=%s",
                            self.sym,
                            c.ts,
                            ma20,
                            ma200,
                            close_for_long,
                            close_for_short,
                            regime,
                            ind_ts,
                            ind_high,
                            ind_low,
                            last_red_ts,
                            last_green_ts,
                        )

                    # Publish indicator state
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
                    ind_id = await self.publisher.publish_indicator(
                        ind_state.to_stream_map()
                    )
                    self._last_id = ind_id

                    # NEW: generate 0..2 signals (supports DISARM+ARM on direct flips)
                    sigs = self.signals.maybe_signals(
                        sym=c.sym,
                        tf=c.tf,
                        now_ts=c.ts,
                        regime=regime,
                        ind_ts=ind_ts,
                        ind_high=ind_high,
                        ind_low=ind_low,
                    )

                    if sigs:
                        if self._catchup_mode:
                            # NEW: in catchup we keep ONLY the last signal (ARM if flip), workers weren’t acting yet.
                            self._last_signal = sigs[-1]
                            for s in sigs:
                                logger.debug(
                                    "Buffered (catchup) signal candidate | sym=%s type=%s side=%s ts=%d",
                                    self.sym,
                                    s.type,
                                    getattr(s, "side", None)
                                    or getattr(s, "prev_side", None),
                                    c.ts,
                                )
                        else:
                            # NEW: publish ALL signals in order (e.g., DISARM then ARM on flips)
                            for s in sigs:
                                self._signal_count += 1
                                await self.publisher.publish_signal(s.to_stream_map())
                                logger.info(
                                    "Signal generated and published | sym=%s type=%s ts=%d total_signals=%d",
                                    self.sym,
                                    s.type,
                                    c.ts,
                                    self._signal_count,
                                )

                    # Milestone logs
                    if self._candle_count % 100 == 0:
                        logger.info(
                            "Processing milestone | sym=%s candles=%d signals=%d ma20_ready=%s ma200_ready=%s mode=%s",
                            self.sym,
                            self._candle_count,
                            self._signal_count,
                            ma20 is not None,
                            ma200 is not None,
                            "catchup" if self._catchup_mode else "live",
                        )

            except asyncio.CancelledError:
                logger.info(
                    "Processor cancelled | sym=%s candles=%d signals=%d",
                    self.sym,
                    self._candle_count,
                    self._signal_count,
                )
                raise
            except Exception as e:
                retry_count += 1
                sleep_time = backoff + random.random()
                logger.error(
                    "Processor crashed | sym=%s error=%s retry=%d backoff=%.2fs candles=%d",
                    self.sym,
                    e,
                    retry_count,
                    sleep_time,
                    self._candle_count,
                    exc_info=True,
                )
                await asyncio.sleep(sleep_time)
                backoff = min(backoff * 2, self.cfg.backoff_max_s)
                logger.info(
                    "Processor restarting | sym=%s new_backoff=%.2fs", self.sym, backoff
                )

    async def _handle_catchup_transition(self, candle_ts: int) -> None:
        """Flush buffered signals when historical replay catches up."""

        if not self._catchup_mode or not self._is_caught_up(candle_ts):
            return

        logger.info(
            "Catchup complete | sym=%s candles_processed=%d transitioning_to_live_mode",
            self.sym,
            self._candle_count,
        )

        if self._last_signal is not None:
            await self.publisher.publish_signal(self._last_signal.to_stream_map())
            self._signal_count += 1
            logger.info(
                "Emitted final catchup signal | sym=%s type=%s count=%d",
                self.sym,
                getattr(self._last_signal, "type", "unknown"),
                self._signal_count,
            )
            self._last_signal = None

        self._catchup_mode = False
