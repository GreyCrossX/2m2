"""
Trading Signal Calculator Service

Processes market data streams, calculates indicators, and generates trading signals.
Implements regime-based trading strategy with SMA crossovers.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Protocol

from app.config import settings
from services.ingestor.keys import st_market, tag as tag_tf
from services.ingestor.redis_io import r as r_ingestor

from .indicators import SMA
from .strategy import Candle, IndicatorState, choose_regime


# ═══════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class CalcConfig:
    """Immutable configuration for the calculator service."""
    
    price_tick: float
    stream_maxlen_ind: int
    stream_maxlen_signal: int
    stream_retention_ms_ind: int
    stream_retention_ms_signal: int
    warmup_log_interval_sec: int
    stream_ready_timeout_sec: int
    
    @classmethod
    def from_env(cls) -> CalcConfig:
        """Load configuration from environment variables."""
        import os
        return cls(
            price_tick=float(os.getenv("PRICE_TICK_DEFAULT", "0.01")),
            stream_maxlen_ind=int(os.getenv("STREAM_MAXLEN_IND", "5000")),
            stream_maxlen_signal=int(os.getenv("STREAM_MAXLEN_SIGNAL", "2000")),
            stream_retention_ms_ind=int(os.getenv("STREAM_RETENTION_MS_IND", "0")),
            stream_retention_ms_signal=int(os.getenv("STREAM_RETENTION_MS_SIGNAL", "0")),
            warmup_log_interval_sec=int(os.getenv("CALC_WARMUP_LOG_EVERY_SEC", "2")),
            stream_ready_timeout_sec=int(os.getenv("CALC_STREAM_READY_TIMEOUT_SEC", "10")),
        )


class Regime(str, Enum):
    """Trading regime states."""
    LONG = "long"
    SHORT = "short"
    NEUTRAL = "neutral"


class SignalType(str, Enum):
    """Signal message types."""
    ARM = "arm"
    DISARM = "disarm"


# ═══════════════════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════════════════

def setup_logging() -> logging.Logger:
    """Configure and return the calculator logger."""
    import os
    logger = logging.getLogger("calc")
    logging.basicConfig(
        level=getattr(logging, os.getenv("CALC_LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    return logger


LOG = setup_logging()


# ═══════════════════════════════════════════════════════════════
#  DATA TRANSFER OBJECTS
# ═══════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class StreamKeys:
    """Redis stream and hash keys for a symbol."""
    market_in: str
    indicator_out: str
    signal_out: str
    snapshot: str
    
    @classmethod
    def for_symbol(cls, symbol: str, timeframe: str = "2m") -> StreamKeys:
        """Generate all Redis keys for a given symbol and timeframe."""
        sym_upper = symbol.upper()
        tag = tag_tf(sym_upper, timeframe)
        return cls(
            market_in=st_market(sym_upper, timeframe),
            indicator_out=f"stream:ind|{{{tag}}}",
            signal_out=f"stream:signal|{{{tag}}}",
            snapshot=f"snap:ind|{{{tag}}}",
        )


@dataclass(frozen=True)
class MarketData:
    """Validated market candle data."""
    ts: int
    open: float
    high: float
    low: float
    close: float
    color: str
    
    @classmethod
    def from_fields(cls, fields: dict[str, str]) -> MarketData:
        """Parse and validate Redis stream fields."""
        return cls(
            ts=int(fields["ts"]),
            open=float(fields["open"]),
            high=float(fields["high"]),
            low=float(fields["low"]),
            close=float(fields["close"]),
            color=fields.get("color", "green"),
        )


@dataclass(frozen=True)
class TradeLevels:
    """Trigger and stop loss levels for a trade."""
    trigger: float
    stop: float


@dataclass
class IndicatorSnapshot:
    """Complete indicator state for a given timestamp."""
    symbol: str
    timeframe: str
    ts: int
    market: MarketData
    ma20: Optional[float]
    ma200: Optional[float]
    regime: str
    ind_ts: Optional[int]
    ind_high: Optional[float]
    ind_low: Optional[float]
    
    def to_redis_dict(self) -> dict[str, str]:
        """Convert to Redis hash format."""
        return {
            "v": "1",
            "sym": self.symbol,
            "tf": self.timeframe,
            "ts": str(self.ts),
            "close": str(self.market.close),
            "open": str(self.market.open),
            "high": str(self.market.high),
            "low": str(self.market.low),
            "color": self.market.color,
            "ma20": "" if self.ma20 is None else str(self.ma20),
            "ma200": "" if self.ma200 is None else str(self.ma200),
            "regime": self.regime,
            "ind_ts": "" if self.ind_ts is None else str(self.ind_ts),
            "ind_high": "" if self.ind_high is None else str(self.ind_high),
            "ind_low": "" if self.ind_low is None else str(self.ind_low),
        }


# ═══════════════════════════════════════════════════════════════
#  REPOSITORY LAYER
# ═══════════════════════════════════════════════════════════════

class RedisRepository(Protocol):
    """Interface for Redis operations."""
    
    def get_last_timestamp(self, stream_key: str) -> Optional[int]: ...
    def write_indicator(self, keys: StreamKeys, snapshot: IndicatorSnapshot, maxlen: int) -> None: ...
    def write_signal(self, stream_key: str, payload: dict[str, str], msg_id: str, maxlen: int) -> None: ...
    def trim_streams(self, keys: StreamKeys, retention_ms_ind: int, retention_ms_sig: int) -> None: ...


class RedisRepositoryImpl:
    """Redis operations implementation."""
    
    def __init__(self, redis_client):
        self._redis = redis_client
    
    def get_last_timestamp(self, stream_key: str) -> Optional[int]:
        """Get timestamp of the last message in a stream."""
        try:
            last = self._redis.xrevrange(stream_key, count=1)
            if last:
                last_id, _ = last[0]
                return int(last_id.split("-")[0])
        except Exception as e:
            LOG.warning("Failed to get last timestamp from %s: %s", stream_key, e)
        return None
    
    def write_indicator(
        self,
        keys: StreamKeys,
        snapshot: IndicatorSnapshot,
        maxlen: int,
    ) -> None:
        """Write indicator data to snapshot hash and stream."""
        data = snapshot.to_redis_dict()
        pipe = self._redis.pipeline()
        pipe.hset(keys.snapshot, mapping=data)
        pipe.xadd(
            keys.indicator_out,
            data,
            id=f"{snapshot.ts}-0",
            maxlen=maxlen,
            approximate=True,
        )
        pipe.execute()
    
    def write_signal(
        self,
        stream_key: str,
        payload: dict[str, str],
        msg_id: str,
        maxlen: int,
    ) -> None:
        """Write a signal message to the signal stream."""
        self._redis.xadd(
            stream_key,
            payload,
            id=msg_id,
            maxlen=maxlen,
            approximate=True,
        )
    
    def trim_streams(
        self,
        keys: StreamKeys,
        retention_ms_ind: int,
        retention_ms_sig: int,
    ) -> None:
        """Trim streams based on time-based retention."""
        now_ms = int(time.time() * 1000)
        if retention_ms_ind > 0:
            self._redis.xtrim(
                keys.indicator_out,
                minid=now_ms - retention_ms_ind,
                approximate=True,
            )
        if retention_ms_sig > 0:
            self._redis.xtrim(
                keys.signal_out,
                minid=now_ms - retention_ms_sig,
                approximate=True,
            )


# ═══════════════════════════════════════════════════════════════
#  BUSINESS LOGIC
# ═══════════════════════════════════════════════════════════════

class TradeLevelCalculator:
    """Calculate trigger and stop levels based on indicator candle."""
    
    def __init__(self, price_tick: float):
        self._tick = price_tick
    
    def calculate_long(self, ind_high: float, ind_low: float) -> TradeLevels:
        """Calculate levels for long entry."""
        return TradeLevels(
            trigger=ind_high + self._tick,
            stop=ind_low - self._tick,
        )
    
    def calculate_short(self, ind_high: float, ind_low: float) -> TradeLevels:
        """Calculate levels for short entry."""
        return TradeLevels(
            trigger=ind_low - self._tick,
            stop=ind_high + self._tick,
        )


class SignalGenerator:
    """Generate trading signals based on regime changes."""
    
    def __init__(self, symbol: str, timeframe: str, level_calc: TradeLevelCalculator):
        self._symbol = symbol
        self._timeframe = timeframe
        self._level_calc = level_calc
        self._last_regime = Regime.NEUTRAL
        self._armed_side: Optional[str] = None
    
    def process(
        self,
        ts: int,
        regime: str,
        indicator_candle: Optional[Candle],
    ) -> list[dict[str, str]]:
        """
        Process regime change and return signal payloads to emit.
        
        Returns a list of signal dictionaries in the order they should be emitted.
        """
        signals = []
        current = Regime(regime) if regime in {r.value for r in Regime} else Regime.NEUTRAL
        
        # Check for regime flip (long <-> short)
        is_flip = (
            self._last_regime in (Regime.LONG, Regime.SHORT)
            and current in (Regime.LONG, Regime.SHORT)
            and current != self._last_regime
        )
        
        # CASE 1: Neutral → Armed (Long/Short)
        if self._last_regime == Regime.NEUTRAL and current in (Regime.LONG, Regime.SHORT):
            arm_signal = self._create_arm_signal(ts, current, indicator_candle)
            if arm_signal:
                signals.append(arm_signal)
                self._armed_side = current.value
        
        # CASE 2: Armed → Neutral
        elif self._last_regime in (Regime.LONG, Regime.SHORT) and current == Regime.NEUTRAL:
            signals.append(self._create_disarm_signal(
                ts,
                self._last_regime.value,
                f"regime:{self._last_regime.value}->neutral",
            ))
            self._armed_side = None
        
        # CASE 3: Direct Flip (Long → Short or Short → Long)
        elif is_flip:
            # First disarm old side
            signals.append(self._create_disarm_signal(
                ts,
                self._last_regime.value,
                f"regime:{self._last_regime.value}->{current.value} (direct-flip)",
            ))
            self._armed_side = None
            
            # Then arm new side
            arm_signal = self._create_arm_signal(ts, current, indicator_candle)
            if arm_signal:
                signals.append(arm_signal)
                self._armed_side = current.value
        
        self._last_regime = current
        return signals
    
    def _create_arm_signal(
        self,
        ts: int,
        regime: Regime,
        indicator_candle: Optional[Candle],
    ) -> Optional[dict[str, str]]:
        """Create ARM signal payload."""
        if indicator_candle is None:
            LOG.info(
                "[%s SIGNAL] SKIP ARM (%s): no indicator candle yet (ts=%s)",
                self._symbol, regime.value, ts
            )
            return None
        
        levels = (
            self._level_calc.calculate_long(indicator_candle.high, indicator_candle.low)
            if regime == Regime.LONG
            else self._level_calc.calculate_short(indicator_candle.high, indicator_candle.low)
        )
        
        return {
            "v": "1",
            "type": SignalType.ARM.value,
            "side": regime.value,
            "sym": self._symbol,
            "tf": self._timeframe,
            "ts": str(ts),
            "ind_ts": str(indicator_candle.ts),
            "ind_high": str(indicator_candle.high),
            "ind_low": str(indicator_candle.low),
            "trigger": str(levels.trigger),
            "stop": str(levels.stop),
        }
    
    def _create_disarm_signal(self, ts: int, prev_side: str, reason: str) -> dict[str, str]:
        """Create DISARM signal payload."""
        return {
            "v": "1",
            "type": SignalType.DISARM.value,
            "prev_side": prev_side,
            "sym": self._symbol,
            "tf": self._timeframe,
            "ts": str(ts),
            "reason": reason,
        }


# ═══════════════════════════════════════════════════════════════
#  STREAM PROCESSOR
# ═══════════════════════════════════════════════════════════════

class StreamProcessor:
    """Main processor for market data streams."""
    
    def __init__(
        self,
        symbol: str,
        config: CalcConfig,
        repository: RedisRepository,
    ):
        self._symbol = symbol.upper().strip()
        self._config = config
        self._repo = repository
        self._keys = StreamKeys.for_symbol(self._symbol)
        
        # Indicators
        self._sma20 = SMA(20)
        self._sma200 = SMA(200)
        self._indicator_state = IndicatorState(price_tick=config.price_tick)
        
        # Signal generation
        self._signal_gen = SignalGenerator(
            self._symbol,
            "2m",
            TradeLevelCalculator(config.price_tick),
        )
        
        # State tracking
        self._last_out_ts: Optional[int] = None
        self._last_sig_ts: Optional[int] = None
        self._current_sig_ts: Optional[int] = None
        self._current_sig_seq: int = 0
        self._last_warmup_log: float = 0.0
    
    async def run(self) -> None:
        """Main processing loop."""
        LOG.info("[calc %s] Starting stream processor", self._symbol)
        
        # Wait for input stream to be ready
        await self._wait_for_stream()
        
        # Resume from last processed timestamps
        self._initialize_resume_state()
        
        # Start background trim task
        asyncio.create_task(self._trim_task())
        
        # Process stream
        await self._consume_stream()
    
    async def _wait_for_stream(self) -> None:
        """Wait for the input stream to have data."""
        deadline = time.time() + self._config.stream_ready_timeout_sec
        last_log = 0.0
        
        while True:
            try:
                xlen = r_ingestor.xlen(self._keys.market_in)
                if xlen > 0:
                    LOG.info("[calc %s] Input stream ready (len=%d)", self._symbol, xlen)
                    return
                
                now = time.time()
                if now - last_log >= 2.0:
                    LOG.info("[calc %s] Waiting for input stream data…", self._symbol)
                    last_log = now
                
                if now >= deadline:
                    LOG.warning(
                        "[calc %s] Input stream still empty after timeout",
                        self._symbol
                    )
                    return
            except Exception as e:
                LOG.warning("[calc %s] Stream probe error: %s", self._symbol, e)
                if time.time() >= deadline:
                    return
            
            await asyncio.sleep(0.5)
    
    def _initialize_resume_state(self) -> None:
        """Load last processed timestamps for resume capability."""
        self._last_out_ts = self._repo.get_last_timestamp(self._keys.indicator_out)
        self._last_sig_ts = self._repo.get_last_timestamp(self._keys.signal_out)
        
        if self._last_out_ts:
            LOG.info("[calc %s] Resume from indicator ts=%s", self._symbol, self._last_out_ts)
        if self._last_sig_ts:
            LOG.info("[calc %s] Resume from signal ts=%s", self._symbol, self._last_sig_ts)
    
    async def _trim_task(self) -> None:
        """Background task to trim streams based on retention policy."""
        if (
            self._config.stream_retention_ms_ind == 0
            and self._config.stream_retention_ms_signal == 0
        ):
            return
        
        LOG.info(
            "[calc %s] Time-based trim enabled (ind=%dms, sig=%dms)",
            self._symbol,
            self._config.stream_retention_ms_ind,
            self._config.stream_retention_ms_signal,
        )
        
        while True:
            try:
                self._repo.trim_streams(
                    self._keys,
                    self._config.stream_retention_ms_ind,
                    self._config.stream_retention_ms_signal,
                )
            except Exception as e:
                LOG.warning("[calc %s] Trim error: %s", self._symbol, e)
            
            await asyncio.sleep(60)
    
    async def _consume_stream(self) -> None:
        """Consume and process market data stream."""
        last_id = "0-0"
        is_live = False
        
        LOG.info(
            "[calc %s] Consuming %s → %s & %s",
            self._symbol,
            self._keys.market_in,
            self._keys.indicator_out,
            self._keys.signal_out,
        )
        
        while True:
            try:
                items = r_ingestor.xread(
                    {self._keys.market_in: last_id},
                    count=(200 if is_live else 1000),
                    block=(2000 if is_live else None),
                )
            except Exception as e:
                LOG.error("[calc %s] xread error: %s", self._symbol, e)
                await asyncio.sleep(0.5)
                continue
            
            if not items:
                if not is_live:
                    is_live = True
                    last_id = "$"
                    LOG.info("[calc %s] Bootstrap complete → live mode", self._symbol)
                continue
            
            _, entries = items[0]
            
            for msg_id, fields in entries:
                last_id = msg_id
                self._process_message(fields, is_live)
    
    def _process_message(self, fields: dict[str, str], is_live: bool) -> None:
        """Process a single market data message."""
        try:
            market = MarketData.from_fields(fields)
        except (KeyError, ValueError) as e:
            LOG.warning("[%s] Malformed fields: %s", self._symbol, e)
            return
        
        # Reset signal sequence counter for new timestamp
        if self._current_sig_ts != market.ts:
            self._current_sig_ts = market.ts
            self._current_sig_seq = 0
        
        # Update indicators
        ma20 = self._sma20.update(market.close)
        ma200 = self._sma200.update(market.close)
        
        # Log warmup progress
        if ma20 is None or ma200 is None:
            self._log_warmup_progress(market.close)
        
        # Calculate regime
        regime = choose_regime(market.close, ma20, ma200)
        
        # Update indicator state
        candle = Candle(
            ts=market.ts,
            open=market.open,
            high=market.high,
            low=market.low,
            close=market.close,
            color=market.color,
        )
        calc_result = self._indicator_state.update(candle, regime)
        
        # Create snapshot
        snapshot = IndicatorSnapshot(
            symbol=self._symbol,
            timeframe="2m",
            ts=market.ts,
            market=market,
            ma20=ma20,
            ma200=ma200,
            regime=regime,
            ind_ts=calc_result.indicator.ts if calc_result.indicator else None,
            ind_high=calc_result.indicator.high if calc_result.indicator else None,
            ind_low=calc_result.indicator.low if calc_result.indicator else None,
        )
        
        # Write indicator data
        should_emit_out = self._last_out_ts is None or market.ts > self._last_out_ts
        if should_emit_out:
            try:
                self._repo.write_indicator(
                    self._keys,
                    snapshot,
                    self._config.stream_maxlen_ind,
                )
                self._last_out_ts = market.ts
            except Exception as e:
                LOG.error("[calc %s] Failed to write indicator: %s", self._symbol, e)
        
        # Process signals (only in live mode)
        if is_live:
            self._process_signals(market.ts, regime, calc_result.indicator)
        
        # Debug logging
        if ma20 and ma200 and LOG.isEnabledFor(logging.DEBUG):
            LOG.debug(
                "[%s 2m] ts=%s close=%s ma20=%.2f ma200=%.2f regime=%s",
                self._symbol, market.ts, market.close, ma20, ma200, regime
            )
    
    def _log_warmup_progress(self, close: float) -> None:
        """Log indicator warmup progress periodically."""
        now = time.time()
        if now - self._last_warmup_log >= self._config.warmup_log_interval_sec:
            rem20 = max(0, 20 - self._sma20.count)
            rem200 = max(0, 200 - self._sma200.count)
            LOG.info(
                "[%s 2m] close=%s warmup: ma20 in %d bars, ma200 in %d bars",
                self._symbol, close, rem20, rem200
            )
            self._last_warmup_log = now
    
    def _process_signals(
        self,
        ts: int,
        regime: str,
        indicator_candle: Optional[Candle],
    ) -> None:
        """Generate and emit trading signals based on regime changes."""
        # Skip if already processed this timestamp
        should_emit = self._last_sig_ts is None or ts > self._last_sig_ts
        if not should_emit:
            if LOG.isEnabledFor(logging.DEBUG):
                LOG.debug(
                    "[%s SIGNAL] Skip emit @%s (already processed)",
                    self._symbol, ts
                )
            return
        
        # Generate signals
        signals = self._signal_gen.process(ts, regime, indicator_candle)
        
        # Emit each signal
        for signal_payload in signals:
            self._current_sig_seq += 1
            msg_id = f"{ts}-{self._current_sig_seq}"
            
            try:
                self._repo.write_signal(
                    self._keys.signal_out,
                    signal_payload,
                    msg_id,
                    self._config.stream_maxlen_signal,
                )
                self._last_sig_ts = ts
                
                # Log based on signal type
                if signal_payload["type"] == SignalType.ARM.value:
                    LOG.info(
                        "[%s SIGNAL] ARM %s ind_ts=%s trig=%s stop=%s",
                        self._symbol,
                        signal_payload["side"],
                        signal_payload["ind_ts"],
                        signal_payload["trigger"],
                        signal_payload["stop"],
                    )
                else:
                    LOG.info(
                        "[%s SIGNAL] DISARM side=%s reason=%s",
                        self._symbol,
                        signal_payload.get("prev_side", "?"),
                        signal_payload.get("reason", "?"),
                    )
            except Exception as e:
                LOG.error(
                    "[calc %s] Failed to write signal (type=%s): %s",
                    self._symbol,
                    signal_payload.get("type"),
                    e,
                )


# ═══════════════════════════════════════════════════════════════
#  SERVICE ORCHESTRATION
# ═══════════════════════════════════════════════════════════════

async def run_symbol_processor(symbol: str, config: CalcConfig) -> None:
    """Run the stream processor for a symbol with crash recovery."""
    symbol = symbol.upper().strip()
    LOG.info("[calc %s] Launching processor", symbol)
    
    backoff = 1.0
    repository = RedisRepositoryImpl(r_ingestor)
    
    while True:
        try:
            processor = StreamProcessor(symbol, config, repository)
            await processor.run()
            LOG.warning("[calc %s] Processor exited normally (will restart)", symbol)
        except asyncio.CancelledError:
            LOG.info("[calc %s] Processor cancelled", symbol)
            raise
        except Exception as e:
            LOG.exception("[calc %s] Processor crashed: %s", symbol, e)
        
        # Exponential backoff with cap
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2.0, 30.0)


async def main() -> None:
    """Main entry point for the calculator service."""
    config = CalcConfig.from_env()
    
    # Get configured symbols
    pairs = settings.pairs_1m_list()
    symbols = sorted({sym.upper().strip() for sym, _ in pairs if sym})
    
    if not symbols:
        raise RuntimeError("No symbols configured")
    
    LOG.info("Starting calc service for symbols: %s", symbols)
    
    # Create tasks for each symbol
    tasks = []
    for symbol in symbols:
        task = asyncio.create_task(
            run_symbol_processor(symbol, config),
            name=f"calc:{symbol}",
        )
        tasks.append(task)
        LOG.info("[calc %s] Task created", symbol)
    
    # Run all tasks
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        LOG.info("Shutting down calculator service")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())