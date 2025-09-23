# services/calc/main.py
from __future__ import annotations
import asyncio
import logging
import os
import time
from typing import Dict, Any, Optional

from app.config import settings
from services.ingestor.keys import st_market    # 2m input stream
from services.ingestor.keys import tag as tag_tf
from services.ingestor.redis_io import r as r_ingestor  # Redis client

from .indicators import SMA
from .strategy import Candle, IndicatorState, choose_regime

LOG = logging.getLogger("calc")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")

PRICE_TICK = float(os.getenv("PRICE_TICK_DEFAULT", "0.01"))  # TODO: per-symbol tick later

# ── stream caps (length-based, recommended) ───────────────────────────────────
STREAM_MAXLEN_IND    = int(os.getenv("STREAM_MAXLEN_IND", "5000"))    # calc events
STREAM_MAXLEN_SIGNAL = int(os.getenv("STREAM_MAXLEN_SIGNAL", "2000")) # signals

# ── optional time-based retention (ms); 0 disables ───────────────────────────
STREAM_RETENTION_MS_IND    = int(os.getenv("STREAM_RETENTION_MS_IND", "0"))
STREAM_RETENTION_MS_SIGNAL = int(os.getenv("STREAM_RETENTION_MS_SIGNAL", "0"))
# ─────────────────────────────────────────────────────────────────────────────

def market_stream(sym: str) -> str:
    return st_market(sym, "2m")

def indicator_stream(sym: str) -> str:
    return f"stream:ind|{{{tag_tf(sym, '2m')}}}"

def signal_stream(sym: str) -> str:
    return f"stream:signal|{{{tag_tf(sym, '2m')}}}"

def snapshot_hash(sym: str) -> str:
    return f"snap:ind|{{{tag_tf(sym, '2m')}}}"

def to_float(s: str) -> float:
    return float(s)

async def _trim_task_sym(sym: str):
    """Optional periodic time-based trimming using XTRIM MINID (approx)."""
    out = indicator_stream(sym)
    sig = signal_stream(sym)
    if STREAM_RETENTION_MS_IND == 0 and STREAM_RETENTION_MS_SIGNAL == 0:
        return
    LOG.info("[calc %s] time-based trim enabled (ind=%dms, sig=%dms)",
             sym, STREAM_RETENTION_MS_IND, STREAM_RETENTION_MS_SIGNAL)
    while True:
        now = int(time.time() * 1000)
        try:
            if STREAM_RETENTION_MS_IND > 0:
                r_ingestor.xtrim(out, minid=now - STREAM_RETENTION_MS_IND, approximate=True)
            if STREAM_RETENTION_MS_SIGNAL > 0:
                r_ingestor.xtrim(sig, minid=now - STREAM_RETENTION_MS_SIGNAL, approximate=True)
        except Exception as e:
            LOG.warning("[calc %s] trim error: %s", sym, e)
        await asyncio.sleep(60)

async def consume_symbol(sym: str):
    """
    Consume 2m market stream for `sym`, compute SMA20/200, regime, indicator candle,
    and publish snapshot + calc event + high-level signal events for Celery.
    """
    stream_in  = market_stream(sym)
    stream_out = indicator_stream(sym)
    stream_sig = signal_stream(sym)
    snap_key   = snapshot_hash(sym)

    sma20 = SMA(20)
    sma200 = SMA(200)
    istate = IndicatorState(price_tick=PRICE_TICK)

    # Track last indicator we announced to avoid duplicate ARMs
    last_indicator_ts: Optional[int] = None
    last_regime: str = "neutral"

    # launch optional time-based trimmer
    asyncio.create_task(_trim_task_sym(sym))

    last_id = "$"  # follow live only; set "0-0" to backfill from history
    LOG.info("[calc %s] consuming %s → writing %s & %s", sym, stream_in, stream_out, stream_sig)

    while True:
        items = r_ingestor.xread({stream_in: last_id}, count=10, block=2000)
        if not items:
            continue
        _, entries = items[0]
        for msg_id, fields in entries:
            last_id = msg_id
            try:
                ts    = int(fields["ts"])
                close = to_float(fields["close"])
                open_ = to_float(fields["open"])
                high  = to_float(fields["high"])
                low   = to_float(fields["low"])
                color = fields.get("color", "green")
            except Exception as e:
                LOG.warning("[%s] malformed fields at %s: %s", sym, msg_id, e)
                continue

            v20  = sma20.update(close)
            v200 = sma200.update(close)

            # WARMUP progress printing
            rem20  = max(0, 20  - sma20.count)
            rem200 = max(0, 200 - sma200.count)
            if rem20 > 0 or rem200 > 0:
                LOG.info("[%s 2m] close=%s warmup: ma20 in %d bars, ma200 in %d bars",
                         sym, close, rem20, rem200)

            regime = choose_regime(close, v20, v200)

            # Update indicator candle state (and compute triggers/stops)
            cres = istate.update(
                Candle(ts=ts, open=open_, high=high, low=low, close=close, color=color),
                regime
            )

            # Build calc snapshot/event (strings)
            out: Dict[str, str] = {
                "v": "1",
                "sym": sym, "tf": "2m",
                "ts": str(ts),
                "close": str(close), "open": str(open_), "high": str(high), "low": str(low), "color": color,
                "ma20":  "" if v20  is None else f"{v20}",
                "ma200": "" if v200 is None else f"{v200}",
                "regime": regime,
                "ind_ts":   "" if not cres.indicator else str(cres.indicator.ts),
                "ind_high": "" if not cres.indicator else f"{cres.indicator.high}",
                "ind_low":  "" if not cres.indicator else f"{cres.indicator.low}",
                "trigger_long":  "" if cres.trigger_long  is None else f"{cres.trigger_long}",
                "stop_long":     "" if cres.stop_long     is None else f"{cres.stop_long}",
                "trigger_short": "" if cres.trigger_short is None else f"{cres.trigger_short}",
                "stop_short":    "" if cres.stop_short    is None else f"{cres.stop_short}",
            }

            # Write snapshot + calc event (bounded + explicit ID = '<ts>-0')
            pipe = r_ingestor.pipeline()
            pipe.hset(snap_key, mapping=out)
            pipe.xadd(stream_out, out, id=f"{ts}-0",
                      maxlen=STREAM_MAXLEN_IND, approximate=True)
            pipe.execute()

            # ── SIGNALS for Celery workers (bounded + explicit ID) ────────────
            # ARM when indicator candle changes under a valid regime
            if regime == "long" and cres.indicator and cres.indicator.ts != last_indicator_ts:
                r_ingestor.xadd(stream_sig, {
                    "v": "1", "type": "arm", "side": "long", "sym": sym, "tf": "2m",
                    "ts": str(ts), "ind_ts": str(cres.indicator.ts),
                    "trigger": f"{cres.trigger_long}", "stop": f"{cres.stop_long}"
                }, id=f"{ts}-0", maxlen=STREAM_MAXLEN_SIGNAL, approximate=True)
                last_indicator_ts = cres.indicator.ts
                LOG.info("[%s SIGNAL] ARM long ind_ts=%s trigger=%s stop=%s",
                         sym, last_indicator_ts, out["trigger_long"], out["stop_long"])

            if regime == "short" and cres.indicator and cres.indicator.ts != last_indicator_ts:
                r_ingestor.xadd(stream_sig, {
                    "v": "1", "type": "arm", "side": "short", "sym": sym, "tf": "2m",
                    "ts": str(ts), "ind_ts": str(cres.indicator.ts),
                    "trigger": f"{cres.trigger_short}", "stop": f"{cres.stop_short}"
                }, id=f"{ts}-0", maxlen=STREAM_MAXLEN_SIGNAL, approximate=True)
                last_indicator_ts = cres.indicator.ts
                LOG.info("[%s SIGNAL] ARM short ind_ts=%s trigger=%s stop=%s",
                         sym, last_indicator_ts, out["trigger_short"], out["stop_short"])

            # DISARM if we leave the regime (before a worker fills an order)
            if last_regime in ("long", "short") and regime != last_regime:
                r_ingestor.xadd(stream_sig, {
                    "v": "1", "type": "disarm", "prev_side": last_regime, "sym": sym, "tf": "2m",
                    "ts": str(ts), "reason": f"regime:{last_regime}->{regime}"
                }, id=f"{ts}-0", maxlen=STREAM_MAXLEN_SIGNAL, approximate=True)
                LOG.info("[%s SIGNAL] DISARM side=%s reason=regime-change->%s",
                         sym, last_regime, regime)
                last_indicator_ts = None

            last_regime = regime

            # Low-noise calc confirmation when MAs are ready
            if v20 is not None and v200 is not None:
                LOG.info(
                    "[%s 2m] close=%s ma20=%.2f ma200=%.2f regime=%s ind=%s trigL=%s trigS=%s",
                    sym, close, v20, v200, regime,
                    out['ind_ts'] or "-", out['trigger_long'] or "-", out['trigger_short'] or "-"
                )

async def main():
    pairs = settings.pairs_1m_list()
    syms = [sym for sym, tf in pairs if tf.lower() == "1m"]
    if not syms:
        raise RuntimeError("No symbols configured")
    tasks = [asyncio.create_task(consume_symbol(sym)) for sym in syms]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
