# services/calc/main.py
from __future__ import annotations
import asyncio
import logging
import os
import time
from typing import Dict, Optional

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

def _last_written_ts(stream_key: str) -> Optional[int]:
    """Return the numeric millisecond TS (int) of the latest entry in a stream, or None."""
    try:
        last = r_ingestor.xrevrange(stream_key, count=1)
        if last:
            last_id, _ = last[0]
            return int(last_id.split("-")[0])
    except Exception as e:
        LOG.warning("xrevrange(%s) failed: %s", stream_key, e)
    return None

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

def _fmt_none(x: Optional[float]) -> str:
    return "" if x is None else f"{x}"

def _calc_long_levels(ind_high: float, ind_low: float) -> tuple[float, float]:
    # LONG: entry above indicator high; stop below indicator low
    trigger = ind_high + PRICE_TICK
    stop    = ind_low  - PRICE_TICK
    return trigger, stop

def _calc_short_levels(ind_high: float, ind_low: float) -> tuple[float, float]:
    # SHORT: entry below indicator low; stop above indicator high
    trigger = ind_low  - PRICE_TICK
    stop    = ind_high + PRICE_TICK
    return trigger, stop

async def consume_symbol(sym: str):
    """
    Consume 2m market stream for `sym`, compute SMA20/200, regime, indicator candle,
    and publish snapshot + calc event + high-level signal events for Celery workers.

    Signal policy (per spec):
      - neutral -> long/short : ARM (with indicator candle info and trigger/stop)
      - long/short -> neutral : DISARM
      - long <-> short (direct flip): DISARM (old) then ARM (new) on the same bar
    """
    stream_in  = market_stream(sym)
    stream_out = indicator_stream(sym)
    stream_sig = signal_stream(sym)
    snap_key   = snapshot_hash(sym)

    sma20 = SMA(20)
    sma200 = SMA(200)
    istate = IndicatorState(price_tick=PRICE_TICK)

    # Resume watermarks (avoid XADD id <= last error)
    last_out_ts = _last_written_ts(stream_out)
    last_sig_ts = _last_written_ts(stream_sig)
    if last_out_ts is not None:
        LOG.info("[calc %s] resume: last calc ts=%s (skip <= this ts)", sym, last_out_ts)
    if last_sig_ts is not None:
        LOG.info("[calc %s] resume: last signal ts=%s (skip <= this ts)", sym, last_sig_ts)

    # ── state for signal emission ─────────────────────────────────────────────
    last_regime: str = "neutral"             # previous bar regime
    armed_side: Optional[str] = None         # "long" | "short" if armed, else None

    # Per-bar sequence for signals so we can DISARM+ARM on same ts
    current_sig_ts: Optional[int] = None
    current_sig_seq: int = 0

    # launch optional time-based trimmer
    asyncio.create_task(_trim_task_sym(sym))

    # Bootstrap from history (0-0), then switch to live ($). Emit NO signals during bootstrap.
    last_id = "0-0"
    live = False
    LOG.info("[calc %s] consuming %s → writing %s & %s (bootstrap from history)", sym, stream_in, stream_out, stream_sig)

    while True:
        # Non-blocking larger reads while bootstrapping; modest blocking once live
        if live:
            items = r_ingestor.xread({stream_in: last_id}, count=200, block=2000)
        else:
            items = r_ingestor.xread({stream_in: last_id}, count=1000)

        if not items:
            if not live:
                # end of backlog → follow live now
                live = True
                last_id = "$"
                LOG.info("[calc %s] bootstrap complete → switching to live", sym)
                continue
            else:
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

            # Reset per-bar signal sequence for a new ts
            if current_sig_ts != ts:
                current_sig_ts = ts
                current_sig_seq = 0

            v20  = sma20.update(close)
            v200 = sma200.update(close)

            # WARMUP progress printing
            rem20  = max(0, 20  - sma20.count)
            rem200 = max(0, 200 - sma200.count)
            if rem20 > 0 or rem200 > 0:
                LOG.info("[%s 2m] close=%s warmup: ma20 in %d bars, ma200 in %d bars",
                         sym, close, rem20, rem200)

            regime = choose_regime(close, v20, v200)

            # Update indicator candle state (indicator = last red for long / last green for short)
            cres = istate.update(
                Candle(ts=ts, open=open_, high=high, low=low, close=close, color=color),
                regime
            )
            ind = cres.indicator  # Candle | None

            # ── publish calc snapshot/event ───────────────────────────────────
            out: Dict[str, str] = {
                "v": "1",
                "sym": sym, "tf": "2m",
                "ts": str(ts),
                "close": str(close), "open": str(open_), "high": str(high), "low": str(low), "color": color,
                "ma20":  _fmt_none(v20),
                "ma200": _fmt_none(v200),
                "regime": regime,
                "ind_ts":   "" if not ind else str(ind.ts),
                "ind_high": "" if not ind else f"{ind.high}",
                "ind_low":  "" if not ind else f"{ind.low}",
            }

            # Always refresh snapshot; append to calc stream only if strictly newer than last_out_ts
            should_emit_out = (last_out_ts is None) or (ts > last_out_ts)
            pipe = r_ingestor.pipeline()
            pipe.hset(snap_key, mapping=out)
            if should_emit_out:
                pipe.xadd(
                    stream_out, out, id=f"{ts}-0",
                    maxlen=STREAM_MAXLEN_IND, approximate=True
                )
            pipe.execute()
            if should_emit_out:
                last_out_ts = ts  # advance watermark

            # ── During bootstrap, DO NOT emit signals ────────────────────────
            if not live:
                last_regime = regime
                continue

            # ── SIGNALS: FSM with direct flip safety ─────────────────────────
            flip = last_regime in ("long", "short") and regime in ("long", "short") and regime != last_regime

            # Only emit signals for bars strictly newer than the last signal we wrote
            should_emit_sig = (last_sig_ts is None) or (ts > last_sig_ts)
            if not should_emit_sig:
                last_regime = regime
                continue

            # CASE A: neutral -> long/short : ARM
            if last_regime == "neutral" and regime in ("long", "short"):
                if ind is None:
                    LOG.info("[%s SIGNAL] SKIP ARM (%s): no indicator candle yet", sym, regime)
                else:
                    if regime == "long":
                        trigger, stop = _calc_long_levels(ind.high, ind.low)
                        payload = {
                            "v": "1", "type": "arm", "side": "long", "sym": sym, "tf": "2m",
                            "ts": str(ts),
                            "ind_ts": str(ind.ts), "ind_high": f"{ind.high}", "ind_low": f"{ind.low}",
                            "trigger": f"{trigger}", "stop": f"{stop}"
                        }
                    else:  # short
                        trigger, stop = _calc_short_levels(ind.high, ind.low)
                        payload = {
                            "v": "1", "type": "arm", "side": "short", "sym": sym, "tf": "2m",
                            "ts": str(ts),
                            "ind_ts": str(ind.ts), "ind_high": f"{ind.high}", "ind_low": f"{ind.low}",
                            "trigger": f"{trigger}", "stop": f"{stop}"
                        }
                    current_sig_seq += 1
                    r_ingestor.xadd(
                        stream_sig, payload, id=f"{ts}-{current_sig_seq}",
                        maxlen=STREAM_MAXLEN_SIGNAL, approximate=True
                    )
                    last_sig_ts = ts
                    armed_side = regime
                    LOG.info("[%s SIGNAL] ARM %s ind_ts=%s trig=%s stop=%s",
                             sym, regime, payload["ind_ts"], payload["trigger"], payload["stop"])

            # CASE B: long/short -> neutral : DISARM
            if last_regime in ("long", "short") and regime == "neutral":
                current_sig_seq += 1
                r_ingestor.xadd(
                    stream_sig,
                    {
                        "v": "1", "type": "disarm", "prev_side": last_regime, "sym": sym, "tf": "2m",
                        "ts": str(ts), "reason": f"regime:{last_regime}->neutral"
                    },
                    id=f"{ts}-{current_sig_seq}", maxlen=STREAM_MAXLEN_SIGNAL, approximate=True
                )
                last_sig_ts = ts
                LOG.info("[%s SIGNAL] DISARM side=%s reason=regime-change->neutral",
                         sym, last_regime)
                armed_side = None

            # CASE C: long <-> short (direct flip): DISARM old then ARM new
            if flip:
                # First DISARM the previous side
                current_sig_seq += 1
                r_ingestor.xadd(
                    stream_sig,
                    {
                        "v": "1", "type": "disarm", "prev_side": last_regime, "sym": sym, "tf": "2m",
                        "ts": str(ts), "reason": f"regime:{last_regime}->{regime} (direct-flip)"
                    },
                    id=f"{ts}-{current_sig_seq}", maxlen=STREAM_MAXLEN_SIGNAL, approximate=True
                )
                last_sig_ts = ts
                LOG.info("[%s SIGNAL] DISARM (direct flip) side=%s -> %s",
                         sym, last_regime, regime)
                armed_side = None

                # Then ARM the new side if we have an indicator
                if ind is None:
                    LOG.info("[%s SIGNAL] SKIP ARM after flip to %s: no indicator candle yet",
                             sym, regime)
                else:
                    if regime == "long":
                        trigger, stop = _calc_long_levels(ind.high, ind.low)
                        payload = {
                            "v": "1", "type": "arm", "side": "long", "sym": sym, "tf": "2m",
                            "ts": str(ts),
                            "ind_ts": str(ind.ts), "ind_high": f"{ind.high}", "ind_low": f"{ind.low}",
                            "trigger": f"{trigger}", "stop": f"{stop}"
                        }
                    else:  # short
                        trigger, stop = _calc_short_levels(ind.high, ind.low)
                        payload = {
                            "v": "1", "type": "arm", "side": "short", "sym": sym, "tf": "2m",
                            "ts": str(ts),
                            "ind_ts": str(ind.ts), "ind_high": f"{ind.high}", "ind_low": f"{ind.low}",
                            "trigger": f"{trigger}", "stop": f"{stop}"
                        }
                    current_sig_seq += 1
                    r_ingestor.xadd(
                        stream_sig, payload, id=f"{ts}-{current_sig_seq}",
                        maxlen=STREAM_MAXLEN_SIGNAL, approximate=True
                    )
                    last_sig_ts = ts
                    armed_side = regime
                    LOG.info("[%s SIGNAL] ARM %s (after flip) ind_ts=%s trig=%s stop=%s",
                             sym, regime, payload["ind_ts"], payload["trigger"], payload["stop"])

            # Update last_regime after all signal logic for this bar
            last_regime = regime

            # Low-noise calc confirmation when MAs are ready
            if v20 is not None and v200 is not None:
                LOG.info(
                    "[%s 2m] close=%s ma20=%.2f ma200=%.2f regime=%s ind=%s",
                    sym, close, v20, v200, regime, out['ind_ts'] or "-"
                )

async def main():
    pairs = settings.pairs_1m_list()
    syms = [sym for sym, tf in pairs]  # Take all symbols regardless of timeframe
    if not syms:
        raise RuntimeError("No symbols configured")
    LOG.info("Starting calc service for symbols: %s", syms)
    tasks = [asyncio.create_task(consume_symbol(sym)) for sym in syms]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
