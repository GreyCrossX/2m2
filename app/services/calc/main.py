# services/calc/main.py
from __future__ import annotations
import asyncio
import logging
import os
import time
from typing import Dict, Optional, Tuple

from app.config import settings
from services.ingestor.keys import st_market    # 2m input stream
from services.ingestor.keys import tag as tag_tf
from services.ingestor.redis_io import r as r_ingestor  # Redis client

from .indicators import SMA
from .strategy import Candle, IndicatorState, choose_regime

# ─────────────────────────── Logging ────────────────────────────
LOG = logging.getLogger("calc")
logging.basicConfig(
    level=getattr(logging, os.getenv("CALC_LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

def _lv() -> bool:  # quick DEBUG check
    return LOG.isEnabledFor(logging.DEBUG)

# ─────────────────────────── Settings ───────────────────────────
PRICE_TICK = float(os.getenv("PRICE_TICK_DEFAULT", "0.01"))  # TODO: per-symbol tick later

# stream caps (length-based, recommended)
STREAM_MAXLEN_IND    = int(os.getenv("STREAM_MAXLEN_IND", "5000"))    # calc events
STREAM_MAXLEN_SIGNAL = int(os.getenv("STREAM_MAXLEN_SIGNAL", "2000")) # signals

# optional time-based retention (ms); 0 disables
STREAM_RETENTION_MS_IND    = int(os.getenv("STREAM_RETENTION_MS_IND", "0"))
STREAM_RETENTION_MS_SIGNAL = int(os.getenv("STREAM_RETENTION_MS_SIGNAL", "0"))

# throttle for warmup logs (seconds)
WARMUP_LOG_EVERY_SEC = int(os.getenv("CALC_WARMUP_LOG_EVERY_SEC", "2"))

# ─────────────────────────── Helpers ────────────────────────────
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

# Normalize symbol to UPPERCASE everywhere keys are derived
def market_stream(sym: str) -> str:
    return st_market(sym.upper(), "2m")

def indicator_stream(sym: str) -> str:
    return f"stream:ind|{{{tag_tf(sym.upper(), '2m')}}}"

def signal_stream(sym: str) -> str:
    return f"stream:signal|{{{tag_tf(sym.upper(), '2m')}}}"

def snapshot_hash(sym: str) -> str:
    return f"snap:ind|{{{tag_tf(sym.upper(), '2m')}}}"

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
            LOG.warning("[calc %s] trim error on (%s,%s): %s", sym, out, sig, e)
        await asyncio.sleep(60)

def _fmt_none(x: Optional[float]) -> str:
    return "" if x is None else f"{x}"

def _calc_long_levels(ind_high: float, ind_low: float) -> Tuple[float, float]:
    trigger = ind_high + PRICE_TICK
    stop    = ind_low  - PRICE_TICK
    return trigger, stop

def _calc_short_levels(ind_high: float, ind_low: float) -> Tuple[float, float]:
    trigger = ind_low  - PRICE_TICK
    stop    = ind_high + PRICE_TICK
    return trigger, stop

def _fields_preview(fields: Dict[str, str]) -> str:
    want = ("ts", "open", "high", "low", "close", "color")
    pairs = []
    for k in want:
        v = fields.get(k)
        pairs.append(f"{k}={v if v is not None else '<MISSING>'}")
    return ", ".join(pairs)

def _safe_int(s: str, default: Optional[int] = None) -> Optional[int]:
    try:
        return int(s)
    except Exception:
        return default

# ───────────────────────── Core Consumer ────────────────────────
async def consume_symbol(sym: str):
    """
    Consume 2m market stream for `sym`, compute SMA20/200, regime, indicator candle,
    publish snapshot + calc event + high-level signal events for Celery workers.
    """
    LOG.info("[calc %s] task created", sym.upper())
    sym = sym.upper()

    # Derive keys & log them
    try:
        stream_in  = market_stream(sym)
        stream_out = indicator_stream(sym)
        stream_sig = signal_stream(sym)
        snap_key   = snapshot_hash(sym)
    except Exception as e:
        LOG.exception("[calc %s] key derivation failed: %s", sym, e)
        raise

    LOG.info("[calc %s] I/O keys: IN=%s  OUT=%s  SIG=%s  SNAP=%s",
             sym, stream_in, stream_out, stream_sig, snap_key)

    # Redis probe + input stream probe
    try:
        r_ingestor.ping()
        xlen  = r_ingestor.xlen(stream_in)
        first = r_ingestor.xrange(stream_in, count=1) or []
        last  = r_ingestor.xrevrange(stream_in, count=1) or []
        LOG.info("[calc %s] input stream len=%s, first=%s, last=%s",
                 sym, xlen,
                 first and f"{first[0][0]} ts={_safe_int(first[0][1].get('ts',''))}" or "None",
                 last and f"{last[0][0]} ts={_safe_int(last[0][1].get('ts',''))}" or "None")
        if first:
            LOG.debug("[calc %s] first fields: %s", sym, _fields_preview(first[0][1]))
        if last:
            LOG.debug("[calc %s] last  fields: %s", sym, _fields_preview(last[0][1]))
    except Exception as e:
        LOG.exception("[calc %s] Redis/stream probe failed: %s", sym, e)
        # continue; the loop will retry xread with backoff

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

    last_regime: str = "neutral"
    armed_side: Optional[str] = None  # retained for future policies

    # Per-bar sequence for signals so we can DISARM+ARM on same ts
    current_sig_ts: Optional[int] = None
    current_sig_seq: int = 0

    # Optional trimmer
    asyncio.create_task(_trim_task_sym(sym))

    # Bootstrap from history (0-0), then switch to live ($).
    last_id = "0-0"
    live = False
    LOG.info("[calc %s] consuming %s → writing %s & %s (bootstrap from history)",
             sym, stream_in, stream_out, stream_sig)

    last_warmup_log = 0.0

    while True:
        try:
            if live:
                items = r_ingestor.xread({stream_in: last_id}, count=200, block=2000)
            else:
                items = r_ingestor.xread({stream_in: last_id}, count=1000)
        except Exception as e:
            LOG.error("[calc %s] xread error on %s (last_id=%s): %s", sym, stream_in, last_id, e)
            await asyncio.sleep(0.5)
            continue

        if not items:
            if not live:
                live = True
                last_id = "$"
                LOG.info("[calc %s] bootstrap complete → switching to live", sym)
            # else: idle while live
            continue

        stream_name, entries = items[0]
        if stream_name != stream_in:
            LOG.warning("[calc %s] xread returned unexpected stream %s (expected %s)",
                        sym, stream_name, stream_in)

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
                LOG.warning("[%s] malformed fields at %s: %s. fields_preview={%s}",
                            sym, msg_id, e, _fields_preview(fields))
                continue

            # Reset per-bar signal sequence for a new ts
            if current_sig_ts != ts:
                current_sig_ts = ts
                current_sig_seq = 0

            v20  = sma20.update(close)
            v200 = sma200.update(close)

            # Throttled warmup logs
            if v20 is None or v200 is None:
                now = time.time()
                if now - last_warmup_log >= WARMUP_LOG_EVERY_SEC:
                    rem20  = max(0, 20  - sma20.count)
                    rem200 = max(0, 200 - sma200.count)
                    LOG.info("[%s 2m] close=%s warmup: ma20 in %d bars, ma200 in %d bars",
                             sym, close, rem20, rem200)
                    last_warmup_log = now

            regime = choose_regime(close, v20, v200)

            # Indicator candle update
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

            should_emit_out = (last_out_ts is None) or (ts > last_out_ts)
            pipe = r_ingestor.pipeline()
            try:
                pipe.hset(snap_key, mapping=out)
                if should_emit_out:
                    pipe.xadd(
                        stream_out, out, id=f"{ts}-0",
                        maxlen=STREAM_MAXLEN_IND, approximate=True
                    )
                pipe.execute()
            except Exception as e:
                LOG.error("[calc %s] snapshot/xadd error (out=%s, ts=%s): %s",
                          sym, stream_out, ts, e)
            else:
                if should_emit_out:
                    last_out_ts = ts  # advance watermark

            # ── During bootstrap, no signals ─────────────────────────────────
            if not live:
                last_regime = regime
                continue

            # ── SIGNALS: FSM with direct flip safety ─────────────────────────
            flip = last_regime in ("long", "short") and regime in ("long", "short") and regime != last_regime

            # Only emit signals for bars strictly newer than the last signal we wrote
            should_emit_sig = (last_sig_ts is None) or (ts > last_sig_ts)
            if not should_emit_sig:
                if _lv():
                    LOG.debug("[%s SIGNAL] skip emit @%s (ts<=last_sig_ts=%s)", sym, ts, last_sig_ts)
                last_regime = regime
                continue

            # CASE A: neutral -> long/short : ARM
            if last_regime == "neutral" and regime in ("long", "short"):
                if ind is None:
                    LOG.info("[%s SIGNAL] SKIP ARM (%s): no indicator candle yet (ts=%s)",
                             sym, regime, ts)
                else:
                    if regime == "long":
                        trigger, stop = _calc_long_levels(ind.high, ind.low)
                        payload = {
                            "v": "1", "type": "arm", "side": "long", "sym": sym, "tf": "2m",
                            "ts": str(ts),
                            "ind_ts": str(ind.ts), "ind_high": f"{ind.high}", "ind_low": f"{ind.low}",
                            "trigger": f"{trigger}", "stop": f"{stop}"
                        }
                    else:
                        trigger, stop = _calc_short_levels(ind.high, ind.low)
                        payload = {
                            "v": "1", "type": "arm", "side": "short", "sym": sym, "tf": "2m",
                            "ts": str(ts),
                            "ind_ts": str(ind.ts), "ind_high": f"{ind.high}", "ind_low": f"{ind.low}",
                            "trigger": f"{trigger}", "stop": f"{stop}"
                        }
                    current_sig_seq += 1
                    try:
                        r_ingestor.xadd(
                            stream_sig, payload, id=f"{ts}-{current_sig_seq}",
                            maxlen=STREAM_MAXLEN_SIGNAL, approximate=True
                        )
                    except Exception as e:
                        LOG.error("[%s SIGNAL] ARM write failed (sig=%s ts=%s): %s",
                                  sym, stream_sig, ts, e)
                    else:
                        last_sig_ts = ts
                        armed_side = regime
                        LOG.info("[%s SIGNAL] ARM %s ind_ts=%s trig=%s stop=%s",
                                 sym, regime, payload["ind_ts"], payload["trigger"], payload["stop"])

            # CASE B: long/short -> neutral : DISARM
            if last_regime in ("long", "short") and regime == "neutral":
                current_sig_seq += 1
                payload = {
                    "v": "1", "type": "disarm", "prev_side": last_regime, "sym": sym, "tf": "2m",
                    "ts": str(ts), "reason": f"regime:{last_regime}->neutral"
                }
                try:
                    r_ingestor.xadd(
                        stream_sig, payload,
                        id=f"{ts}-{current_sig_seq}", maxlen=STREAM_MAXLEN_SIGNAL, approximate=True
                    )
                except Exception as e:
                    LOG.error("[%s SIGNAL] DISARM write failed (sig=%s ts=%s): %s",
                              sym, stream_sig, ts, e)
                else:
                    last_sig_ts = ts
                    LOG.info("[%s SIGNAL] DISARM side=%s reason=regime-change->neutral",
                             sym, last_regime)
                    armed_side = None

            # CASE C: long <-> short : DISARM old then ARM new
            if flip:
                # DISARM old
                current_sig_seq += 1
                payload_d = {
                    "v": "1", "type": "disarm", "prev_side": last_regime, "sym": sym, "tf": "2m",
                    "ts": str(ts), "reason": f"regime:{last_regime}->{regime} (direct-flip)"
                }
                try:
                    r_ingestor.xadd(
                        stream_sig, payload_d,
                        id=f"{ts}-{current_sig_seq}", maxlen=STREAM_MAXLEN_SIGNAL, approximate=True
                    )
                except Exception as e:
                    LOG.error("[%s SIGNAL] DISARM (flip) write failed (sig=%s ts=%s): %s",
                              sym, stream_sig, ts, e)
                else:
                    last_sig_ts = ts
                    LOG.info("[%s SIGNAL] DISARM (direct flip) side=%s -> %s",
                             sym, last_regime, regime)
                    armed_side = None

                # ARM new
                if ind is None:
                    LOG.info("[%s SIGNAL] SKIP ARM after flip to %s: no indicator candle yet (ts=%s)",
                             sym, regime, ts)
                else:
                    if regime == "long":
                        trigger, stop = _calc_long_levels(ind.high, ind.low)
                        payload_a = {
                            "v": "1", "type": "arm", "side": "long", "sym": sym, "tf": "2m",
                            "ts": str(ts),
                            "ind_ts": str(ind.ts), "ind_high": f"{ind.high}", "ind_low": f"{ind.low}",
                            "trigger": f"{trigger}", "stop": f"{stop}"
                        }
                    else:
                        trigger, stop = _calc_short_levels(ind.high, ind.low)
                        payload_a = {
                            "v": "1", "type": "arm", "side": "short", "sym": sym, "tf": "2m",
                            "ts": str(ts),
                            "ind_ts": str(ind.ts), "ind_high": f"{ind.high}", "ind_low": f"{ind.low}",
                            "trigger": f"{trigger}", "stop": f"{stop}"
                        }
                    current_sig_seq += 1
                    try:
                        r_ingestor.xadd(
                            stream_sig, payload_a, id=f"{ts}-{current_sig_seq}",
                            maxlen=STREAM_MAXLEN_SIGNAL, approximate=True
                        )
                    except Exception as e:
                        LOG.error("[%s SIGNAL] ARM (after flip) write failed (sig=%s ts=%s): %s",
                                  sym, stream_sig, ts, e)
                    else:
                        last_sig_ts = ts
                        armed_side = regime
                        LOG.info("[%s SIGNAL] ARM %s (after flip) ind_ts=%s trig=%s stop=%s",
                                 sym, regime, payload_a["ind_ts"], payload_a["trigger"], payload_a["stop"])

            # Low-noise calc confirmation when MAs are ready
            if v20 is not None and v200 is not None and _lv():
                LOG.debug(
                    "[%s 2m] ts=%s close=%s ma20=%.2f ma200=%.2f regime=%s ind=%s",
                    sym, ts, close, v20, v200, regime, out['ind_ts'] or "-"
                )

            last_regime = regime

# ─────────────────────── Crash-guard wrapper ─────────────────────
async def consume_symbol_guarded(sym: str):
    LOG.info("[calc %s] launching guarded task", sym.upper())
    try:
        await consume_symbol(sym)
    except asyncio.CancelledError:
        LOG.info("[calc %s] task cancelled", sym.upper())
        raise
    except Exception as e:
        LOG.exception("[calc %s] task crashed: %s", sym.upper(), e)

# ───────────────────────────── main() ────────────────────────────
async def main():
    pairs = settings.pairs_1m_list()
    # keep only 1m entries; uppercase symbols to match stream keys
    syms = []
    for sym, tf in pairs:
        if (tf or "").strip().lower() == "1m":
            syms.append(sym.upper())

    if not syms:
        raise RuntimeError("No symbols configured")
    LOG.info("Starting calc service for symbols: %s", syms)

    tasks = [asyncio.create_task(consume_symbol_guarded(sym)) for sym in syms]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
