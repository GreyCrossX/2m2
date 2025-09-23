# services/ingestor/main.py
from __future__ import annotations

import asyncio
import logging
import signal
from typing import Dict, Any
from datetime import datetime, timezone
import os
import time

from app.config import settings
from .redis_io import ping_redis, dedupe_once, r  # NOTE: import r so we can use XADD/XTRIM options
from .keys import st_market, cid_1m, cid_2m, dedupe_key
from .normalize import normalize_closed_kline_1m
from .aggregator import TwoMinuteAggregator, OneMinute
from .binance_ws import listen_1m

LOG = logging.getLogger("ingestor")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

# ── runtime toggles ────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("INGESTOR_LOG_LEVEL", "INFO").upper()
logging.getLogger().setLevel(LOG_LEVEL)
LOG_1M = (os.getenv("INGESTOR_LOG_1M", "false").lower() == "true")

# Stream caps (length-based, fast & recommended)
STREAM_MAXLEN_1M = int(os.getenv("STREAM_MAXLEN_1M", "5000"))
STREAM_MAXLEN_2M = int(os.getenv("STREAM_MAXLEN_2M", "5000"))

# Optional time-based retention (ms); 0 disables
STREAM_RETENTION_MS_1M = int(os.getenv("STREAM_RETENTION_MS_1M", "0"))
STREAM_RETENTION_MS_2M = int(os.getenv("STREAM_RETENTION_MS_2M", "0"))
# ──────────────────────────────────────────────────────────────────────────────

def _iso(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def _parity(ts_ms: int) -> str:
    m = (ts_ms // 60000) % 60
    return "even" if (m % 2) == 0 else "odd"

def _f(s: str) -> float:
    return float(s)

def _xadd_with_caps(stream: str, fields: Dict[str, str], ts_ms: int, maxlen: int) -> str:
    """
    Add to a Redis Stream with:
      - explicit ID = '<close_ts_ms>-0' (lets us trim by time with MINID)
      - MAXLEN ~ maxlen (approximate, efficient)
    """
    return r.xadd(
        stream,
        fields,
        id=f"{ts_ms}-0",
        maxlen=maxlen,
        approximate=True,
    )

async def _trim_task(sym: str):
    """
    Optional periodic time-based trimming using XTRIM MINID.
    Only runs if STREAM_RETENTION_MS_* > 0.
    """
    s1 = st_market(sym, "1m")
    s2 = st_market(sym, "2m")
    if STREAM_RETENTION_MS_1M == 0 and STREAM_RETENTION_MS_2M == 0:
        return  # nothing to do

    LOG.info("[%s] time-based trim enabled (1m=%dms, 2m=%dms)",
             sym, STREAM_RETENTION_MS_1M, STREAM_RETENTION_MS_2M)
    while True:
        now = int(time.time() * 1000)
        try:
            if STREAM_RETENTION_MS_1M > 0:
                r.xtrim(s1, minid=now - STREAM_RETENTION_MS_1M, approximate=True)
            if STREAM_RETENTION_MS_2M > 0:
                r.xtrim(s2, minid=now - STREAM_RETENTION_MS_2M, approximate=True)
        except Exception as e:
            LOG.warning("[%s] trim error: %s", sym, e)
        await asyncio.sleep(60)  # run once a minute

async def _run_symbol(sym: str):
    LOG.info("Starting 1m listener for %s", sym)
    agg = TwoMinuteAggregator(sym)

    # launch optional time-based trimmer
    asyncio.create_task(_trim_task(sym))

    async def on_msg(sym: str, msg: Dict[str, Any]):
        row = normalize_closed_kline_1m(msg)
        if not row:
            return

        ts = int(row["ts"])

        if LOG_1M:
            LOG.info(
                "[%s 1m CLOSED] ts=%s (%s, %s) o=%s h=%s l=%s c=%s v=%s n=%s",
                sym, ts, _iso(ts), _parity(ts),
                row["open"], row["high"], row["low"], row["close"], row["volume"], row["trades"]
            )

        # write raw 1m (deduped) with caps + explicit time ID
        if dedupe_once(dedupe_key(cid_1m(sym, ts))):
            _xadd_with_caps(st_market(sym, "1m"), row, ts_ms=ts, maxlen=STREAM_MAXLEN_1M)

        parity = _parity(ts)
        if parity == "odd" and getattr(agg, "pending_even", None) is None:
            LOG.debug("[%s] odd %s with no pending even → skipping", sym, _iso(ts))
        if parity == "even":
            LOG.debug("[%s] starting 2m window at even %s", sym, _iso(ts))

        c1 = OneMinute(
            ts=ts,
            open=_f(row["open"]),
            high=_f(row["high"]),
            low=_f(row["low"]),
            close=_f(row["close"]),
            volume=_f(row["volume"]),
            trades=int(row["trades"]),
        )
        two = agg.ingest(c1)
        if two:
            ts2 = int(two["ts"])
            if _parity(ts2) != "odd":
                LOG.warning("[%s 2m] close minute not odd (ts=%s %s)", sym, ts2, _iso(ts2))
            if dedupe_once(dedupe_key(cid_2m(sym, ts2))):
                # write 2m with caps + explicit time ID (odd close)
                _xadd_with_caps(st_market(sym, "2m"), two, ts_ms=ts2, maxlen=STREAM_MAXLEN_2M)
                LOG.info(
                    "[%s 2m EMIT] ts=%s (%s, odd) open=%s high=%s low=%s close=%s vol=%s trades=%s",
                    sym, two["ts"], _iso(ts2),
                    two["open"], two["high"], two["low"], two["close"], two["volume"], two["trades"]
                )

    await listen_1m(sym, on_message=on_msg)

async def main():
    ping_redis()
    LOG.info("Connected to Redis.")

    pairs = settings.pairs_1m_list()
    symbols = [sym for sym, tf in pairs if tf.lower() == "1m"]
    if not symbols:
        raise RuntimeError("PAIRS_1M is empty or has no 1m entries")

    tasks = [asyncio.create_task(_run_symbol(sym)) for sym in symbols]

    stop = asyncio.Event()
    def _signal_handler(*_): LOG.warning("Shutdown signal received."); stop.set()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_running_loop().add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            pass

    await stop.wait()
    LOG.info("Cancelling %d tasks...", len(tasks))
    for t in tasks: t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    LOG.info("Ingestor stopped cleanly.")

if __name__ == "__main__":
    asyncio.run(main())
