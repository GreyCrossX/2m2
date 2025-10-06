# services/ingestor/backfill.py
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List, Tuple
from urllib.parse import urlencode
from urllib.request import urlopen, Request

from .redis_io import r, dedupe_once
from .keys import st_market, cid_1m, cid_2m, dedupe_key
from .aggregator import TwoMinuteAggregator, OneMinute

LOG = logging.getLogger("ingestor.backfill")

# Stream caps (length-based)
STREAM_MAXLEN_1M = int(os.getenv("STREAM_MAXLEN_1M", "5000"))
STREAM_MAXLEN_2M = int(os.getenv("STREAM_MAXLEN_2M", "5000"))

# Binance market -> REST base
def _rest_base() -> str:
    market = (os.getenv("BINANCE_MARKET") or "um_futures").lower()
    # USDⓈ-M Futures
    if market in ("um_futures", "um-futures", "futures", "usdm"):
        return "https://fapi.binance.com"
    # COIN-M Futures (not used here, but supported)
    if market in ("cm_futures", "cm-futures", "coinm"):
        return "https://dapi.binance.com"
    # Spot fallback
    return "https://api.binance.com"

def _build_klines_url(symbol: str, interval: str, limit: int, end_ms: int | None = None) -> str:
    base = _rest_base()
    path = "/fapi/v1/klines" if "fapi" in base else "/api/v3/klines"
    q = {"symbol": symbol.upper(), "interval": interval, "limit": int(limit)}
    if end_ms:
        q["endTime"] = int(end_ms)
    return f"{base}{path}?{urlencode(q)}"

def _http_get_json(url: str, timeout: int = 15) -> Any:
    req = Request(url, headers={"User-Agent": "ingestor-backfill/1.0"})
    with urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))

def _xadd_with_caps(stream: str, fields: Dict[str, str], ts_ms: int, maxlen: int) -> str:
    # Use explicit ID "<ts>-0" so time-based trims (MINID) align
    return r.xadd(stream, fields, id=f"{ts_ms}-0", maxlen=maxlen, approximate=True)

def _as_row_from_kl(line: List[Any]) -> Tuple[Dict[str, str], OneMinute]:
    """
    Convert a REST kline array to:
      - 1m stream row (str->str)
      - OneMinute object for 2m aggregation
    Shape per Binance docs:
      [ openTime, open, high, low, close, volume, closeTime, quoteVolume, trades, takerBuyBase, takerBuyQuote, ignore ]
    """
    open_time = int(line[0])
    open_ = float(line[1])
    high = float(line[2])
    low = float(line[3])
    close = float(line[4])
    volume = float(line[5])
    close_time = int(line[6])
    trades = int(line[8])

    row = {
        "ts": str(close_time),  # we treat the bar timestamp as close ms
        "open": f"{open_}",
        "high": f"{high}",
        "low": f"{low}",
        "close": f"{close}",
        "volume": f"{volume}",
        "trades": str(trades),
        # color is optional for downstream; include for completeness
        "color": "green" if close >= open_ else "red",
    }

    one = OneMinute(
        ts=close_time,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=volume,
        trades=trades,
    )
    return row, one

async def backfill_symbol(
    sym: str,
    *,
    min_two_min: int = 150,
    one_min_limit: int = 500,
    logger: logging.Logger | None = None,
) -> Dict[str, Any]:
    """
    Fetch recent 1m klines via REST, write them into:
      - stream:market|{SYMBOL|1m}   (deduped, capped)
      - stream:market|{SYMBOL|2m}   (via TwoMinuteAggregator)

    Returns: {"ok": True, "wrote_1m": N1, "wrote_2m": N2}
    """
    log = logger or LOG
    try:
        need_1m = max(2 * int(min_two_min), int(one_min_limit))
        end_ms = int(time.time() * 1000)
        url = _build_klines_url(sym, "1m", limit=need_1m, end_ms=end_ms)
        data = _http_get_json(url)

        if not isinstance(data, list) or not data:
            return {"ok": False, "error": "empty_or_bad_response"}

        # Ensure ascending order (Binance returns ascending, but be safe)
        klines: List[List[Any]] = sorted(data, key=lambda x: int(x[6]))

        s1 = st_market(sym, "1m")
        s2 = st_market(sym, "2m")
        agg = TwoMinuteAggregator(sym)

        wrote_1m = 0
        wrote_2m = 0

        for arr in klines:
            row_1m, one = _as_row_from_kl(arr)
            ts = int(row_1m["ts"])

            # 1m write (deduplicated)
            if dedupe_once(dedupe_key(cid_1m(sym, ts))):
                _xadd_with_caps(s1, row_1m, ts_ms=ts, maxlen=STREAM_MAXLEN_1M)
                wrote_1m += 1

            # Feed aggregator for 2m
            two = agg.ingest(one)
            if two:
                ts2 = int(two["ts"])
                if dedupe_once(dedupe_key(cid_2m(sym, ts2))):
                    # include optional color for 2m row
                    try:
                        o = float(two["open"]); c = float(two["close"])
                        two["color"] = "green" if c >= o else "red"
                    except Exception:
                        pass
                    _xadd_with_caps(s2, two, ts_ms=ts2, maxlen=STREAM_MAXLEN_2M)
                    wrote_2m += 1

        log.info("[backfill %s] wrote %d x 1m and %d x 2m", sym, wrote_1m, wrote_2m)
        return {"ok": True, "wrote_1m": wrote_1m, "wrote_2m": wrote_2m}

    except Exception as e:
        log.error("[backfill %s] failed: %s", sym, e)
        return {"ok": False, "error": str(e)}
