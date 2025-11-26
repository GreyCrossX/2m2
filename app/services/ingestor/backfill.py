# services/ingestor/backfill.py
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List, Tuple, cast
from urllib.parse import urlencode
from urllib.request import urlopen, Request

from .redis_io import r, dedupe_once
from .keys import st_market, cid_1m, cid_2m, dedupe_key
from .aggregator import TwoMinuteAggregator, OneMinute

LOG = logging.getLogger("ingestor.backfill")

# Stream caps (length-based)
STREAM_MAXLEN_1M = int(os.getenv("STREAM_MAXLEN_1M", "5000"))
STREAM_MAXLEN_2M = int(os.getenv("STREAM_MAXLEN_2M", "5000"))

# ───────────────────────── helpers ─────────────────────────


def _rest_base() -> str:
    market = (os.getenv("BINANCE_MARKET") or "um_futures").lower()
    if market in ("um_futures", "um-futures", "futures", "usdm"):
        return "https://fapi.binance.com"  # USDⓈ-M Futures
    if market in ("cm_futures", "cm-futures", "coinm"):
        return "https://dapi.binance.com"  # COIN-M (unused here)
    return "https://api.binance.com"  # spot fallback


def _build_klines_url(
    symbol: str, interval: str, limit: int, end_ms: int | None = None
) -> str:
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


def _last_stream_ts(stream: str) -> int | None:
    """
    Return the numeric ms timestamp (XADD ID prefix) of the newest entry,
    or None if the stream is empty or on error.
    """
    try:
        last = cast(list, r.xrevrange(stream, count=1))
        if last:
            last_id, _ = last[0]
            return int(last_id.split("-")[0])
    except Exception as e:
        LOG.warning("xrevrange(%s) failed: %s", stream, e)
    return None


def _xadd_with_caps(
    stream: str, fields: Dict[str, str], ts_ms: int, maxlen: int
) -> str:
    # explicit ID "<ts>-0" so MINID trims align with our bar timestamps
    fields_cast = cast(dict, fields)
    return str(
        r.xadd(
            stream,
            fields_cast,
            id=f"{ts_ms}-0",
            maxlen=maxlen,
            approximate=True,
        )
    )  # type: ignore[arg-type]


def _as_row_from_kl(line: List[Any]) -> Tuple[Dict[str, str], OneMinute]:
    """
    Convert a Binance kline array to:
      - 1m stream row (str->str)
      - OneMinute object for 2m aggregation

    Kline shape:
      [ openTime, open, high, low, close, volume,
        closeTime, quoteVolume, trades, takerBuyBase, takerBuyQuote, ignore ]
    """
    open_ = float(line[1])
    high = float(line[2])
    low = float(line[3])
    close = float(line[4])
    volume = float(line[5])
    close_time = int(line[6])
    trades = int(line[8])

    row = {
        "ts": str(close_time),  # we treat bar ts as close ms
        "open": f"{open_}",
        "high": f"{high}",
        "low": f"{low}",
        "close": f"{close}",
        "volume": f"{volume}",
        "trades": str(trades),
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


# ───────────────────────── main ─────────────────────────


async def backfill_symbol(
    sym: str,
    *,
    min_two_min: int = 150,
    one_min_limit: int = 500,
    logger: logging.Logger | None = None,
) -> Dict[str, Any]:
    """
    Fetch recent 1m klines via REST, write them into:
      - stream:market|{SYMBOL:1m}   (deduped, forward-only)
      - stream:market|{SYMBOL:2m}   (via TwoMinuteAggregator, forward-only)

    Returns: {"ok": True, "wrote_1m": N1, "wrote_2m": N2, "skipped_1m": K1, "skipped_2m": K2}
    """
    log = logger or LOG
    try:
        need_1m = max(2 * int(min_two_min), int(one_min_limit))
        end_ms = int(time.time() * 1000)
        url = _build_klines_url(sym, "1m", limit=need_1m, end_ms=end_ms)
        data = _http_get_json(url)
        if not isinstance(data, list) or not data:
            return {"ok": False, "error": "empty_or_bad_response"}

        # Ensure ascending by closeTime (Binance already does, but be explicit)
        klines: List[List[Any]] = sorted(data, key=lambda x: int(x[6]))

        s1 = st_market(sym, "1m")
        s2 = st_market(sym, "2m")

        # Get current stream tops (important for forward-only writes)
        last1 = _last_stream_ts(s1)
        last2 = _last_stream_ts(s2)

        agg = TwoMinuteAggregator(sym)

        wrote_1m = 0
        wrote_2m = 0
        skipped_1m = 0
        skipped_2m = 0

        for arr in klines:
            row_1m, one = _as_row_from_kl(arr)
            ts1 = int(row_1m["ts"])

            # ── 1m forward-only ──
            # If the stream already has newer/equal data, skip to avoid XADD ID error.
            if last1 is not None and ts1 <= last1:
                skipped_1m += 1
            else:
                if dedupe_once(dedupe_key(cid_1m(sym, ts1))):
                    _xadd_with_caps(s1, row_1m, ts_ms=ts1, maxlen=STREAM_MAXLEN_1M)
                    wrote_1m += 1
                    last1 = ts1  # advance watermark as we go (keeps loop O(1))

            # ── feed 2m aggregator ──
            two = agg.ingest(one)
            if not two:
                continue

            ts2 = int(two["ts"])

            # 2m forward-only guard
            if last2 is not None and ts2 <= last2:
                skipped_2m += 1
                continue

            # include optional color for 2m
            try:
                o = float(two["open"])
                c = float(two["close"])
                two["color"] = "green" if c >= o else "red"
            except Exception:
                pass

            if dedupe_once(dedupe_key(cid_2m(sym, ts2))):
                _xadd_with_caps(s2, two, ts_ms=ts2, maxlen=STREAM_MAXLEN_2M)
                wrote_2m += 1
                last2 = ts2  # advance watermark

        log.info(
            "[backfill %s] wrote %d x 1m (+%d skipped) and %d x 2m (+%d skipped)",
            sym,
            wrote_1m,
            skipped_1m,
            wrote_2m,
            skipped_2m,
        )

        return {
            "ok": True,
            "wrote_1m": wrote_1m,
            "wrote_2m": wrote_2m,
            "skipped_1m": skipped_1m,
            "skipped_2m": skipped_2m,
        }

    except Exception as e:
        log.error("[backfill %s] failed: %s", sym, e)
        return {"ok": False, "error": str(e)}
