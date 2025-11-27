from __future__ import annotations

import json
import logging
from decimal import Decimal, InvalidOperation
from urllib.error import URLError, HTTPError
from urllib.request import urlopen
from typing import Dict, List

logger = logging.getLogger(__name__)


def load_tick_sizes(
    symbols: List[str],
    *,
    exchange_info_url: str,
    fallback: Decimal,
) -> Dict[str, Decimal]:
    """
    Fetch tick sizes from Binance exchangeInfo and return a map {SYM: tick_size}.

    - Uses PRICE_FILTER.tickSize when available.
    - Falls back to pricePrecision-derived tick or provided `fallback` when missing/invalid.
    """
    sym_set = {s.upper() for s in symbols}
    ticks: Dict[str, Decimal] = {}

    try:
        with urlopen(exchange_info_url, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except (URLError, HTTPError, TimeoutError, ValueError) as exc:
        logger.warning(
            "Failed to fetch exchangeInfo; using fallback tick sizes | url=%s err=%s",
            exchange_info_url,
            exc,
        )
        return {s: fallback for s in sym_set}

    for entry in data.get("symbols", []) or data.get("data", []):
        sym = str(entry.get("symbol", "")).upper()
        if sym not in sym_set:
            continue

        tick_raw = None
        for f in entry.get("filters", []):
            if f.get("filterType") == "PRICE_FILTER":
                tick_raw = f.get("tickSize")
                break

        tick_val: Decimal | None = None
        if tick_raw not in (None, "", "0"):
            try:
                tick_val = Decimal(str(tick_raw))
            except (InvalidOperation, ValueError):
                tick_val = None

        if tick_val is None or tick_val <= 0:
            # Derive from pricePrecision if present
            pp = entry.get("pricePrecision")
            if pp is not None:
                try:
                    tick_val = Decimal(1).scaleb(-int(pp))
                except (InvalidOperation, ValueError):
                    tick_val = None

        ticks[sym] = tick_val if tick_val and tick_val > 0 else fallback

    # Ensure all requested symbols have an entry
    for sym in sym_set:
        ticks.setdefault(sym, fallback)

    logger.info(
        "Loaded tick sizes from exchangeInfo | url=%s map=%s fallback=%s",
        exchange_info_url,
        ticks,
        fallback,
    )
    return ticks
