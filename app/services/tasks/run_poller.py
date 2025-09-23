# app/scripts/run_poller.py
from __future__ import annotations

import os
import sys
from typing import List

from app.config import settings

# Your actual poller implementation:
from app.services.tasks.signal_poller import run_for_symbol  # adapt if your function is named differently


def main() -> None:
    # Prefer CLI args like: python -m app.scripts.run_poller BTCUSDT ETHUSDT
    args = sys.argv[1:]
    if args:
        symbols = [s.upper() for s in args]
    else:
        # Fall back to env-configured pairs (PAIRS_1M="BTCUSDT:1m,ETHUSDT:1m")
        pairs = settings.pairs_1m_list()
        symbols = [sym for sym, tf in pairs if tf.lower() == "2m" or tf.lower() == "1m"]
        if not symbols:
            symbols = ["BTCUSDT"]  # safe default for tonight

    # Run one process per symbol *inside* the same container (simple serial loop)
    # If your run_symbol is blocking (while True: ...), call it once for the first symbol.
    # For multiple symbols concurrently, you could spawn threads/tasks inside run_poller instead.
    sym = symbols[0]
    print(f"[poller] starting for symbol={sym}")
    run_for_symbol(sym)  # blocking loop

if __name__ == "__main__":
    main()
