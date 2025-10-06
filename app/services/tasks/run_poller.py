from __future__ import annotations

import logging
import os
import sys
import threading
import time
from typing import List

from app.config import settings
from app.services.tasks.signal_poller import run_for_symbol

LOG = logging.getLogger("poller")


def _parse_symbols(tokens: List[str]) -> List[str]:
    """Split on commas/whitespace, uppercase, de-dupe preserving order."""
    out: List[str] = []
    seen = set()
    for tok in tokens:
        if not tok:
            continue
        # allow comma- or space-separated inputs
        parts = []
        for piece in tok.replace(";", ",").split(","):
            parts.extend(piece.strip().split())
        for p in parts:
            sym = p.strip().upper()
            if sym and sym not in seen:
                seen.add(sym)
                out.append(sym)
    return out


def _symbols_from_args_env_settings(argv: List[str]) -> List[str]:
    # 1) CLI args
    syms = _parse_symbols(argv)
    if syms:
        return syms

    # 2) POLLER_SYMBOLS env (e.g. "BTCUSDT,ETHUSDT")
    env_syms = os.getenv("POLLER_SYMBOLS", "")
    syms = _parse_symbols([env_syms]) if env_syms else []
    if syms:
        return syms

    # 3) settings.pairs_1m_list()
    pairs = settings.pairs_1m_list()
    return _parse_symbols([",".join(sym for sym, tf in pairs if tf.lower() in ("1m", "2m"))])


def _run_threaded(symbols: List[str]) -> None:
    LOG.info("Starting signal pollers for symbols=%s", ",".join(symbols))
    threads: List[threading.Thread] = []
    for sym in symbols:
        t = threading.Thread(target=run_for_symbol, args=(sym,), name=f"poller-{sym}", daemon=True)
        t.start()
        threads.append(t)

    # keep the main thread alive while children run
    try:
        while any(t.is_alive() for t in threads):
            time.sleep(1)
    except KeyboardInterrupt:
        LOG.info("Shutdown requested. Exiting.")


def main() -> None:
    symbols = _symbols_from_args_env_settings(sys.argv[1:])
    if not symbols:
        LOG.error("No symbols configured for poller. Exiting.")
        sys.exit(1)

    if len(symbols) == 1:
        sym = symbols[0]
        LOG.info("Starting signal poller for symbol=%s", sym)
        run_for_symbol(sym)
    else:
        _run_threaded(symbols)


if __name__ == "__main__":
    # basic logging if app didn't configure it
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    main()
