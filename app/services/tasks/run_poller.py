from __future__ import annotations

import logging
import os
import re
import sys
from typing import List

from app.config import settings
from app.services.tasks.signal_poller import run_for_symbol

LOG = logging.getLogger("poller")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

def _split_syms(s: str) -> List[str]:
    return [x.strip().upper() for x in re.split(r"[,\s;]+", s) if x.strip()]

def _symbols_from_args_or_env(argv: List[str]) -> List[str]:
    # prefer CLI args
    if argv:
        return _split_syms(",".join(argv))
    # then env
    env = os.getenv("POLLER_SYMBOLS", "")
    if env.strip():
        return _split_syms(env)
    # then settings
    pairs = settings.pairs_1m_list()
    return [sym.upper() for sym, tf in pairs if tf.lower() in ("1m", "2m")]

def main() -> None:
    symbols = _symbols_from_args_or_env(sys.argv[1:])
    if not symbols:
        LOG.error("No symbols configured for poller (args/env/settings empty). Exiting.")
        sys.exit(1)

    sym = symbols[0]  # single blocking loop; scale with more containers
    LOG.info("Starting signal poller for symbol=%s", sym)
    run_for_symbol(sym)

if __name__ == "__main__":
    main()
