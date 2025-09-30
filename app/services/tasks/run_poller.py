from __future__ import annotations

import logging
import sys
from typing import List

from app.config import settings
from app.services.tasks.signal_poller import run_for_symbol


LOG = logging.getLogger("poller")


def _symbols_from_args_or_env(argv: List[str]) -> List[str]:
    if argv:
        return [s.upper() for s in argv]

    # Example: settings.pairs_1m_list() -> List[Tuple[str, str]]
    pairs = settings.pairs_1m_list()
    syms = [sym.upper() for sym, tf in pairs if tf.lower() in ("1m", "2m")]
    return syms


def main() -> None:
    symbols = _symbols_from_args_or_env(sys.argv[1:])
    if not symbols:
        LOG.error("No symbols configured for poller (args empty and pairs_1m_list returned none). Exiting.")
        sys.exit(1)

    sym = symbols[0]  # run single blocking loop; scale via more containers
    LOG.info("Starting signal poller for symbol=%s", sym)
    run_for_symbol(sym)


if __name__ == "__main__":
    # Basic logging config if app didn't set it yet
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    main()
