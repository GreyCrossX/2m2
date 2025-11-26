from __future__ import annotations
import logging
import os

_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


def setup_logging() -> None:
    fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    logging.basicConfig(level=_LEVEL, format=fmt)
