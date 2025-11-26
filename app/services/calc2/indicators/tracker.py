from __future__ import annotations
from collections import deque
from typing import Deque
import logging
from ..models import Candle

logger = logging.getLogger(__name__)


class IndicatorTracker:
    """Tracks recent candle colors (for optional heuristics or diagnostics)."""

    def __init__(self, capacity: int = 10) -> None:
        self.capacity = capacity
        self.colors: Deque[str] = deque(maxlen=capacity)
        logger.info("IndicatorTracker initialized | capacity=%d", capacity)

    def on_candle(self, c: Candle) -> None:
        self.colors.append(c.color)
        streak = self.streak()
        logger.debug(
            "Candle color tracked | ts=%d color=%s streak=%d total_tracked=%d",
            c.ts,
            c.color,
            streak,
            len(self.colors),
        )

    def streak(self) -> int:
        if not self.colors:
            return 0
        last = self.colors[-1]
        i = len(self.colors) - 2
        s = 1
        while i >= 0 and self.colors[i] == last:
            s += 1
            i -= 1

        if s >= 3:
            logger.debug("Notable streak detected | color=%s length=%d", last, s)

        return s
