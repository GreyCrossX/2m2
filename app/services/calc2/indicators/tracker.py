from __future__ import annotations
from collections import deque
from typing import Deque
from ..models import Candle




class IndicatorTracker:
    """Tracks recent candle colors (for optional heuristics or diagnostics)."""


    def __init__(self, capacity: int = 10) -> None:
        self.capacity = capacity
        self.colors: Deque[str] = deque(maxlen=capacity)


    def on_candle(self, c: Candle) -> None:
        self.colors.append(c.color)

    def streak(self) -> int:
        if not self.colors:
            return 0
        last = self.colors[-1]
        i = len(self.colors) - 2
        s = 1
        while i >= 0 and self.colors[i] == last:
            s += 1
            i -= 1
        return s