# services/calc/indicators.py
from collections import deque
from typing import Deque, Optional

class SMA:
    def __init__(self, period: int):
        self.period = period
        self.buf: Deque[float] = deque(maxlen=period)
        self._sum: float = 0.0

    def update(self, x: float) -> Optional[float]:
        if len(self.buf) == self.period:
            self._sum -= self.buf[0]
        self.buf.append(x)
        self._sum += x
        if len(self.buf) < self.period:
            return None
        return self._sum / self.period

    @property
    def count(self) -> int:
        """How many samples currently in the window (<= period)."""
        return len(self.buf)
