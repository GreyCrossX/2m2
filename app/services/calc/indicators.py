from collections import deque
from typing import Deque, Optional

class SMA:
    def __init__(self, period: int):
        self.period = period
        self.buf: Deque[float] = deque()
        self._sum: float = 0.0

    def update(self, x: float) -> Optional[float]:
        self.buf.append(x)
        self._sum += x
        if len(self.buf) > self.period:
            self._sum -= self.buf.popleft()
        if len(self.buf) < self.period:
            return None
        return self._sum / self.period

    @property
    def count(self) -> int:
        """How many samples currently in the window (<= period)."""
        return min(len(self.buf), self.period)
