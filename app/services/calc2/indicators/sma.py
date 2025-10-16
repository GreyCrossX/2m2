from collections import deque
from decimal import Decimal
from typing import Optional

class SMA:
    """Rolling Simple Moving Average."""

    def __init__(self, window: int) -> None:
        if window <= 0:
            raise ValueError("window must be > 0")
        self.window = window
        self.buffer: deque[Decimal] = deque(maxlen=window)
        self._sum = Decimal("0")

    def update(self, value: Decimal) -> Optional[Decimal]:
        if len(self.buffer) == self.buffer.maxlen:
            self._sum -= self.buffer[0]
        self.buffer.append(value)
        self._sum += value
        if len(self.buffer) < self.window:
            return None
        return self._sum / Decimal(str(self.window))