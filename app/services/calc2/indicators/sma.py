from collections import deque
from decimal import Decimal
from typing import Optional
import logging

logger = logging.getLogger(__name__)

class SMA:
    """Rolling Simple Moving Average."""

    def __init__(self, window: int) -> None:
        if window <= 0:
            raise ValueError("window must be > 0")
        self.window = window
        self.buffer: deque[Decimal] = deque(maxlen=window)
        self._sum = Decimal("0")
        logger.debug("SMA initialized | window=%d", window)

    def update(self, value: Decimal) -> Optional[Decimal]:
        old_size = len(self.buffer)
        
        if len(self.buffer) == self.buffer.maxlen:
            removed = self.buffer[0]
            self._sum -= removed
            logger.debug("SMA buffer full, removing oldest | removed=%s", removed)
        
        self.buffer.append(value)
        self._sum += value
        
        if len(self.buffer) < self.window:
            logger.debug("SMA warming up | size=%d/%d value=%s", len(self.buffer), self.window, value)
            return None
        
        result = self._sum / Decimal(str(self.window))
        
        if old_size < self.window and len(self.buffer) == self.window:
            logger.info("SMA ready | window=%d initial_value=%s", self.window, result)
        
        return result