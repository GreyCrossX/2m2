from __future__ import annotations
import logging
from decimal import Decimal
from typing import Optional, Tuple

from .sma import SMA
from ..models import Candle

logger = logging.getLogger(__name__)

class IndicatorCandle:
    """Coordinates indicator calculation for each candle"""

    def __init__(self, ma20_window: int = 20, ma200_window: int = 200) -> None:
        if ma20_window <= 0 or ma200_window <= 0:
            raise ValueError("SMA windows must be > 0")
        self.sma20 = SMA(ma20_window)
        self.sma200 = SMA(ma200_window)
        self._last_ts: Optional[int] = None
        self._last_high: Optional[Decimal] = None
        self._last_low: Optional[Decimal] = None
        logger.info("IndicatorCandle initialized | ma20_window=%d ma200_window=%d", ma20_window, ma200_window)

    def on_candle(self, c: Candle) -> Tuple[Optional[Decimal], Optional[Decimal], int, Decimal, Decimal]:
        logger.debug("Processing candle | ts=%d close=%s high=%s low=%s", c.ts, c.close, c.high, c.low)
        
        ma20 = self.sma20.update(c.close)
        ma200 = self.sma200.update(c.close)
        
        self._last_ts = c.ts
        self._last_high = c.high
        self._last_low = c.low
        
        if ma20 is None:
            logger.debug("MA20 not ready yet | buffer_size=%d/%d", len(self.sma20.buffer), self.sma20.window)
        if ma200 is None:
            logger.debug("MA200 not ready yet | buffer_size=%d/%d", len(self.sma200.buffer), self.sma200.window)
        
        if ma20 is not None and ma200 is not None:
            logger.debug("Indicators calculated | ts=%d ma20=%s ma200=%s high=%s low=%s", 
                        c.ts, ma20, ma200, c.high, c.low)
        
        return ma20, ma200, c.ts, c.high, c.low