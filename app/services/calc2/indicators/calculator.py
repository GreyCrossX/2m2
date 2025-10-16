from __future__ import annotations
from decimal import Decimal
from typing import Optional, Tuple


from .sma import SMA
from ..models import Candle

class IndicatorCandle:
    "Coordinates indicator calculation for each candle"

    def __init__(self, ma20_window: int = 20, ma200_window: int = 200) -> None:
        if ma20_window <= 0 or ma200_window <= 0:
            raise ValueError("SMA windows must be > 0")
        self.sma20 = SMA(ma20_window)
        self.sma200 = SMA(ma200_window)
        self._last_ts: Optional[int] = None
        self._last_high: Optional[Decimal] = None
        self._last_low: Optional[Decimal] = None


    def on_candle(self, c: Candle) -> Tuple[Optional[Decimal], Optional[Decimal], int, Decimal, Decimal]:
        ma20 = self.sma20.update(c.close)
        ma200 = self.sma200.update(c.close)
        self._last_ts = c.ts
        self._last_high = c.high
        self._last_low = c.low
        return ma20, ma200, c.ts, c.high, c.low