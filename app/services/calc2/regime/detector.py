from __future__ import annotations
from typing import Literal, Optional
from decimal import Decimal


Regime = Literal["long", "short", "neutral"]


class RegimeDetector:
    """Detects regime from moving averages.


    Decision rule (your updated logic):
    - neutral if any of ma20, ma200, or indicator_close are missing
    - long if ma20 > ma200 and indicator_close > ma20
    - short if ma20 < ma200 and indicator_close < ma20
    - neutral otherwise
    """


    def decide(self,ma20: Optional[Decimal],ma200: Optional[Decimal], indicator_close: Optional[Decimal]) -> Regime:
        if ma20 is None or ma200 is None or indicator_close is None:
            return "neutral"
        if ma20 > ma200 and indicator_close > ma20:
            return "long"
        if ma20 < ma200 and indicator_close < ma20:
            return "short"
        return "neutral"