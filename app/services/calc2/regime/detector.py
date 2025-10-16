from __future__ import annotations
from typing import Literal, Optional
from decimal import Decimal
import logging

logger = logging.getLogger(__name__)

Regime = Literal["long", "short", "neutral"]

class RegimeDetector:
    """Detects regime from moving averages.

    Decision rule:
    - neutral if any of ma20, ma200, or indicator_close are missing
    - long if ma20 > ma200 and indicator_close > ma20
    - short if ma20 < ma200 and indicator_close < ma20
    - neutral otherwise
    """

    def __init__(self) -> None:
        self._last_regime: Optional[Regime] = None
        logger.info("RegimeDetector initialized")

    def decide(self, ma20: Optional[Decimal], ma200: Optional[Decimal], indicator_close: Optional[Decimal]) -> Regime:
        if ma20 is None or ma200 is None or indicator_close is None:
            logger.debug("Regime neutral (missing data) | ma20=%s ma200=%s close=%s", 
                        ma20, ma200, indicator_close)
            regime = "neutral"
        elif ma20 > ma200 and indicator_close > ma20:
            logger.debug("Regime long | ma20=%s > ma200=%s and close=%s > ma20", 
                        ma20, ma200, indicator_close)
            regime = "long"
        elif ma20 < ma200 and indicator_close < ma20:
            logger.debug("Regime short | ma20=%s < ma200=%s and close=%s < ma20", 
                        ma20, ma200, indicator_close)
            regime = "short"
        else:
            logger.debug("Regime neutral (no clear trend) | ma20=%s ma200=%s close=%s", 
                        ma20, ma200, indicator_close)
            regime = "neutral"
        
        if self._last_regime is not None and self._last_regime != regime:
            logger.info("Regime change detected | %s -> %s | ma20=%s ma200=%s close=%s", 
                       self._last_regime, regime, ma20, ma200, indicator_close)
        
        self._last_regime = regime
        return regime