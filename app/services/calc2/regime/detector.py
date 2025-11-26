from __future__ import annotations
from typing import Literal, Optional
from decimal import Decimal
import logging

logger = logging.getLogger(__name__)

Regime = Literal["long", "short", "neutral"]


class RegimeDetector:
    """Detects regime from moving averages.

    Decision rule:
    - neutral if ma20 or ma200 are missing
    - long  if (ma20 > ma200) and (close_for_long  > ma20)   # close_for_long  := last RED close (fallback applied by caller)
    - short if (ma20 < ma200) and (close_for_short < ma20)   # close_for_short := last GREEN close (fallback applied by caller)
    - neutral otherwise
    """

    def __init__(self) -> None:
        self._last_regime: Optional[Regime] = None
        logger.info("RegimeDetector initialized")

    def decide(
        self,
        *,
        ma20: Optional[Decimal],
        ma200: Optional[Decimal],
        close_for_long: Decimal,  # last red close (caller must provide fallback)
        close_for_short: Decimal,  # last green close (caller must provide fallback)
    ) -> Regime:
        if ma20 is None or ma200 is None:
            logger.debug(
                "Regime neutral (missing data) | ma20=%s ma200=%s", ma20, ma200
            )
            regime: Regime = "neutral"
        elif (ma20 > ma200) and (close_for_long > ma20):
            logger.debug(
                "Regime long | ma20=%s > ma200=%s and close_for_long=%s > ma20",
                ma20,
                ma200,
                close_for_long,
            )
            regime = "long"
        elif (ma20 < ma200) and (close_for_short < ma20):
            logger.debug(
                "Regime short | ma20=%s < ma200=%s and close_for_short=%s < ma20",
                ma20,
                ma200,
                close_for_short,
            )
            regime = "short"
        else:
            logger.debug(
                "Regime neutral (no clear trend) | ma20=%s ma200=%s close_for_long=%s close_for_short=%s",
                ma20,
                ma200,
                close_for_long,
                close_for_short,
            )
            regime = "neutral"

        if (self._last_regime is not None) and (self._last_regime != regime):
            logger.info(
                "Regime change detected | %s -> %s | ma20=%s ma200=%s close_for_long=%s close_for_short=%s",
                self._last_regime,
                regime,
                ma20,
                ma200,
                close_for_long,
                close_for_short,
            )

        self._last_regime = regime
        return regime
