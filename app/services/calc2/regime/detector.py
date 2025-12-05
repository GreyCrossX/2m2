from __future__ import annotations
from typing import Literal, Optional
from decimal import Decimal
import logging

logger = logging.getLogger(__name__)

Regime = Literal["long", "short", "neutral"]


class RegimeDetector:
    """Detect regimes strictly on MA20/MA200 crossovers.

    Behaviour aligned to the trading rules:
    - Stay neutral until we have observed a *crossover*.
    - Bullish crossover (MA20 crosses above MA200) -> regime=long
    - Bearish crossover (MA20 crosses below MA200) -> regime=short
    - Maintain the last non-neutral regime until the opposite crossover occurs.
    """

    def __init__(self) -> None:
        self._last_regime: Optional[Regime] = None
        self._last_sign: Optional[int] = None  # sign of (ma20 - ma200)
        logger.info("RegimeDetector initialized")

    def decide(
        self,
        *,
        ma20: Optional[Decimal],
        ma200: Optional[Decimal],
        close_for_long: Decimal,  # retained for interface compatibility/logging
        close_for_short: Decimal,  # retained for interface compatibility/logging
        ts: int | None = None,
    ) -> Regime:
        if ma20 is None or ma200 is None:
            logger.debug(
                "Regime neutral (missing data) | ma20=%s ma200=%s", ma20, ma200
            )
            self._last_sign = None
            self._last_regime = "neutral"
            return "neutral"

        diff = ma20 - ma200
        sign = 1 if diff > 0 else -1 if diff < 0 else 0

        # First ready datapoint: seed sign, stay neutral until we see a flip.
        if self._last_sign is None:
            self._last_sign = sign
            logger.debug(
                "Regime neutral (warming, awaiting first crossover) | ma20=%s ma200=%s sign=%d",
                ma20,
                ma200,
                sign,
            )
            return self._last_regime or "neutral"

        # Default to previous regime until checks below
        regime = self._last_regime or "neutral"

        if sign == 0:
            logger.debug(
                "Regime unchanged (MAs equal) | ma20=%s ma200=%s last_regime=%s",
                ma20,
                ma200,
                regime,
            )
            self._last_sign = sign
            self._last_regime = regime
            return regime

        if sign != self._last_sign:
            logger.info(
                "Crossover detected | ts=%s sign %d -> %d | ma20=%s ma200=%s",
                ts if ts is not None else "-",
                self._last_sign,
                sign,
                ma20,
                ma200,
            )

        candidate = "long" if sign > 0 else "short"

        # Additional price-action gate: only enter trend if price is on the correct side of BOTH MAs
        if candidate == "long":
            if close_for_long < ma20 and close_for_long < ma200:
                regime = "long"
            else:
                logger.debug(
                    "Regime gated to neutral (price above MA) | close_for_long=%s ma20=%s ma200=%s",
                    close_for_long,
                    ma20,
                    ma200,
                )
                regime = "neutral"
        else:
            if close_for_short > ma20 and close_for_short > ma200:
                regime = "short"
            else:
                logger.debug(
                    "Regime gated to neutral (price below MA) | close_for_short=%s ma20=%s ma200=%s",
                    close_for_short,
                    ma20,
                    ma200,
                )
                regime = "neutral"

        self._last_sign = sign
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
