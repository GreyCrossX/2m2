from __future__ import annotations
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional
import logging

from ..models import ArmSignal, DisarmSignal

logger = logging.getLogger(__name__)

@dataclass
class SignalGenerator:
    tick_size: Decimal
    version: str = "1"

    def __post_init__(self) -> None:
        self._prev_regime: Optional[str] = None
        logger.info("SignalGenerator initialized | tick_size=%s version=%s", self.tick_size, self.version)

    def maybe_signal(
        self,
        *,
        sym: str,
        tf: str,
        now_ts: int,
        regime: str,
        ind_ts: int,
        ind_high: Decimal,
        ind_low: Decimal,
    ) -> Optional[object]:
        prev = self._prev_regime
        logger.debug("Evaluating signal | sym=%s ts=%d prev_regime=%s current_regime=%s", 
                    sym, now_ts, prev, regime)
        
        self._prev_regime = regime

        if prev is None:
            logger.debug("No signal (first candle) | sym=%s regime=%s", sym, regime)
            return None

        # ARM signal on regime change to long/short
        if regime in ("long", "short") and regime != prev:
            if regime == "long":
                trigger = ind_high + self.tick_size
                stop = ind_low - self.tick_size
                logger.info("ARM LONG signal | sym=%s ts=%d prev=%s trigger=%s stop=%s ind_high=%s ind_low=%s", 
                           sym, now_ts, prev, trigger, stop, ind_high, ind_low)
                return ArmSignal(
                    v=self.version,
                    type="arm",
                    side="long",
                    sym=sym,
                    tf=tf,
                    ts=now_ts,
                    ind_ts=ind_ts,
                    ind_high=ind_high,
                    ind_low=ind_low,
                    trigger=trigger,
                    stop=stop,
                )
            else:
                trigger = ind_low - self.tick_size
                stop = ind_high + self.tick_size
                logger.info("ARM SHORT signal | sym=%s ts=%d prev=%s trigger=%s stop=%s ind_high=%s ind_low=%s", 
                           sym, now_ts, prev, trigger, stop, ind_high, ind_low)
                return ArmSignal(
                    v=self.version,
                    type="arm",
                    side="short",
                    sym=sym,
                    tf=tf,
                    ts=now_ts,
                    ind_ts=ind_ts,
                    ind_high=ind_high,
                    ind_low=ind_low,
                    trigger=trigger,
                    stop=stop,
                )

        # DISARM signal when leaving long/short
        if prev in ("long", "short") and regime not in (prev,):
            reason = f"regime:{prev}->{regime}"
            logger.info("DISARM signal | sym=%s ts=%d prev_side=%s new_regime=%s reason=%s", 
                       sym, now_ts, prev, regime, reason)
            return DisarmSignal(
                v=self.version,
                type="disarm",
                prev_side=prev,  # type: ignore[arg-type]
                sym=sym,
                tf=tf,
                ts=now_ts,
                reason=reason,
            )

        logger.debug("No signal | sym=%s prev=%s regime=%s", sym, prev, regime)
        return None