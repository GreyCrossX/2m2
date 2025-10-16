from __future__ import annotations
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from ..models import ArmSignal, DisarmSignal

@dataclass
class SignalGenerator:
    tick_size: Decimal
    version: str = "1"

    def __post_init__(self) -> None:
        self._prev_regime: Optional[str] = None

def maybe_signal(self,*,sym: str,tf: str,now_ts: int,regime: str,ind_ts: int,ind_high: Decimal,ind_low: Decimal,) -> Optional[object]:

    prev = self._prev_regime
    self._prev_regime = regime

    if prev is None:
        return None

    if regime in ("long", "short") and regime != prev:
        if regime == "long":
            trigger = (ind_high + self.tick_size)
            stop = (ind_low - self.tick_size)
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
            trigger = (ind_low - self.tick_size)
            stop = (ind_high + self.tick_size)
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
    
    if prev in ("long", "short") and regime not in (prev,):
        return DisarmSignal(
        v=self.version,
        type="disarm",
        prev_side=prev, # type: ignore[arg-type]
        sym=sym,
        tf=tf,
        ts=now_ts,
        reason=f"regime:{prev}->{regime}",
        )

    return None