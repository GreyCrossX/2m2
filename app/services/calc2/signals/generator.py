from __future__ import annotations
from dataclasses import dataclass
from decimal import Decimal
from typing import List, Optional, Literal
import logging

from ..models import ArmSignal, DisarmSignal

logger = logging.getLogger(__name__)

Regime = Literal["long", "short", "neutral"]


@dataclass
class SignalGenerator:
    tick_size: Decimal
    tick_sizes: dict[str, Decimal] | None = None
    version: str = "1"

    def __post_init__(self) -> None:
        self._prev_regime: Optional[Regime] = None
        self._active_arm: Optional[ArmSignal] = None
        logger.info(
            "SignalGenerator initialized | tick_size=%s version=%s",
            self.tick_size,
            self.version,
        )

    def _tick_for(self, sym: str) -> Decimal:
        """Return the tick size for a symbol, falling back to default."""
        if self.tick_sizes:
            t = self.tick_sizes.get(sym.upper())
            if t is not None and t > 0:
                return t
        return self.tick_size

    # NEW: helper to build ARM signals (keeps logic in one place)
    def _arm(
        self,
        *,
        sym: str,
        tf: str,
        now_ts: int,
        side: Literal["long", "short"],
        ind_ts: int,
        ind_high: Decimal,
        ind_low: Decimal,
    ) -> ArmSignal:
        tick = self._tick_for(sym)
        if side == "long":
            trigger = ind_high + tick
            stop = ind_low - tick
            logger.info(
                "ARM LONG signal | sym=%s ts=%d trigger=%s stop=%s ind_high=%s ind_low=%s",
                sym,
                now_ts,
                trigger,
                stop,
                ind_high,
                ind_low,
            )
        else:
            trigger = ind_low - tick
            stop = ind_high + tick
            logger.info(
                "ARM SHORT signal | sym=%s ts=%d trigger=%s stop=%s ind_high=%s ind_low=%s",
                sym,
                now_ts,
                trigger,
                stop,
                ind_high,
                ind_low,
            )

        return ArmSignal(
            v=self.version,
            type="arm",
            side=side,
            sym=sym,
            tf=tf,
            ts=now_ts,
            ind_ts=ind_ts,
            ind_high=ind_high,
            ind_low=ind_low,
            trigger=trigger,
            stop=stop,
        )

    # NEW: helper to build DISARM signals (shared by neutral transitions and flips)
    def _disarm(
        self,
        *,
        sym: str,
        tf: str,
        now_ts: int,
        prev_side: Literal["long", "short"],
        new_regime: Regime,
        reason: str,
    ) -> DisarmSignal:
        logger.info(
            "DISARM signal | sym=%s ts=%d prev_side=%s new_regime=%s reason=%s",
            sym,
            now_ts,
            prev_side,
            new_regime,
            reason,
        )
        return DisarmSignal(
            v=self.version,
            type="disarm",
            prev_side=prev_side,
            sym=sym,
            tf=tf,
            ts=now_ts,
            reason=reason,
        )

    # NEW: list-returning API that can emit 0, 1, or 2 signals (DISARM then ARM on flip)
    def maybe_signals(
        self,
        *,
        sym: str,
        tf: str,
        now_ts: int,
        regime: Regime,
        ind_ts: int,
        ind_high: Decimal,
        ind_low: Decimal,
    ) -> List[ArmSignal | DisarmSignal]:
        prev = self._prev_regime
        logger.debug(
            "Evaluating signals | sym=%s ts=%d prev_regime=%s current_regime=%s",
            sym,
            now_ts,
            prev,
            regime,
        )

        out: List[ArmSignal | DisarmSignal] = []

        # First data point: just initialize state
        if prev is None:
            self._prev_regime = regime
            logger.debug("No signal (first candle) | sym=%s regime=%s", sym, regime)
            return out

        # No change -> no signal unless a new indicator candle arrives for an active ARM
        if regime == prev:
            if (
                regime in ("long", "short")
                and self._active_arm
                and self._active_arm.side == regime
            ):
                if ind_ts != self._active_arm.ind_ts:
                    logger.info(
                        "Indicator candle update detected | sym=%s tf=%s prev_ind_ts=%s new_ind_ts=%s side=%s",
                        sym,
                        tf,
                        self._active_arm.ind_ts,
                        ind_ts,
                        regime,
                    )
                    out.append(
                        self._disarm(
                            sym=sym,
                            tf=tf,
                            now_ts=now_ts,
                            prev_side=self._active_arm.side,
                            new_regime=regime,
                            reason="update_pending",
                        )
                    )
                    new_arm = self._arm(
                        sym=sym,
                        tf=tf,
                        now_ts=now_ts,
                        side=regime,
                        ind_ts=ind_ts,
                        ind_high=ind_high,
                        ind_low=ind_low,
                    )
                    out.append(new_arm)
                    self._active_arm = new_arm
                else:
                    logger.debug(
                        "No signal (same regime/indicator) | sym=%s regime=%s ind_ts=%s",
                        sym,
                        regime,
                        ind_ts,
                    )
            else:
                logger.debug(
                    "No signal (same regime, no active ARM) | sym=%s regime=%s",
                    sym,
                    regime,
                )
            self._prev_regime = regime
            return out

        # neutral -> long/short: ARM
        if prev == "neutral" and regime in ("long", "short"):
            new_arm = self._arm(
                sym=sym,
                tf=tf,
                now_ts=now_ts,
                side=regime,
                ind_ts=ind_ts,
                ind_high=ind_high,
                ind_low=ind_low,
            )
            self._active_arm = new_arm
            out.append(new_arm)

        # long/short -> neutral: DISARM
        elif prev in ("long", "short") and regime == "neutral":
            out.append(
                self._disarm(
                    sym=sym,
                    tf=tf,
                    now_ts=now_ts,
                    prev_side=prev,
                    new_regime=regime,
                    reason=f"regime:{prev}->neutral",
                )
            )
            self._active_arm = None

        # NEW: direct flip long <-> short: DISARM(prev) THEN ARM(new)
        elif (
            prev in ("long", "short") and regime in ("long", "short") and prev != regime
        ):
            out.append(
                self._disarm(
                    sym=sym,
                    tf=tf,
                    now_ts=now_ts,
                    prev_side=prev,
                    new_regime=regime,
                    reason=f"flip:{prev}->{regime}",
                )
            )
            new_arm = self._arm(
                sym=sym,
                tf=tf,
                now_ts=now_ts,
                side=regime,
                ind_ts=ind_ts,
                ind_high=ind_high,
                ind_low=ind_low,
            )
            self._active_arm = new_arm
            out.append(new_arm)

        self._prev_regime = regime
        if regime == "neutral" and not out:
            self._active_arm = None
        return out

    # NEW: compatibility shim (optional) — preserves old single-object API if still used elsewhere
    def maybe_signal(
        self,
        *,
        sym: str,
        tf: str,
        now_ts: int,
        regime: Regime,
        ind_ts: int,
        ind_high: Decimal,
        ind_low: Decimal,
    ) -> Optional[object]:
        """Return the last signal of maybe_signals() for backward compatibility."""
        sigs = self.maybe_signals(
            sym=sym,
            tf=tf,
            now_ts=now_ts,
            regime=regime,
            ind_ts=ind_ts,
            ind_high=ind_high,
            ind_low=ind_low,
        )
        return sigs[-1] if sigs else None
