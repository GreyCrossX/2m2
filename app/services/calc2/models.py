"""
Data models for the calc service.
All data transfer objects and domain models.
"""
from dataclasses import dataclass
from enum import Enum
from decimal import Decimal
from typing import Optional, Literal, Dict, Any

Color = Literal["green", "red"]
Regime = Literal["long", "short"]
SignalType = Literal["arm", "disarm"]

@dataclass(frozen=True)
class Candle:
    """Market candle data."""
    ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    trades: int
    color: Color
    
    @staticmethod
    def from_msg(msg: Dict[str, Any]) -> "Candle":
    # All fields arrive as strings per input schema
        return Candle(
        ts=int(msg["ts"]),
        sym=str(msg["sym"]),
        tf=str(msg["tf"]),
        open=Decimal(str(msg["open"])),
        high=Decimal(str(msg["high"])),
        low=Decimal(str(msg["low"])),
        close=Decimal(str(msg["close"])),
        volume=Decimal(str(msg.get("volume", "0"))),
        trades=int(msg.get("trades", 0)),
        color=("green" if str(msg.get("color", "")).lower() == "green" else "red"),
    )


@dataclass(frozen=True)
class IndicatorState:
    """Simplified candle for indicator tracking."""
    v: str
    sym: str
    tf: str
    ts: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    color: Color
    ma20: Optional[Decimal]
    ma200: Optional[Decimal]
    regime: Regime
    ind_ts: int
    ind_high: Decimal
    ind_low: Decimal

    def to_stream_map(self) -> Dict[str, str]:
        def _fmt(x):
            return f"{x}" if x is not None else ""
        return {
            "v": self.v,
            "sym": self.sym,
            "tf": self.tf,
            "ts": str(self.ts),
            "open": _fmt(self.open),
            "high": _fmt(self.high),
            "low": _fmt(self.low),
            "close": _fmt(self.close),
            "color": self.color,
            "ma20": _fmt(self.ma20),
            "ma200": _fmt(self.ma200),
            "regime": self.regime,
            "ind_ts": str(self.ind_ts),
            "ind_high": _fmt(self.ind_high),
            "ind_low": _fmt(self.ind_low),
        }


@dataclass(frozen=True)
class ArmSignal:
    v:str
    type: SignalType
    side: Regime
    sym: str
    tf: str
    ts: int
    ind_ts: int
    ind_high: Decimal
    ind_low: Decimal
    trigger: Decimal
    stop: Decimal

    def _to_stream_map(self) -> Dict[str, str]:
        return {
            "v": self.v,
            "type": self.type,
            "side": self.side,
            "sym": self.sym,
            "tf": self.tf,
            "ts": str(self.ts),
            "ind_ts": str(self.ind_ts),
            "ind_high": f"{self.ind_high}",
            "ind_low": f"{self.ind_low}",
            "trigger": f"{self.trigger}",
            "stop": f"{self.stop}",
        }
    
@dataclass(frozen=True)
class DisarmSignal:
    v: str
    type: Literal["disarm"]
    prev_side: Literal["long", "short"]
    sym: str
    tf: str
    ts: int
    reason: str


    def to_stream_map(self) -> Dict[str, str]:
        return {
            "v": self.v,
            "type": self.type,
            "prev_side": self.prev_side,
            "sym": self.sym,
            "tf": self.tf,
            "ts": str(self.ts),
            "reason": self.reason,
        }