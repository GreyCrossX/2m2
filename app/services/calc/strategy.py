from dataclasses import dataclass
from typing import Optional

@dataclass
class Candle:
    ts: int
    open: float
    high: float
    low: float
    close: float
    color: str  # "green" | "red"

@dataclass
class IndicatorCandle:
    ts: int
    high: float
    low: float
    color: str  # "green"/"red"

@dataclass
class CalcResult:
    ma20: Optional[float]
    ma200: Optional[float]
    regime: str                # "long" | "short" | "neutral"
    indicator: Optional[IndicatorCandle]
    trigger_long: Optional[float]
    stop_long: Optional[float]
    trigger_short: Optional[float]
    stop_short: Optional[float]

def choose_regime(close: float, ma20: Optional[float], ma200: Optional[float]) -> str:
    if ma20 is None or ma200 is None:
        return "neutral"
    # LONG regime: price > MA20 and MA20 > MA200 (Price, 20, 200)
    if close > ma20 and ma20 > ma200:
        return "long"
    # SHORT regime: price < MA20 and MA200 < MA20 (200, 20, Price)
    if close < ma20 and ma20 < ma200:
        return "short"
    return "neutral"

class IndicatorState:
    """Keeps the current indicator candle based on regime rules."""
    def __init__(self, price_tick: float = 0.01):
        self.price_tick = price_tick
        self.last_red: Optional[IndicatorCandle] = None
        self.last_green: Optional[IndicatorCandle] = None
        self.regime: str = "neutral"

    def update(self, candle: Candle, regime: str) -> CalcResult:
        self.regime = regime

        # Always track last red and last green candles
        if candle.color == "red":
            self.last_red = IndicatorCandle(ts=candle.ts, high=candle.high, low=candle.low, color="red")
        elif candle.color == "green":
            self.last_green = IndicatorCandle(ts=candle.ts, high=candle.high, low=candle.low, color="green")

        # Set current indicator based on regime
        current_indicator: Optional[IndicatorCandle] = None
        trig_long = stop_long = trig_short = stop_short = None

        if regime == "long":
            # Indicator candle is the most recent CLOSED RED 2m candle.
            current_indicator = self.last_red
            if current_indicator:
                trig_long = current_indicator.high + self.price_tick
                stop_long = current_indicator.low - self.price_tick

        elif regime == "short":
            # Indicator candle is the most recent CLOSED GREEN 2m candle.
            current_indicator = self.last_green
            if current_indicator:
                trig_short = current_indicator.low - self.price_tick
                stop_short = current_indicator.high + self.price_tick

        # In neutral regime, current_indicator stays None but we keep last_red/last_green

        return CalcResult(
            ma20=None, ma200=None, regime=regime,
            indicator=current_indicator,
            trigger_long=trig_long, stop_long=stop_long,
            trigger_short=trig_short, stop_short=stop_short,
        )