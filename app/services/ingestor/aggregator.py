from dataclasses import dataclass
from typing import Optional


@dataclass
class OneMinute:
    ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    trades: int


def _is_even(ts_ms: int) -> bool:
    return ((ts_ms // 60000) % 60) % 2 == 0


class TwoMinuteAggregator:
    def __init__(self, sym: str):
        self.sym = sym
        self.pending_even: Optional[OneMinute] = None

    def ingest(self, c1: OneMinute) -> dict | None:
        if _is_even(c1.ts):
            self.pending_even = c1
            return None
        if self.pending_even is None:
            return None
        e = self.pending_even
        o = c1
        two = {
            "v": "1",
            "ts": str(o.ts),
            "sym": self.sym,
            "tf": "2m",
            "open": f"{e.open}",
            "high": f"{max(e.high, o.high)}",
            "low": f"{min(e.low, o.low)}",
            "close": f"{o.close}",
            "volume": f"{e.volume + o.volume}",
            "trades": f"{e.trades + o.trades}",
            "color": "green" if o.close >= e.open else "red",
            "src": "binance-agg-1m->2m",
        }
        self.pending_even = None
        return two
