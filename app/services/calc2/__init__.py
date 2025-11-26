# ruff: noqa: E402, F401
# app/services/calc/__init__.py
"""Calc service - Trading signal calculator."""

from .main import main

__all__ = ["main"]


# app/services/calc/indicators/__init__.py
"""Indicator calculation components."""
from .indicators.sma import SMA
from .indicators.calculator import IndicatorCandle
from .indicators.tracker import IndicatorTracker

__all__ += ["SMA", "IndicatorCandle", "IndicatorTracker"]


# app/services/calc/regime/__init__.py
"""Regime detection components."""
from .regime.detector import RegimeDetector

__all__ += ["RegimeDetector"]


# app/services/calc/signals/__init__.py
"""Signal generation components."""
from .signals.generator import SignalGenerator

__all__ += ["SignalGenerator"]


# app/services/calc/streams/__init__.py
"""Stream I/O components."""
from .streams.consumer import StreamConsumer
from .streams.publisher import StreamPublisher

__all__ += ["StreamConsumer", "StreamPublisher"]


# app/services/calc/processors/__init__.py
"""Processor components."""
from .processors.symbol_processor import SymbolProcessor

__all__ += ["SymbolProcessor"]


# app/services/calc/utils/__init__.py
"""Utility components."""
from .utils.keys import st_ind, st_market, st_signal

__all__ += ["st_ind", "st_market", "st_signal"]
