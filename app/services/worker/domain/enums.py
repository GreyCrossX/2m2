from __future__ import annotations

from enum import Enum


class OrderStatus(str, Enum):
    ARMED = "armed"                 # Signal received, not yet placed
    PENDING = "pending"             # Order placed, waiting fill
    FILLED = "filled"               # Order filled, position open
    CANCELLED = "cancelled"         # Order cancelled (DISARM)
    FAILED = "failed"               # Order failed (API error)
    SKIPPED_LOW_BALANCE = "skipped_low_balance"
    SKIPPED_WHITELIST = "skipped_whitelist"


class SignalType(str, Enum):
    ARM = "arm"
    DISARM = "disarm"


class Side(str, Enum):
    LONG = "long"
    SHORT = "short"
    BOTH = "both"  # Only valid for configuration (whitelist)
