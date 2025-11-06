from __future__ import annotations
from enum import Enum


class OrderStatus(str, Enum):
    ARMED = "armed"
    PENDING = "pending"
    FILLED = "filled"
    CLOSED = "closed"
    CANCELLED = "cancelled"
    FAILED = "failed"
    SKIPPED_LOW_BALANCE = "skipped_low_balance"
    SKIPPED_WHITELIST = "skipped_whitelist"

    def __str__(self) -> str:
        return self.value


class SignalType(str, Enum):
    ARM = "arm"
    DISARM = "disarm"

    def __str__(self) -> str:
        return self.value


class OrderSide(str, Enum):
    """Side applicable to orders and order_states.side (Postgres enum: order_side_enum)."""
    LONG = "long"
    SHORT = "short"

    def __str__(self) -> str:
        return self.value

    @classmethod
    def from_value(cls, value: str | "OrderSide" | "SideWhitelist") -> "OrderSide":
        if isinstance(value, OrderSide):
            return value
        try:
            # If value is another Enum with a 'value' attribute
            return cls(str(getattr(value, "value", value)))
        except Exception:
            return cls(str(value))

    def exit_side(self) -> "OrderSide":
        """Return the side that would close/reduce a position opened on ``self``."""
        if self == OrderSide.LONG:
            return OrderSide.SHORT
        if self == OrderSide.SHORT:
            return OrderSide.LONG
        raise ValueError(f"Unsupported order side: {self}")


def exit_side_for(entry_side: OrderSide) -> OrderSide:
    """Helper function mirroring :meth:`OrderSide.exit_side` for functional use."""
    return entry_side.exit_side()


class SideWhitelist(str, Enum):
    """
    Bot configuration whitelist (Postgres enum: side_whitelist_enum).
    Note: not to be used in order_states; only in config/whitelist checks.
    """
    BOTH = "both"
    LONG = "long"
    SHORT = "short"

    def __str__(self) -> str:
        return self.value
