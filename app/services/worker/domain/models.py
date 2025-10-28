from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Mapping, Optional, Union
from uuid import UUID, uuid4

from .enums import OrderStatus, SignalType, OrderSide, SideWhitelist
from .exceptions import InvalidSignalException


# ---------- helpers (pure, domain-level) ----------

def _parse_decimal(name: str, value: Any) -> Decimal:
    if value is None:
        raise InvalidSignalException(f"Missing decimal field: {name}")
    try:
        # Accept str/float/Decimal
        return value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise InvalidSignalException(f"Invalid decimal for '{name}': {value!r}") from exc


def _parse_int(name: str, value: Any) -> int:
    if value is None:
        raise InvalidSignalException(f"Missing integer field: {name}")
    try:
        return int(value)
    except (ValueError, TypeError) as exc:
        raise InvalidSignalException(f"Invalid integer for '{name}': {value!r}") from exc


def _ms_to_datetime(ms: int) -> datetime:
    # Redis & signals use epoch milliseconds
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def _parse_order_side(name: str, value: Any) -> OrderSide:
    """Parse a true trading side (only 'long' or 'short')."""
    if value is None:
        raise InvalidSignalException(f"Missing side field: {name}")
    try:
        s = str(value).strip().lower()
    except Exception:
        raise InvalidSignalException(f"Invalid side for '{name}': {value!r}")
    if s == "long":
        return OrderSide.LONG
    if s == "short":
        return OrderSide.SHORT
    # 'both' is NEVER valid for real orders/signals
    raise InvalidSignalException(f"Invalid order side for '{name}': {value!r}")


def _parse_whitelist_side(name: str, value: Any) -> SideWhitelist:
    """Parse bot configuration whitelist side ('long'|'short'|'both')."""
    if value is None:
        raise InvalidSignalException(f"Missing side field: {name}")
    try:
        s = str(value).strip().lower()
    except Exception:
        raise InvalidSignalException(f"Invalid side for '{name}': {value!r}")
    if s == "both":
        return SideWhitelist.BOTH
    if s == "long":
        return SideWhitelist.LONG
    if s == "short":
        return SideWhitelist.SHORT
    raise InvalidSignalException(f"Invalid whitelist side for '{name}': {value!r}")


# ---------- Signals ----------

@dataclass(frozen=True)
class ArmSignal:
    """
    Parsed ARM entry signal.
    Matches payload shape:

    {
      "v": "1",
      "type": "arm",
      "side": "long" | "short",
      "sym": "BTCUSDT",
      "tf": "2m",
      "ts": "1697890123456",
      "ind_ts": "1697890000000",
      "ind_high": "35000.50",
      "ind_low": "34900.25",
      "trigger": "35001.50",
      "stop": "34899.25"
    }
    """
    version: str
    side: OrderSide
    symbol: str
    timeframe: str
    ts_ms: int
    ts: datetime
    ind_ts_ms: int
    ind_ts: datetime
    ind_high: Decimal
    ind_low: Decimal
    trigger: Decimal
    stop: Decimal
    # Redis stream message id can be attached by the application layer via dataclasses.replace(...)
    signal_msg_id: Optional[str] = None  # e.g., "1697890123456-0"

    @classmethod
    def from_stream(cls, data: Mapping[str, Any]) -> "ArmSignal":
        if str(data.get("type", "")).lower() != SignalType.ARM.value:
            raise InvalidSignalException("Expected ARM signal type.")
        version = str(data.get("v", ""))
        side = _parse_order_side("side", data.get("side"))
        symbol = str(data.get("sym", "")).upper()
        timeframe = str(data.get("tf", ""))
        if not symbol or not timeframe or not version:
            raise InvalidSignalException("Missing required string fields: v/sym/tf.")

        ts_ms = _parse_int("ts", data.get("ts"))
        ind_ts_ms = _parse_int("ind_ts", data.get("ind_ts"))

        ind_high = _parse_decimal("ind_high", data.get("ind_high"))
        ind_low = _parse_decimal("ind_low", data.get("ind_low"))
        trigger = _parse_decimal("trigger", data.get("trigger"))
        stop = _parse_decimal("stop", data.get("stop"))

        # Basic logical checks
        if trigger <= 0 or stop <= 0:
            raise InvalidSignalException("trigger/stop must be > 0.")
        if ind_low > ind_high:
            raise InvalidSignalException("ind_low must be <= ind_high.")

        return cls(
            version=version,
            side=side,
            symbol=symbol,
            timeframe=timeframe,
            ts_ms=ts_ms,
            ts=_ms_to_datetime(ts_ms),
            ind_ts_ms=ind_ts_ms,
            ind_ts=_ms_to_datetime(ind_ts_ms),
            ind_high=ind_high,
            ind_low=ind_low,
            trigger=trigger,
            stop=stop,
        )

    @property
    def type(self) -> SignalType:
        return SignalType.ARM


@dataclass(frozen=True)
class DisarmSignal:
    """
    Parsed DISARM (cancel) signal.

    {
      "v": "1",
      "type": "disarm",
      "prev_side": "long" | "short",
      "sym": "BTCUSDT",
      "tf": "2m",
      "ts": "1697890223456",
      "reason": "regime:long->neutral" | "flip:long->short"
    }
    """
    version: str
    prev_side: OrderSide
    symbol: str
    timeframe: str
    ts_ms: int
    ts: datetime
    reason: str
    signal_msg_id: Optional[str] = None

    @classmethod
    def from_stream(cls, data: Mapping[str, Any]) -> "DisarmSignal":
        if str(data.get("type", "")).lower() != SignalType.DISARM.value:
            raise InvalidSignalException("Expected DISARM signal type.")
        version = str(data.get("v", ""))
        prev_side = _parse_order_side("prev_side", data.get("prev_side"))
        symbol = str(data.get("sym", "")).upper()
        timeframe = str(data.get("tf", ""))
        if not symbol or not timeframe or not version:
            raise InvalidSignalException("Missing required string fields: v/sym/tf.")

        ts_ms = _parse_int("ts", data.get("ts"))
        reason = str(data.get("reason", "")).strip()
        if not reason:
            raise InvalidSignalException("Missing reason for DISARM signal.")

        return cls(
            version=version,
            prev_side=prev_side,
            symbol=symbol,
            timeframe=timeframe,
            ts_ms=ts_ms,
            ts=_ms_to_datetime(ts_ms),
            reason=reason,
        )

    @property
    def type(self) -> SignalType:
        return SignalType.DISARM


Signal = Union[ArmSignal, DisarmSignal]


# ---------- Bot configuration ----------

@dataclass(frozen=True)
class BotConfig:
    """
    Bot configuration as read from persistent storage (e.g., Postgres).
    Only domain-relevant attributes are modeled here.
    """
    id: UUID
    user_id: UUID
    cred_id: UUID

    symbol: str
    timeframe: str
    enabled: bool
    env: str  # "testnet" | "prod"

    side_whitelist: SideWhitelist  # 'both' is allowed here
    leverage: int

    use_balance_pct: bool
    balance_pct: Decimal                 # used if use_balance_pct is True
    fixed_notional: Optional[Decimal]    # alternative sizing mode
    max_position_usdt: Optional[Decimal] # risk cap per position

    def allows_side(self, side: OrderSide) -> bool:
        """Check if the bot is configured to trade the given side."""
        return (
            self.side_whitelist == SideWhitelist.BOTH
            or (self.side_whitelist == SideWhitelist.LONG and side == OrderSide.LONG)
            or (self.side_whitelist == SideWhitelist.SHORT and side == OrderSide.SHORT)
        )


# ---------- Order lifecycle state ----------

@dataclass
class OrderState:
    """
    Aggregate capturing per-signal, per-bot order lifecycle.

    Note: timestamps are UTC.
    """
    bot_id: UUID
    signal_id: str                   # Redis stream message ID
    status: OrderStatus
    side: OrderSide
    symbol: str

    trigger_price: Decimal
    stop_price: Decimal
    quantity: Decimal

    id: UUID = field(default_factory=uuid4)
    order_id: Optional[int] = None   # Binance-assigned orderId (int per UMFutures)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def touch(self) -> None:
        self.updated_at = datetime.now(timezone.utc)

    def mark(self, status: OrderStatus, *, order_id: Optional[int] = None) -> None:
        self.status = status
        if order_id is not None:
            self.order_id = order_id
        self.touch()


# ---------- Position tracking ----------

@dataclass
class Position:
    """
    Represents an open position associated with a bot.
    """
    bot_id: UUID
    symbol: str
    side: OrderSide

    entry_price: Decimal
    quantity: Decimal
    stop_loss: Decimal
    take_profit: Decimal

    unrealized_pnl: Decimal = Decimal("0")
    opened_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def update_unrealized(self, mark_price: Decimal) -> None:
        """
        Update unrealized PnL with a simple linear model:
          LONG: (mark - entry) * qty
          SHORT: (entry - mark) * qty
        """
        mark = _parse_decimal("mark_price", mark_price)
        if self.side == OrderSide.LONG:
            self.unrealized_pnl = (mark - self.entry_price) * self.quantity
        elif self.side == OrderSide.SHORT:
            self.unrealized_pnl = (self.entry_price - mark) * self.quantity
        else:
            # 'both' is never valid for positions
            raise InvalidSignalException("Invalid position side")
