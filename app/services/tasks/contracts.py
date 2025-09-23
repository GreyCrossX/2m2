"""
Pure contracts & types used across worker modules.

- No project-internal imports
- Only typing + decimal
"""

from __future__ import annotations

from decimal import Decimal
from typing import Literal, NotRequired, Optional, Protocol, TypedDict


# ──────────────────────────────────────────────────────────────────────────────
# Calc / Signal messages
# ──────────────────────────────────────────────────────────────────────────────

SignalType = Literal["arm", "disarm"]
Side = Literal["long", "short"]
TF = Literal["1m", "2m", "5m", "15m", "1h", "4h", "1d"]

class SignalMsg(TypedDict, total=False):
    """
    Minimal shape expected from calc's signal stream.
    Keep fields as strings if coming straight from Redis to avoid float drift.
    """
    v: str
    type: SignalType
    sym: str
    tf: TF
    ts: str              # unix ms timestamp as string
    side: NotRequired[Side]        # for 'arm'
    prev_side: NotRequired[Side]   # for 'disarm'
    ind_ts: NotRequired[str]       # indicator candle ts
    trigger: NotRequired[str]
    stop: NotRequired[str]
    reason: NotRequired[str]       # e.g., "regime:long->neutral"


# ──────────────────────────────────────────────────────────────────────────────
# Worker payloads (ARM/DISARM)
# ──────────────────────────────────────────────────────────────────────────────

class ArmPayload(TypedDict):
    bot_id: str
    signal_id: str         # e.g., "BTCUSDT:169..:long"
    sym: str
    side: Side
    trigger: Decimal
    stop: Decimal
    tp_ratio: Decimal      # take-profit multiple (R multiple), e.g. 1.8


class DisarmPayload(TypedDict):
    bot_id: str
    signal_id: str
    sym: str
    side: Side             # previous side to disarm/cancel


# ──────────────────────────────────────────────────────────────────────────────
# Exchange order payloads (map cleanly to your new_order(**kwargs) tool)
# ──────────────────────────────────────────────────────────────────────────────

OrderType = Literal[
    "MARKET",
    "LIMIT",
    "STOP_MARKET",
    "STOP",
    "TAKE_PROFIT_MARKET",
    "TAKE_PROFIT",
]

WorkingType = Literal["MARK_PRICE", "CONTRACT_PRICE", "LAST_PRICE"]

class OrderPayload(TypedDict, total=False):
    symbol: str
    side: Literal["BUY", "SELL"]
    order_type: OrderType
    quantity: NotRequired[Decimal]
    reduce_only: NotRequired[bool]
    stop_price: NotRequired[Decimal]
    working_type: NotRequired[WorkingType]
    close_position: NotRequired[bool]
    # Optional client IDs for idempotency:
    client_order_id: NotRequired[str]


class BracketPayloads(TypedDict):
    stop_loss: OrderPayload
    take_profit: OrderPayload


# ──────────────────────────────────────────────────────────────────────────────
# Bot configuration & state
# ──────────────────────────────────────────────────────────────────────────────

SideMode = Literal["both", "long_only", "short_only"]
BotStatus = Literal["active", "paused"]

class BotConfig(TypedDict, total=False):
    bot_id: str
    user_id: str
    sym: str
    side_mode: SideMode
    risk_per_trade: Decimal     # fraction of free balance, e.g. Decimal("0.005")
    leverage: Decimal           # e.g., Decimal("5")
    tp_ratio: Decimal           # R multiple for TP
    max_qty: NotRequired[Decimal]
    status: BotStatus           # "active" | "paused"


class BotState(TypedDict, total=False):
    last_signal_id: Optional[str]
    armed_entry_order_id: Optional[str]
    bracket_ids: Optional[str]  # CSV of order ids
    position_side: Optional[Side]     # "long" | "short" | None (= flat)
    position_qty: Optional[Decimal]
    avg_entry_price: Optional[Decimal]


# ──────────────────────────────────────────────────────────────────────────────
# Exchange filters (symbol metadata used for rounding & validation)
# ──────────────────────────────────────────────────────────────────────────────

class ExchangeFilters(TypedDict, total=False):
    """
    Typical futures filters distilled to what we actually need.
    """
    tick_size: Decimal      # price tick granularity
    step_size: Decimal      # quantity step granularity
    min_qty: Decimal        # minimum order qty
    min_notional: Decimal   # minimum notional value for an order
    price_precision: NotRequired[int]   # optional (SDK sometimes needs it)
    quantity_precision: NotRequired[int]


# ──────────────────────────────────────────────────────────────────────────────
# Small contracts for results/validation (pure functions return these)
# ──────────────────────────────────────────────────────────────────────────────

class SizingResult(TypedDict, total=False):
    ok: bool
    qty: Decimal
    raw_qty: Decimal
    risk_amount: Decimal
    distance: Decimal
    clamped_qty: Decimal
    notional: Decimal
    warnings: list[str]
    errors: list[str]


# ──────────────────────────────────────────────────────────────────────────────
# Optional protocol for an exchange facade (kept here to avoid cycles)
# ──────────────────────────────────────────────────────────────────────────────

class ExchangeClient(Protocol):
    def new_order(self, **kwargs) -> dict: ...
    def cancel_order(self, **kwargs) -> dict: ...
