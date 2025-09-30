"""
Pure contracts & types used across worker modules.

- No project-internal imports
- Only typing + decimal
"""

from __future__ import annotations

from decimal import Decimal
from typing import Literal, NotRequired, Optional, Protocol, TypedDict


# Calc / Signal messages

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
    ts: str
    side: NotRequired[Side]
    prev_side: NotRequired[Side]
    ind_ts: NotRequired[str]
    trigger: NotRequired[str]
    stop: NotRequired[str]
    reason: NotRequired[str]


# Worker payloads (ARM/DISARM)

class ArmPayload(TypedDict):
    bot_id: str
    signal_id: str
    sym: str
    side: Side
    trigger: Decimal
    stop: Decimal
    tp_ratio: Decimal


class DisarmPayload(TypedDict):
    bot_id: str
    signal_id: str
    sym: str
    side: Side


# Exchange order payloads (map cleanly to exchange.new_order(**kwargs))

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
    client_order_id: NotRequired[str]


class BracketPayloads(TypedDict):
    stop_loss: OrderPayload
    take_profit: OrderPayload


# Bot configuration & state

SideMode = Literal["both", "long_only", "short_only"]
BotStatus = Literal["active", "paused"]


class BotConfig(TypedDict, total=False):
    bot_id: str
    user_id: str
    sym: str
    side_mode: SideMode
    risk_per_trade: Decimal
    leverage: Decimal
    tp_ratio: Decimal
    max_qty: NotRequired[Decimal]
    status: BotStatus


class BotState(TypedDict, total=False):
    last_signal_id: Optional[str]
    armed_entry_order_id: Optional[str]
    bracket_ids: Optional[str]
    position_side: Optional[Side]
    position_qty: Optional[Decimal]
    avg_entry_price: Optional[Decimal]


# Exchange filters (symbol metadata used for rounding & validation)

class ExchangeFilters(TypedDict, total=False):
    tick_size: Decimal
    step_size: Decimal
    min_qty: Decimal
    min_notional: Decimal
    price_precision: NotRequired[int]
    quantity_precision: NotRequired[int]


# Small contracts for results/validation

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


# Protocol for an exchange facade

class ExchangeClient(Protocol):
    def new_order(self, **kwargs) -> dict: ...
    def cancel_order(self, **kwargs) -> dict: ...
