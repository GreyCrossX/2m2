# app/services/tasks/domain.py

from __future__ import annotations

from decimal import Decimal
from typing import Optional, TypedDict

from .contracts import (
    ArmPayload,
    BotConfig,
    ExchangeFilters,
    OrderPayload,
    BracketPayloads,
)
from .filters_source import get_symbol_filters
from .balance_source import get_free_balance
from .sizing import compute_position_size
from .payloads import build_entry_order, build_brackets


class Plan(TypedDict, total=False):
    ok: bool
    sym: str
    side: str
    qty: Decimal
    entry: OrderPayload
    brackets: Optional[BracketPayloads]
    tp_price: Optional[Decimal]
    diagnostics: dict
    preplace_brackets: bool
    signal_id: str


def build_plan(
    *,
    arm: ArmPayload,
    bot_cfg: BotConfig,
    working_type: str = "MARK_PRICE",
    preplace_brackets: bool = True,
    override_filters: Optional[ExchangeFilters] = None,
) -> Plan:
    """
    ARM → build a STOP_MARKET entry at `trigger`.
    If preplace_brackets=True, pre-place reduceOnly SL/TP using `trigger` as provisional entry.
    """
    sym = arm["sym"]
    side = arm["side"]
    trigger = Decimal(str(arm["trigger"]))
    stop = Decimal(str(arm["stop"]))

    filters = override_filters or get_symbol_filters(sym) or {}
    balance = get_free_balance(bot_cfg["user_id"], asset="USDT")
    risk = bot_cfg.get("risk_per_trade", Decimal("0.005"))
    lev = bot_cfg.get("leverage", Decimal("1"))
    tp_ratio = bot_cfg.get("tp_ratio", Decimal("1.5"))

    # Always compute sizing and include in diagnostics (even if it fails)
    sizing = compute_position_size(
        trigger=trigger,
        stop=stop,
        balance_free=balance,
        risk_per_trade=risk,
        leverage=lev,
        filters=filters,
    )

    plan: Plan = {
        "ok": False,
        "sym": sym,
        "side": side,
        "signal_id": arm["signal_id"],
        "diagnostics": {
            "sizing": sizing,                   # <-- always present
            "filters_present": bool(filters),
            "balance_free": str(balance),
            "tp_ratio": str(tp_ratio),
            "notes": [],
        },
        "preplace_brackets": preplace_brackets,
    }

    if not sizing.get("ok"):
        # preserve failure reason in diagnostics, no further construction
        if balance <= 0:
            plan["diagnostics"]["notes"].append("no free balance")
        return plan

    qty = sizing["qty"]
    max_qty = bot_cfg.get("max_qty")

    # Test expectation: exceeding max_qty → plan is NOT OK (do not clamp)
    if max_qty is not None and qty > max_qty:
        plan["diagnostics"]["notes"].append(f"qty {qty} exceeds max_qty={max_qty}")
        return plan

    # Idempotent client order IDs
    coid_base = f"{bot_cfg['bot_id']}:{arm['signal_id']}"
    entry_client_id = f"{coid_base}:entry"
    sl_client_id = f"{coid_base}:sl"
    tp_client_id = f"{coid_base}:tp"

    entry = build_entry_order(
        sym=sym,
        side=side,
        qty=qty,
        trigger=trigger,
        working_type=working_type,
        client_order_id=entry_client_id,
        filters=filters,
    )

    brackets: Optional[BracketPayloads] = None
    tp_price: Optional[Decimal] = None

    if preplace_brackets:
        brackets, tp_price = build_brackets(
            sym=sym,
            side=side,
            qty=qty,
            avg_entry_price=trigger,
            stop=stop,
            tp_ratio=tp_ratio,
            working_type=working_type,
            sl_client_order_id=sl_client_id,
            tp_client_order_id=tp_client_id,
            filters=filters,
        )

    plan.update(
        ok=True,
        qty=qty,
        entry=entry,
        brackets=brackets,
        tp_price=tp_price,
    )
    return plan
