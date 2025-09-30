"""
Arm/Disarm Domain Logic (stateless coordinators)

Produces a Plan:
  - entry: one Entry OrderPayload
  - brackets: {stop_loss, take_profit} (optional if preplace_brackets=False)
  - qty: Decimal
  - tp_price: Decimal | None
  - diagnostics: dict
  - preplace_brackets: bool
  - signal_id: str
"""

from __future__ import annotations

import hashlib
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


def _safe_coid(base: str, tag: str, max_len: int = 32) -> str:
    """
    Binance futures newClientOrderId has a small max length.
    Keep a short tag suffix and hash the base to ensure uniqueness and fixed size.
    """
    tag = tag.strip().replace(" ", "")[:8]
    h = hashlib.sha1(base.encode("utf-8")).hexdigest()  # 40 hex chars
    coid = f"{tag}-{h}"  # e.g., "entry-<hash>"
    return coid[:max_len]


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
    sym = arm["sym"].upper()
    side = arm["side"]
    trigger = Decimal(str(arm["trigger"]))
    stop = Decimal(str(arm["stop"]))

    # Basic safety checks on bot config
    status = bot_cfg.get("status", "active")
    if status != "active":
        return {
            "ok": False,
            "sym": sym,
            "side": side,
            "signal_id": arm["signal_id"],
            "diagnostics": {"notes": ["bot not active"]},
            "preplace_brackets": preplace_brackets,
        }

    side_mode = bot_cfg.get("side_mode", "both")
    if side_mode == "long_only" and side != "long":
        return {
            "ok": False,
            "sym": sym,
            "side": side,
            "signal_id": arm["signal_id"],
            "diagnostics": {"notes": ["blocked by side_mode=long_only"]},
            "preplace_brackets": preplace_brackets,
        }
    if side_mode == "short_only" and side != "short":
        return {
            "ok": False,
            "sym": sym,
            "side": side,
            "signal_id": arm["signal_id"],
            "diagnostics": {"notes": ["blocked by side_mode=short_only"]},
            "preplace_brackets": preplace_brackets,
        }

    filters = override_filters or get_symbol_filters(sym) or {}
    balance = get_free_balance(bot_cfg["user_id"], asset="USDT")
    risk = bot_cfg.get("risk_per_trade", Decimal("0.005"))
    lev = bot_cfg.get("leverage", Decimal("1"))
    tp_ratio = bot_cfg.get("tp_ratio", Decimal("1.5"))

    plan: Plan = {
        "ok": False,
        "sym": sym,
        "side": side,
        "signal_id": arm["signal_id"],
        "diagnostics": {
            "filters_present": bool(filters),
            "balance_free": str(balance),
            "tp_ratio": str(tp_ratio),
            "notes": [],
        },
        "preplace_brackets": preplace_brackets,
    }

    if balance <= 0:
        plan["diagnostics"]["notes"].append("no free balance")
        return plan
    if trigger <= 0 or stop <= 0 or trigger == stop:
        plan["diagnostics"]["notes"].append("invalid trigger/stop")
        return plan
    if risk <= 0:
        plan["diagnostics"]["notes"].append("risk_per_trade <= 0")
        return plan
    if lev <= 0:
        plan["diagnostics"]["notes"].append("leverage <= 0")
        return plan

    sizing = compute_position_size(
        trigger=trigger,
        stop=stop,
        balance_free=balance,
        risk_per_trade=risk,
        leverage=lev,
        filters=filters,
    )
    plan["diagnostics"]["sizing"] = sizing

    if not sizing.get("ok"):
        plan["diagnostics"]["notes"].append("sizing failed")
        return plan

    qty = sizing["qty"]
    max_qty = bot_cfg.get("max_qty")
    if max_qty is not None and qty > max_qty:
        plan["diagnostics"]["notes"].append(f"qty clamped to max_qty={max_qty}")
        qty = max_qty

    coid_base = f"{bot_cfg['bot_id']}|{arm['signal_id']}|{sym}|{side}"
    entry_client_id = _safe_coid(coid_base, "entry")
    sl_client_id = _safe_coid(coid_base, "sl")
    tp_client_id = _safe_coid(coid_base, "tp")

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
