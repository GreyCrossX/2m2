"""
Reconciler (side-effect app logic)

Fetches live exchange state and normalizes our BotState:
- Updates position info (qty, avg_entry_price, side)
- Removes tracking for orders that are no longer open
- Optionally returns inconsistencies (e.g., tracked order missing on exchange)

Exports:
- reconcile_bot(bot_id) -> {ok, updated_state: BotState, removed_orders: list[str], inconsistencies: list[str]}
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List

from .exchange import get_open_orders, get_positions
from .state import (
    read_bot_config,
    read_bot_state,
    write_bot_state,
    list_tracked_orders,
    untrack_open_order,
)


def _to_decimal(v: Any) -> Decimal:
    try:
        return Decimal(str(v))
    except Exception:
        return Decimal("0")


def reconcile_bot(bot_id: str) -> Dict[str, Any]:
    cfg = read_bot_config(bot_id)
    if not cfg:
        return {"ok": False, "error": "bot config not found"}

    sym = cfg["sym"]
    user_id = cfg["user_id"]

    st = read_bot_state(bot_id) or {}

    oo = get_open_orders(user_id, symbol=sym)
    if not oo.get("ok"):
        return {"ok": False, "error": f"open orders fetch failed: {oo.get('error')}"}
    open_orders = oo.get("orders", []) or []
    open_ids = {str(o.get("orderId") or o.get("orderID") or "") for o in open_orders}

    pos = get_positions(user_id, symbol=sym)
    if not pos.get("ok"):
        return {"ok": False, "error": f"positions fetch failed: {pos.get('error')}"}
    positions = pos.get("positions", []) or []

    # Interpret position (assuming one-way mode; hedge would track both sides separately)
    position_qty = Decimal("0")
    avg_entry_price = Decimal("0")
    position_side = None

    for p in positions:
        qty = _to_decimal(p.get("positionAmt") or p.get("position_amount") or "0")
        entry_px = _to_decimal(p.get("entryPrice") or p.get("avgEntryPrice") or "0")
        if qty != 0:
            position_qty = qty.copy_abs()
            avg_entry_price = entry_px
            position_side = "long" if qty > 0 else "short"
            break

    # Remove tracked orders that are no longer open
    tracked = list_tracked_orders(bot_id)
    removed: List[str] = []
    for oid in tracked:
        if oid not in open_ids:
            untrack_open_order(bot_id, oid)
            removed.append(oid)

    # Update state
    st["position_qty"] = position_qty if position_qty != 0 else None
    st["avg_entry_price"] = avg_entry_price if position_qty != 0 else None
    st["position_side"] = position_side
    write_bot_state(bot_id, st)

    # Basic inconsistencies (optional)
    inconsistencies: List[str] = []
    armed = st.get("armed_entry_order_id")
    if armed and armed not in open_ids:
        inconsistencies.append(f"armed_entry_order_id {armed} not in open orders")
    br_csv = st.get("bracket_ids") or ""
    for oid in [x for x in br_csv.split(",") if x]:
        if oid not in open_ids:
            inconsistencies.append(f"bracket {oid} not in open orders")

    return {
        "ok": True,
        "updated_state": st,
        "removed_orders": removed,
        "inconsistencies": inconsistencies,
        "open_order_count": len(open_ids),
        "position": {
            "side": position_side,
            "qty": str(position_qty),
            "avg_entry_price": str(avg_entry_price),
        },
    }
