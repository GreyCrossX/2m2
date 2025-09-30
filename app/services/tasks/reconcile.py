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
from typing import Any, Dict, List, Set

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


def _order_keys(o: Dict[str, Any]) -> Set[str]:
    """
    Collect all plausible identifiers an order might expose so our tracking
    (which may use orderId or clientOrderId depending on placement) matches.
    """
    out: Set[str] = set()
    for k in ("orderId", "orderID", "order_id", "clientOrderId", "clientOrderID"):
        val = o.get(k)
        if val is not None:
            out.add(str(val))
    return out


def reconcile_bot(bot_id: str) -> Dict[str, Any]:
    cfg = read_bot_config(bot_id)
    if not cfg:
        return {"ok": False, "error": "bot config not found", "bot_id": bot_id}

    sym = cfg["sym"]
    user_id = cfg["user_id"]

    st = read_bot_state(bot_id) or {}

    # --- Open orders ----------------------------------------------------------
    oo = get_open_orders(user_id, symbol=sym)
    if not oo.get("ok"):
        return {"ok": False, "error": f"open orders fetch failed: {oo.get('error')}", "bot_id": bot_id}
    open_orders = oo.get("orders") or []
    open_ids: Set[str] = set()
    for o in open_orders:
        if isinstance(o, dict):
            open_ids |= _order_keys(o)

    # --- Positions ------------------------------------------------------------
    pos = get_positions(user_id, symbol=sym)
    if not pos.get("ok"):
        return {"ok": False, "error": f"positions fetch failed: {pos.get('error')}", "bot_id": bot_id}
    positions = pos.get("positions") or []

    # Assume one-way mode (single net position). If hedge, first non-zero wins.
    position_qty = Decimal("0")
    avg_entry_price = Decimal("0")
    position_side = None

    for p in positions:
        if not isinstance(p, dict):
            continue
        qty = _to_decimal(p.get("positionAmt") or p.get("position_amount") or "0")
        entry_px = _to_decimal(p.get("entryPrice") or p.get("avgEntryPrice") or "0")
        if qty != 0:
            position_qty = qty.copy_abs()
            avg_entry_price = entry_px
            position_side = "long" if qty > 0 else "short"
            break

    # --- Tracking cleanup -----------------------------------------------------
    tracked = list_tracked_orders(bot_id)
    removed: List[str] = []
    for oid in tracked:
        # If neither orderId nor clientOrderId shows up, untrack it
        if oid not in open_ids:
            untrack_open_order(bot_id, oid)
            removed.append(oid)

    # --- State update ---------------------------------------------------------
    # Only persist the fields we manage here; leave other keys intact.
    st["position_qty"] = position_qty if position_qty != 0 else None
    st["avg_entry_price"] = avg_entry_price if position_qty != 0 else None
    st["position_side"] = position_side
    write_bot_state(bot_id, st)

    # --- Inconsistencies ------------------------------------------------------
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
