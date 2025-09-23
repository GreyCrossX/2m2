"""
Trading Actions (thin application layer)

Side effects at the exchange + Redis state updates.

Exports:
- place_entry_and_track(bot_id, plan) -> {ok, entry_id}
- place_brackets_and_track(bot_id, plan) -> {ok, sl_id, tp_id}
- disarm(bot_id) -> {ok, cancelled:{entry:bool, brackets:int}, reason}
- cancel_tracked_orders(bot_id) -> {ok, cancelled:int}   # utility, not DISARM semantics
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, Optional

from .exchange import new_order, cancel_order, get_open_orders
from .state import (
    read_bot_config,
    read_bot_state,
    write_bot_state,
    track_open_order,
    untrack_open_order,
    list_tracked_orders,
)

# If you want per-user rate limiting, uncomment and use:
# from .rate_limiter import acquire as rl_acquire


def _to_float(v: Any) -> Any:
    return float(v) if isinstance(v, Decimal) else v


def _payload_to_kwargs(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map internal OrderPayload to **kwargs for exchange.new_order.
    Ensures numeric fields are floats for the SDK.
    """
    out: Dict[str, Any] = {}
    for k, v in payload.items():
        if k == "order_type":
            out["order_type"] = v
        elif k == "working_type":
            out["working_type"] = v
        elif k == "client_order_id":
            out["client_order_id"] = v
        elif k == "stop_price":
            out["stop_price"] = _to_float(v)
        elif k == "quantity":
            out["quantity"] = _to_float(v)
        else:
            out[k] = v
    return out


def place_entry_and_track(bot_id: str, plan: Dict[str, Any]) -> Dict[str, Any]:
    cfg = read_bot_config(bot_id)
    if not cfg:
        return {"ok": False, "error": "bot config not found"}
    if not plan.get("ok"):
        return {"ok": False, "error": "plan not ok"}

    entry = plan["entry"]
    kwargs = _payload_to_kwargs(entry)

    # Optional rate limit (uncomment if needed)
    # if not rl_acquire(cfg["user_id"], cost=1):
    #     return {"ok": False, "error": "rate_limited"}

    res = new_order(
        cfg["user_id"],
        symbol=kwargs["symbol"],
        side=kwargs["side"],
        order_type=kwargs["order_type"],
        quantity=kwargs.get("quantity"),
        reduce_only=kwargs.get("reduce_only"),
        stop_price=kwargs.get("stop_price"),
        working_type=kwargs.get("working_type"),
        close_position=kwargs.get("close_position"),
        client_order_id=kwargs.get("client_order_id"),
        positionSide=kwargs.get("positionSide"),
        price=kwargs.get("price"),
        timeInForce=kwargs.get("timeInForce"),
    )
    if not res.get("ok"):
        return {"ok": False, "error": res.get("error", "entry failed")}

    entry_id = res.get("order_id")
    if entry_id:
        track_open_order(bot_id, str(entry_id))

    st = read_bot_state(bot_id)
    st.update({
        "last_signal_id": plan.get("signal_id") or st.get("last_signal_id"),
        "armed_entry_order_id": str(entry_id) if entry_id else None,
    })
    write_bot_state(bot_id, st)

    return {"ok": True, "entry_id": entry_id, "raw": res.get("raw")}


def place_brackets_and_track(bot_id: str, plan: Dict[str, Any]) -> Dict[str, Any]:
    cfg = read_bot_config(bot_id)
    if not cfg:
        return {"ok": False, "error": "bot config not found"}
    if not plan.get("ok"):
        return {"ok": False, "error": "plan not ok"}

    brackets = plan.get("brackets")
    if not brackets:
        return {"ok": True, "skipped": "no brackets in plan"}

    placed_ids: list[str] = []

    for key in ("stop_loss", "take_profit"):
        payload = brackets.get(key)
        if not payload:
            continue
        kwargs = _payload_to_kwargs(payload)

        # Optional rate limit
        # if not rl_acquire(cfg["user_id"], cost=1):
        #     return {"ok": False, "error": f"rate_limited on {key}"}

        res = new_order(
            cfg["user_id"],
            symbol=kwargs["symbol"],
            side=kwargs["side"],
            order_type=kwargs["order_type"],
            quantity=kwargs.get("quantity"),
            reduce_only=kwargs.get("reduce_only"),
            stop_price=kwargs.get("stop_price"),
            working_type=kwargs.get("working_type"),
            close_position=kwargs.get("close_position"),
            client_order_id=kwargs.get("client_order_id"),
            positionSide=kwargs.get("positionSide"),
            price=kwargs.get("price"),
            timeInForce=kwargs.get("timeInForce"),
        )
        if not res.get("ok"):
            return {"ok": False, "error": f"{key} failed: {res.get('error')}", "placed": placed_ids}

        oid = res.get("order_id")
        if oid:
            track_open_order(bot_id, str(oid))
            placed_ids.append(str(oid))

    st = read_bot_state(bot_id)
    st["bracket_ids"] = ",".join(placed_ids) if placed_ids else None
    write_bot_state(bot_id, st)

    return {"ok": True, "sl_tp_ids": placed_ids}


def cancel_tracked_orders(bot_id: str) -> Dict[str, Any]:
    """
    Utility: cancel everything we currently track (entry + brackets).
    Prefer using disarm() for DISARM semantics.
    """
    cfg = read_bot_config(bot_id)
    if not cfg:
        return {"ok": False, "error": "bot config not found"}

    sym = cfg["sym"]
    user_id = cfg["user_id"]
    open_ids = list_tracked_orders(bot_id)

    cancelled = 0
    errors: list[str] = []

    for oid in open_ids:
        res = cancel_order(user_id, symbol=sym, order_id=oid)
        if res.get("ok"):
            untrack_open_order(bot_id, oid)
            cancelled += 1
        else:
            errors.append(f"{oid}: {res.get('error')}")

    st = read_bot_state(bot_id)
    st["armed_entry_order_id"] = None
    st["bracket_ids"] = None
    write_bot_state(bot_id, st)

    out: Dict[str, Any] = {"ok": True, "cancelled": cancelled}
    if errors:
        out["errors"] = errors
    return out


def disarm(bot_id: str) -> Dict[str, Any]:
    """
    DISARM semantics:
      - If entry order hasn't filled (i.e., still OPEN), cancel the entry and any
        pre-placed reduce-only brackets.
      - If entry is already filled, DO NOT cancel brackets (they protect the position).
    """
    cfg = read_bot_config(bot_id)
    if not cfg:
        return {"ok": False, "error": "bot config not found"}

    sym = cfg["sym"]
    user_id = cfg["user_id"]
    st = read_bot_state(bot_id)

    oo = get_open_orders(user_id, symbol=sym)
    if not oo.get("ok"):
        return {"ok": False, "error": f"open orders fetch failed: {oo.get('error')}"}

    open_order_ids = {str(o.get("orderId") or o.get("orderID") or "") for o in oo.get("orders", []) if o}

    entry_id = (st.get("armed_entry_order_id") or "") if st else ""
    brackets_csv = (st.get("bracket_ids") or "") if st else ""
    bracket_ids = [x for x in brackets_csv.split(",") if x]

    entry_open = bool(entry_id and (entry_id in open_order_ids))

    cancelled_entry = False
    cancelled_brackets = 0
    errors: list[str] = []

    if entry_open:
        res = cancel_order(user_id, symbol=sym, order_id=entry_id)
        if res.get("ok"):
            untrack_open_order(bot_id, entry_id)
            cancelled_entry = True
        else:
            errors.append(f"entry {entry_id}: {res.get('error')}")

        for oid in bracket_ids:
            if oid in open_order_ids:
                r2 = cancel_order(user_id, symbol=sym, order_id=oid)
                if r2.get("ok"):
                    untrack_open_order(bot_id, oid)
                    cancelled_brackets += 1
                else:
                    errors.append(f"bracket {oid}: {r2.get('error')}")

        st["armed_entry_order_id"] = None
        st["bracket_ids"] = None
        write_bot_state(bot_id, st)

        out: Dict[str, Any] = {
            "ok": True,
            "cancelled": {"entry": cancelled_entry, "brackets": cancelled_brackets},
            "reason": "entry_not_filled",
        }
        if errors:
            out["errors"] = errors
        return out

    return {
        "ok": True,
        "cancelled": {"entry": False, "brackets": 0},
        "reason": "entry_already_filled_or_absent",
    }
