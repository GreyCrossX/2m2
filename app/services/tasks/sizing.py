from __future__ import annotations

from decimal import Decimal, ROUND_DOWN
from typing import Dict, Any

from .exchange_filters import round_qty as _round_qty


def _d(x: Any) -> Decimal:
    return x if isinstance(x, Decimal) else Decimal(str(x))


def compute_position_size(
    *,
    trigger: Decimal,
    stop: Decimal,
    balance_free: Decimal,
    risk_per_trade: Decimal,          # e.g. Decimal("0.05") for 5%
    leverage: Decimal,                # e.g. Decimal("5")
    filters: Dict[str, Any],          # expects snake_case keys: step_size, min_qty, min_notional, tick_size (optional)
) -> Dict[str, Any]:
    """
    Return a sizing decision:
    {
      "ok": bool,
      "qty": Decimal,           # rounded to step_size
      "distance": Decimal,      # |trigger - stop|
      "risk_usdt": Decimal,
      "leverage": Decimal,
      "notional": Decimal,
      "constraints": { ... },
      "notes": [ ... ],
    }

    Model:
      risk_usdt = balance_free * risk_per_trade
      distance  = |trigger - stop|
      qty_raw   = (risk_usdt * leverage) / distance
      qty       = round_to_step(qty_raw)

    Enforce min_qty / min_notional.
    """
    trigger = _d(trigger)
    stop = _d(stop)
    balance_free = _d(balance_free)
    risk_per_trade = _d(risk_per_trade)
    leverage = _d(leverage)

    notes: list[str] = []
    distance = (trigger - stop).copy_abs()
    if distance <= 0:
        return {"ok": False, "qty": Decimal("0"), "distance": distance, "notes": ["invalid stop/trigger"]}

    risk_usdt = (balance_free * risk_per_trade)
    if risk_usdt <= 0:
        return {"ok": False, "qty": Decimal("0"), "distance": distance, "notes": ["no risk budget"]}

    qty_raw = (risk_usdt * leverage) / distance

    step = _d(filters.get("step_size", "0.001"))
    min_qty = _d(filters.get("min_qty", "0"))
    min_notional = _d(filters.get("min_notional", "0"))

    qty = _round_qty(qty_raw, {"step_size": step}) if step > 0 else qty_raw

    constraints = {
        "step_size": str(step),
        "min_qty": str(min_qty),
        "min_notional": str(min_notional),
    }

    # Enforce min_qty
    if min_qty > 0 and qty < min_qty:
        qty = min_qty

    # Enforce min_notional (approx with entry ~ trigger)
    notional = qty * trigger
    if min_notional > 0 and notional < min_notional:
        need = (min_notional / trigger)
        qty = _round_qty(need, {"step_size": step}) if step > 0 else need
        notes.append("bumped to meet min_notional")
        notional = qty * trigger

    ok = qty > 0
    if not ok:
        notes.append("qty rounded to zero")

    return {
        "ok": ok,
        "qty": qty,
        "distance": distance,
        "risk_usdt": risk_usdt,
        "leverage": leverage,
        "notional": notional if ok else Decimal("0"),
        "constraints": constraints,
        "notes": notes,
    }
