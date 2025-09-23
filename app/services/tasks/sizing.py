# app/services/tasks/sizing.py
from __future__ import annotations

from decimal import Decimal, ROUND_DOWN
from typing import Dict, Any, Optional

# If you already have these helpers, import them; otherwise inline minimal versions.
try:
    from .exchange_filters import round_qty, round_price
except Exception:
    def round_qty(qty: Decimal, step: Decimal) -> Decimal:
        if step <= 0: return qty
        return (qty / step).to_integral_value(rounding=ROUND_DOWN) * step
    def round_price(px: Decimal, tick: Decimal) -> Decimal:
        if tick <= 0: return px
        return (px / tick).to_integral_value(rounding=ROUND_DOWN) * tick


def _d(x: Any) -> Decimal:
    return x if isinstance(x, Decimal) else Decimal(str(x))


def compute_position_size(
    *,
    trigger: Decimal,
    stop: Decimal,
    balance_free: Decimal,
    risk_per_trade: Decimal,          # e.g. 0.005 for 0.5%
    leverage: Decimal,                # e.g. 5
    filters: Dict[str, Any],          # should contain stepSize/minQty/minNotional/tickSize if available
) -> Dict[str, Any]:
    """
    Return a sizing decision:
    {
      "ok": bool,
      "qty": Decimal,           # rounded to stepSize
      "distance": Decimal,      # |trigger - stop|
      "notes": [ ... ],
      "constraints": { ... }    # minQty/minNotional checks
    }

    Simplified model:
      risk_usdt = balance_free * risk_per_trade
      distance  = |trigger - stop|
      qty_raw   = (risk_usdt * leverage) / distance

    Then apply symbol filters: stepSize/minQty/minNotional.
    """
    trigger = _d(trigger)
    stop = _d(stop)
    balance_free = _d(balance_free)
    risk_per_trade = _d(risk_per_trade)
    leverage = _d(leverage)

    notes = []
    distance = (trigger - stop).copy_abs()
    if distance <= 0:
        return {"ok": False, "qty": Decimal("0"), "distance": distance, "notes": ["invalid stop/trigger"]}

    risk_usdt = (balance_free * risk_per_trade)
    if risk_usdt <= 0:
        return {"ok": False, "qty": Decimal("0"), "distance": distance, "notes": ["no risk budget"]}

    # Base qty calculation (USDT-margined futures; loss ≈ distance * qty).
    # Including leverage here lets you scale position for a given risk budget.
    qty_raw = (risk_usdt * leverage) / distance

    step = _d(filters.get("stepSize", "0.001"))
    min_qty = _d(filters.get("minQty", "0"))
    min_notional = _d(filters.get("minNotional", "0"))
    tick = _d(filters.get("tickSize", "0.1"))  # only used if you later need to round prices

    qty = round_qty(qty_raw, step)

    constraints = {
        "stepSize": str(step),
        "minQty": str(min_qty),
        "minNotional": str(min_notional),
        "tickSize": str(tick),
    }

    # Enforce minQty
    if min_qty > 0 and qty < min_qty:
        qty = min_qty

    # Enforce minNotional (approx with entry ~ trigger)
    notional = qty * trigger
    if min_notional > 0 and notional < min_notional:
        # bump qty up to meet min notional and re-round
        need = (min_notional / trigger)
        qty = round_qty(need, step)
        notes.append("bumped to meet minNotional")

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

