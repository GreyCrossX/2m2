from __future__ import annotations

from decimal import Decimal
from typing import Dict

from .exchange_filters import enforce_mins, round_qty


def _to_decimal(value) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _normalise_filters(filters: Dict[str, object]) -> Dict[str, Decimal]:
    out: Dict[str, Decimal] = {}
    for key, value in (filters or {}).items():
        try:
            out[key] = _to_decimal(value)
        except Exception:
            out[key] = Decimal("0")
    return out


def compute_position_size(
    *,
    trigger: Decimal,
    stop: Decimal,
    balance_free: Decimal,
    risk_per_trade: Decimal,
    leverage: Decimal,
    filters: Dict[str, object],
) -> Dict[str, object]:
    price = _to_decimal(trigger)
    stop_price = _to_decimal(stop)
    distance = abs(price - stop_price)
    if distance <= 0:
        return {
            "ok": False,
            "distance": Decimal("0"),
            "qty": Decimal("0"),
            "notes": ["invalid distance"],
        }

    risk_budget = _to_decimal(balance_free) * _to_decimal(risk_per_trade)
    if risk_budget <= 0:
        return {
            "ok": False,
            "distance": distance,
            "qty": Decimal("0"),
            "notes": ["no risk budget"],
        }

    lev = max(_to_decimal(leverage), Decimal("0"))
    if price <= 0:
        return {
            "ok": False,
            "distance": distance,
            "qty": Decimal("0"),
            "notes": ["invalid trigger price"],
        }

    notional = risk_budget * lev
    qty = notional / price

    norm_filters = _normalise_filters(filters)
    _, qty, min_notes = enforce_mins(price, qty, norm_filters)
    qty = round_qty(qty, norm_filters)

    if qty <= 0:
        notes = list(min_notes) or ["qty below exchange minimum"]
        return {"ok": False, "distance": distance, "qty": Decimal("0"), "notes": notes}

    return {
        "ok": True,
        "qty": qty,
        "distance": distance,
        "notional": price * qty,
        "notes": list(min_notes),
    }


__all__ = ["compute_position_size"]
