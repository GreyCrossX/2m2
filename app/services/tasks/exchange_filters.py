from __future__ import annotations

from decimal import Decimal, ROUND_DOWN
from typing import Dict, List, Tuple


def _to_decimal(value) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def round_price(price: Decimal, filters: Dict[str, Decimal | str]) -> Decimal:
    tick = _to_decimal(filters.get("tick_size") or 0)
    if tick <= 0:
        return price
    return (price // tick) * tick


def round_qty(qty: Decimal, filters: Dict[str, Decimal | str]) -> Decimal:
    step = _to_decimal(filters.get("step_size") or 0)
    if step <= 0:
        return qty
    return (qty // step) * step


def enforce_mins(price: Decimal, qty: Decimal, filters: Dict[str, Decimal | str]) -> Tuple[Decimal, Decimal, List[str]]:
    warnings: List[str] = []
    min_qty = _to_decimal(filters.get("min_qty") or 0)
    min_notional = _to_decimal(filters.get("min_notional") or 0)

    target_qty = qty
    if min_qty > 0 and target_qty < min_qty:
        target_qty = min_qty
        warnings.append("enforced min_qty")

    if min_notional > 0 and price > 0:
        required_qty = (min_notional / price).quantize(Decimal("1.00000000"), rounding=ROUND_DOWN)
        if required_qty > target_qty:
            target_qty = required_qty
            warnings.append("enforced min_notional")

    if target_qty <= 0:
        target_qty = Decimal("0")

    return price, target_qty, warnings


def apply_symbol_filters(payload: Dict[str, Decimal | str | float], filters: Dict[str, Decimal | str]) -> Dict[str, Decimal | str | float]:
    out = dict(payload)

    def _round_field(name: str) -> None:
        if name in out and out[name] is not None:
            out[name] = round_price(_to_decimal(out[name]), filters)

    for field in ("price", "stop_price"):
        _round_field(field)

    qty = _to_decimal(out.get("quantity")) if out.get("quantity") is not None else Decimal("0")
    price_for_notional = _to_decimal(out.get("price") or out.get("stop_price") or 0)
    _, adj_qty, _ = enforce_mins(price_for_notional, qty, filters)
    adj_qty = round_qty(adj_qty, filters)
    out["quantity"] = adj_qty

    return out


__all__ = ["round_price", "round_qty", "enforce_mins", "apply_symbol_filters"]
