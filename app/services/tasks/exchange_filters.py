"""
Pure helpers to make order numbers exchange-valid:
- Round by tick/step
- Enforce min qty / min notional
- Apply to order payloads without side effects

No network calls; deterministic.
"""

from __future__ import annotations

from decimal import Decimal, ROUND_DOWN
from typing import Tuple

from .contracts import ExchangeFilters, OrderPayload


# ──────────────────────────────────────────────────────────────────────────────
# Core rounding primitives
# ──────────────────────────────────────────────────────────────────────────────

def _quantize_step(value: Decimal, step: Decimal) -> Decimal:
    """
    Floor-round value to the nearest multiple of `step` (exchange safe).
    """
    if step <= 0:
        return value
    n = (value / step).to_integral_value(rounding=ROUND_DOWN) * step
    return n


def round_price(price: Decimal, filters: ExchangeFilters) -> Decimal:
    tick = filters.get("tick_size")
    if not isinstance(tick, Decimal) or tick <= 0:
        return price
    return _quantize_step(price, tick)


def round_qty(qty: Decimal, filters: ExchangeFilters) -> Decimal:
    step = filters.get("step_size")
    if not isinstance(step, Decimal) or step <= 0:
        return qty
    return _quantize_step(qty, step)


# ──────────────────────────────────────────────────────────────────────────────
# Higher-level application helpers
# ──────────────────────────────────────────────────────────────────────────────

def enforce_mins(
    price: Decimal,
    qty: Decimal,
    filters: ExchangeFilters,
) -> Tuple[Decimal, Decimal, list[str]]:
    """
    Ensure min_qty and min_notional are satisfied by adjusting qty upward.
    Returns (price, qty, warnings).
    """
    warnings: list[str] = []

    min_qty = filters.get("min_qty")
    if isinstance(min_qty, Decimal) and qty < min_qty:
        qty = min_qty
        warnings.append(f"qty increased to min_qty={min_qty}")

    min_notional = filters.get("min_notional")
    if isinstance(min_notional, Decimal) and price > 0:
        notional = price * qty
        if notional < min_notional:
            needed = (min_notional / price)
            if needed > qty:
                qty = needed
                warnings.append(f"qty increased to satisfy min_notional={min_notional}")

    return price, qty, warnings


def apply_symbol_filters(payload: OrderPayload, filters: ExchangeFilters) -> OrderPayload:
    """
    Return a NEW payload with price-like and qty fields rounded/enforced.

    - Rounds `price` (for LIMIT/STOP/TP) and/or `stop_price` by tick_size
    - Rounds `quantity` by step_size
    - Enforces min_qty / min_notional using `price` if present, else `stop_price`
    """
    out: OrderPayload = dict(payload)

    qty = out.get("quantity")
    price = out.get("price")
    stop = out.get("stop_price")

    if isinstance(price, (int, float, Decimal)):
        price = round_price(Decimal(str(price)), filters)
        out["price"] = price

    if isinstance(stop, (int, float, Decimal)):
        stop = round_price(Decimal(str(stop)), filters)
        out["stop_price"] = stop

    if isinstance(qty, (int, float, Decimal)):
        qty = round_qty(Decimal(str(qty)), filters)

        ref_price = price if isinstance(price, Decimal) else stop if isinstance(stop, Decimal) else None
        if isinstance(ref_price, Decimal):
            _, qty, _ = enforce_mins(ref_price, qty, filters)
            qty = round_qty(qty, filters)

        out["quantity"] = qty

    return out
