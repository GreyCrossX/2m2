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
    # n = floor(value / step) * step
    n = (value / step).to_integral_value(rounding=ROUND_DOWN) * step
    return n


def round_price(price: Decimal, filters: ExchangeFilters) -> Decimal:
    tick = filters.get("tick_size", None)
    if tick is None:
        return price
    return _quantize_step(price, tick)


def round_qty(qty: Decimal, filters: ExchangeFilters) -> Decimal:
    step = filters.get("step_size", None)
    if step is None:
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
    Ensure min_qty and min_notional are satisfied by flooring qty if needed.
    Returns (price, qty, warnings).
    """
    warnings: list[str] = []

    min_qty = filters.get("min_qty")
    if isinstance(min_qty, Decimal) and qty < min_qty:
        qty = min_qty
        warnings.append(f"qty increased to min_qty={min_qty}")

    min_notional = filters.get("min_notional")
    if isinstance(min_notional, Decimal):
        notional = price * qty
        if notional < min_notional:
            # increase qty to meet min notional (still floor to step on caller)
            needed = (min_notional / price)
            if needed > qty:
                qty = needed
                warnings.append(
                    f"qty increased to satisfy min_notional={min_notional}"
                )

    return price, qty, warnings


def apply_symbol_filters(payload: OrderPayload, filters: ExchangeFilters) -> OrderPayload:
    """
    Return a NEW payload with price-like and qty fields rounded/enforced.

    - Rounds `stop_price` (if present) by tick_size
    - Rounds `quantity` by step_size
    - Enforces min_qty / min_notional (using stop_price as a proxy for price)
    """
    out: OrderPayload = dict(payload)  # copy

    qty = out.get("quantity")
    stop = out.get("stop_price")  # most of our orders are STOP_* or TP_*

    if stop is not None:
        stop = round_price(Decimal(stop), filters)
        out["stop_price"] = stop

    if qty is not None:
        qty = round_qty(Decimal(qty), filters)

    if stop is not None and qty is not None:
        stop, qty, _ = enforce_mins(Decimal(stop), Decimal(qty), filters)
        qty = round_qty(qty, filters)

    if qty is not None:
        out["quantity"] = qty

    return out
