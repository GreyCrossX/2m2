from __future__ import annotations

from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict


def _D(x: Any) -> Decimal:
    if isinstance(x, Decimal):
        return x
    if x is None or x == "":
        return Decimal("0")
    return Decimal(str(x))


def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    """
    Floor 'value' to the nearest multiple of 'step' (Binance requires ROUND_DOWN).
    Preserve the exponent of 'step' so the result has the right number of decimals.
    """
    if step <= 0:
        return value
    multiples = (value / step).to_integral_value(rounding=ROUND_DOWN)
    return (multiples * step).quantize(step, rounding=ROUND_DOWN)


def _quantize_to_precision(value: Decimal, digits: int) -> Decimal:
    """
    Fallback when tick/step is missing: clamp the number of decimal places by precision.
    Example: digits=3 -> quantize to Decimal('0.001').
    """
    if digits < 0:
        digits = 0
    quantum = Decimal(1).scaleb(-digits)  # 10**(-digits)
    return value.quantize(quantum, rounding=ROUND_DOWN)


def build_symbol_filters(
    exchange_info: Dict[str, Any],
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Build a map: { SYMBOL: { FILTER_TYPE: filter_dict, 'META': {...} } }
    Ensures we have fast access to LOT_SIZE, PRICE_FILTER, (MIN_)NOTIONAL and useful META.
    Works with Binance Futures exchangeInfo structure.
    """
    fallback_ticks = {
        "BTCUSDT": "0.1",
        "ETHUSDT": "0.01",
    }
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}

    symbols = exchange_info.get("symbols") or exchange_info.get("data") or []
    for s in symbols:
        sym = s.get("symbol")
        if not sym:
            continue

        filt_map: Dict[str, Dict[str, Any]] = {}
        for f in s.get("filters", []):
            ftype = f.get("filterType")
            if ftype:
                filt_map[ftype] = f

        # Fill missing PRICE_FILTER.tickSize from known fallbacks when absent
        pf = filt_map.get("PRICE_FILTER")
        if pf is None:
            pf = {"filterType": "PRICE_FILTER"}
            filt_map["PRICE_FILTER"] = pf
        fallback_tick = fallback_ticks.get(str(sym).upper())
        tick = pf.get("tickSize")
        if fallback_tick:
            # Always prefer known futures tick size for these symbols
            pf["tickSize"] = fallback_tick
        elif tick in (None, "", "0"):
            # Secondary guard for other symbols: leave empty, quantize_price will use precision fallback
            pf["tickSize"] = tick

        # Normalize NOTIONAL name across APIs
        if "NOTIONAL" in filt_map and "MIN_NOTIONAL" not in filt_map:
            filt_map["MIN_NOTIONAL"] = filt_map["NOTIONAL"]

        # Carry meta precisions as a fallback when tick/step omitted by some SDKs
        meta = {
            "pricePrecision": s.get("pricePrecision"),
            "quantityPrecision": s.get("quantityPrecision"),
            "baseAssetPrecision": s.get("baseAssetPrecision"),
            "quotePrecision": s.get("quotePrecision"),
        }
        filt_map["META"] = meta
        out[sym.upper()] = filt_map

    return out


def quantize_qty(filters: Dict[str, Dict[str, Any]], quantity: Decimal) -> Decimal:
    """
    Quantize/floor quantity using LOT_SIZE.stepSize, or fall back to quantityPrecision/meta when needed.
    Returns 0 if the floored qty falls below LOT_SIZE.minQty (so caller can treat as invalid/too small).

    NOTE: Enforces a maximum of 3 decimal places for all quantities to comply with Binance precision rules.
    """
    qty = _D(quantity)

    lot = filters.get("LOT_SIZE", {})
    step = _D(lot.get("stepSize", "0"))
    min_qty = _D(lot.get("minQty", "0"))
    max_qty = _D(lot.get("maxQty", "0"))

    if step > 0:
        q_qty = _floor_to_step(qty, step)
    else:
        # Fallback: precision-based floor (some SDKs omit stepSize but give quantityPrecision)
        meta = filters.get("META", {}) or {}
        qp = meta.get("quantityPrecision")
        if qp is None:
            # final defensive fallback to 6 decimals
            q_qty = _quantize_to_precision(qty, 6)
        else:
            try:
                q_qty = _quantize_to_precision(qty, int(qp))
            except Exception:
                q_qty = _quantize_to_precision(qty, 6)

    # Enforce LOT_SIZE bounds when present
    if min_qty > 0 and q_qty < min_qty:
        return Decimal("0")
    if max_qty > 0 and q_qty > max_qty:
        q_qty = _floor_to_step(max_qty, step) if step > 0 else max_qty

    # Enforce at most 3 decimal places (truncate, not round up)
    # This prevents "Precision is over the maximum defined for this asset" errors
    if q_qty > 0:
        q_qty = q_qty.quantize(Decimal("0.001"), rounding=ROUND_DOWN)

    return q_qty


def quantize_price(
    filters: Dict[str, Dict[str, Any]], price: Decimal | None
) -> Decimal | None:
    """
    Quantize/floor price using PRICE_FILTER.tickSize, or fall back to pricePrecision/meta when needed.
    """
    if price is None:
        return None

    prc = _D(price)
    pf = filters.get("PRICE_FILTER", {})
    tick = _D(pf.get("tickSize", "0"))
    min_price = _D(pf.get("minPrice", "0"))
    max_price = _D(pf.get("maxPrice", "0"))

    if tick > 0:
        q_price = prc.quantize(tick, rounding=ROUND_DOWN)
    else:
        # Fallback: precision-based floor (some SDKs omit tickSize but give pricePrecision)
        meta = filters.get("META", {}) or {}
        pp = meta.get("pricePrecision")
        if pp is None:
            # final defensive fallback to 2 decimals
            q_price = _quantize_to_precision(prc, 2)
        else:
            try:
                q_price = _quantize_to_precision(prc, int(pp))
            except Exception:
                q_price = _quantize_to_precision(prc, 2)

    # Respect min/max bounds where present
    if min_price > 0 and q_price < min_price:
        q_price = (
            min_price if tick <= 0 else min_price.quantize(tick, rounding=ROUND_DOWN)
        )
    if max_price > 0 and q_price > max_price:
        q_price = (
            max_price if tick <= 0 else max_price.quantize(tick, rounding=ROUND_DOWN)
        )

    return q_price
