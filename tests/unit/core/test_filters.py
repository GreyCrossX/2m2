from decimal import Decimal

from app.core.exchange.filters import build_symbol_filters, quantize_price, quantize_qty


EX_INFO = {
    "symbols": [
        {
            "symbol": "BTCUSDT",
            "filters": [
                {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001", "maxQty": "100"},
                {"filterType": "PRICE_FILTER", "tickSize": "0.10", "minPrice": "0.10", "maxPrice": "100000"},
                {"filterType": "MIN_NOTIONAL", "notional": "5"},
            ],
        },
        {
            "symbol": "ETHUSDT",
            "filters": [
                {"filterType": "LOT_SIZE", "stepSize": "0.01", "minQty": "0.01"},
                {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
            ],
        },
    ]
}


def test_build_symbol_filters_maps_types() -> None:
    result = build_symbol_filters(EX_INFO)
    assert "BTCUSDT" in result
    btc_filters = result["BTCUSDT"]
    assert "LOT_SIZE" in btc_filters
    assert btc_filters["PRICE_FILTER"]["tickSize"] == "0.10"


def test_quantize_qty_rounds_and_respects_minimum() -> None:
    filters = build_symbol_filters(EX_INFO)["BTCUSDT"]
    assert quantize_qty(filters, Decimal("0.0014")) == Decimal("0.001")
    assert quantize_qty(filters, Decimal("0.0005")) == Decimal("0")
    assert quantize_qty(filters, Decimal("5.678")) == Decimal("5.678").quantize(Decimal("0.001"))


def test_quantize_price_snaps_to_tick() -> None:
    filters = build_symbol_filters(EX_INFO)["BTCUSDT"]
    assert quantize_price(filters, Decimal("12345.678")) == Decimal("12345.6")
    assert quantize_price(filters, Decimal("0.08")) == Decimal("0.10")


def test_quantize_price_handles_none_and_zero() -> None:
    filters = build_symbol_filters(EX_INFO)["ETHUSDT"]
    assert quantize_price(filters, None) is None
    assert quantize_price(filters, Decimal("0")) == Decimal("0")
