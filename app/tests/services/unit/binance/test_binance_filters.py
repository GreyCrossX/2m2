from __future__ import annotations

from decimal import Decimal

import pytest

pytestmark = pytest.mark.unit

from app.services.infrastructure.binance.utils.filters import (
    build_symbol_filters,
    quantize_price,
    quantize_qty,
)


def test_build_symbol_filters_handles_missing_entries() -> None:
    data = {"symbols": [{"symbol": "btcusdt", "filters": [{"filterType": "PRICE_FILTER", "tickSize": "0.1"}]}]}
    result = build_symbol_filters(data)
    assert "BTCUSDT" in result
    assert result["BTCUSDT"]["PRICE_FILTER"]["tickSize"] == "0.1"


@pytest.mark.parametrize(
    "raw, expected",
    [
        (Decimal("0.009"), Decimal("0")),
        (Decimal("0.02"), Decimal("0.02")),
        (Decimal("1.2345"), Decimal("1.23")),
        (Decimal("200"), Decimal("100")),
        (Decimal("-1"), Decimal("0")),
    ],
)
def test_quantize_qty_handles_edges(raw: Decimal, expected: Decimal) -> None:
    filters = {
        "LOT_SIZE": {
            "stepSize": "0.01",
            "minQty": "0.02",
            "maxQty": "100",
        }
    }
    quantized = quantize_qty(filters, raw)
    assert quantized == expected


@pytest.mark.parametrize(
    "raw, expected",
    [
        (None, None),
        (Decimal("0.004"), Decimal("0.05")),
        (Decimal("1.236"), Decimal("1.23")),
        (Decimal("1000"), Decimal("100")),
        (Decimal("-5"), Decimal("0")),
    ],
)
def test_quantize_price_handles_edges(raw: Decimal | None, expected: Decimal | None) -> None:
    filters = {
        "PRICE_FILTER": {
            "tickSize": "0.01",
            "minPrice": "0.05",
            "maxPrice": "100",
        }
    }
    quantized = quantize_price(filters, raw)
    assert quantized == expected
