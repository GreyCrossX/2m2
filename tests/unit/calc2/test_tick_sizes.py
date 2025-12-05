from decimal import Decimal
import json
from typing import Any

import pytest

from app.services.calc2.utils.tick_sizes import load_tick_sizes


class _FakeResponse:
    def __init__(self, payload: Any) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(
        self, exc_type, exc, tb
    ) -> None:  # pragma: no cover - nothing to clean
        return None


def test_load_tick_sizes_prefers_price_filter(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.1"},
                ],
                "pricePrecision": 2,
            },
            {
                "symbol": "ETHUSDT",
                "filters": [],  # no tickSize; should fall back to precision
                "pricePrecision": 3,
            },
        ]
    }

    def fake_urlopen(url: str, timeout: int):
        return _FakeResponse(payload)

    monkeypatch.setattr("app.services.calc2.utils.tick_sizes.urlopen", fake_urlopen)

    ticks = load_tick_sizes(
        ["BTCUSDT", "ETHUSDT", "MISSING"],
        exchange_info_url="http://example.test/exchangeInfo",
        fallback=Decimal("0.05"),
    )

    assert ticks["BTCUSDT"] == Decimal("0.1")
    assert ticks["ETHUSDT"] == Decimal("0.001")  # from pricePrecision
    assert ticks["MISSING"] == Decimal("0.05")  # fallback when symbol absent
