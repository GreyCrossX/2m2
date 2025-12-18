from __future__ import annotations

import pytest

pytest.importorskip("binance_common")

from app.services.infrastructure.binance.binance_usds import (  # noqa: E402
    BinanceUSDS,
    BinanceUSDSConfig,
)


class _ApiResponse:
    def __init__(self, payload):
        self._payload = payload

    def data(self):
        return self._payload


class _RestWithCurrentAllOpenOrders:
    def __init__(self, payload):
        self._payload = payload

    def current_all_open_orders(self, **_kwargs):
        return _ApiResponse(self._payload)


class _RestWithOpenOrders:
    def __init__(self, payload):
        self._payload = payload

    def open_orders(self, **_kwargs):
        return _ApiResponse(self._payload)


def _client(rest_api) -> BinanceUSDS:
    cfg = BinanceUSDSConfig(api_key="k", api_secret="s", testnet=True)
    return BinanceUSDS(cfg, rest_api=rest_api)


def test_open_orders_uses_current_all_open_orders() -> None:
    payload = [{"orderId": 1}, {"orderId": 2}]
    client = _client(_RestWithCurrentAllOpenOrders(payload))
    assert client.open_orders(symbol="BTCUSDT") == payload


def test_open_orders_falls_back_to_open_orders() -> None:
    payload = [{"orderId": 9}]
    client = _client(_RestWithOpenOrders(payload))
    assert client.open_orders(symbol="BTCUSDT") == payload
