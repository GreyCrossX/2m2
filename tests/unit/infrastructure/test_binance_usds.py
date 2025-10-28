from __future__ import annotations

from dataclasses import dataclass

import pytest
from binance_common import errors as binance_errors

from app.infrastructure.exchange.binance_usds import BinanceUSDS, BinanceUSDSConfig
from app.services.worker.domain.exceptions import (
    DomainAuthError,
    DomainBadRequest,
    DomainExchangeDown,
    DomainRateLimit,
)


@dataclass
class _DummyModel:
    payload: object

    def model_dump(self, mode: str = "python") -> object:  # noqa: D401 - tiny helper
        return self.payload


class _DummyResponse:
    def __init__(self, payload: object) -> None:
        self._payload = payload

    def data(self) -> object:
        return self._payload


class _RecordingRestAPI:
    def __init__(self) -> None:
        self.called_with: dict | None = None

    def exchange_information(self) -> _DummyResponse:
        return _DummyResponse(_DummyModel({"symbols": []}))

    def get_current_position_mode(self) -> _DummyResponse:
        return _DummyResponse(_DummyModel({"dualSidePosition": True}))

    def change_position_mode(self, **_: object) -> _DummyResponse:
        return _DummyResponse(_DummyModel({"dualSidePosition": False}))

    def position_information_v3(self, **_: object) -> _DummyResponse:
        entries = [_DummyModel({"symbol": "BTCUSDT", "positionAmt": "0"})]
        return _DummyResponse(_DummyModel(entries))

    def account_information_v3(self) -> _DummyResponse:
        return _DummyResponse(_DummyModel({"totalInitialMargin": "10"}))

    def futures_account_balance_v3(self) -> _DummyResponse:
        balances = [_DummyModel({"asset": "USDT", "balance": "15"})]
        return _DummyResponse(_DummyModel(balances))

    def new_order(self, **kwargs: object) -> _DummyResponse:
        self.called_with = kwargs
        return _DummyResponse(_DummyModel({"orderId": 123}))

    def query_order(self, **kwargs: object) -> _DummyResponse:
        self.called_with = kwargs
        return _DummyResponse(_DummyModel({"status": "NEW"}))

    def cancel_order(self, **kwargs: object) -> _DummyResponse:
        self.called_with = kwargs
        return _DummyResponse(_DummyModel({"status": "CANCELED"}))

    def change_initial_leverage(self, **_: object) -> _DummyResponse:
        return _DummyResponse(_DummyModel({"leverage": 5}))


@pytest.fixture
def adapter() -> tuple[BinanceUSDS, _RecordingRestAPI]:
    rest = _RecordingRestAPI()
    cfg = BinanceUSDSConfig(api_key="k", api_secret="s")
    return BinanceUSDS(cfg, rest_api=rest), rest


def test_normalizes_model_dump(adapter: tuple[BinanceUSDS, _RecordingRestAPI]) -> None:
    gateway, rest = adapter

    info = gateway.exchange_info()
    mode = gateway.get_position_mode()
    positions = gateway.position_information("BTCUSDT")
    order = gateway.new_order(symbol="btcusdt", timeInForce="GTC", reduceOnly=True)

    assert info == {"symbols": []}
    assert mode == {"dualSidePosition": True}
    assert positions == [{"symbol": "BTCUSDT", "positionAmt": "0"}]
    assert rest.called_with["time_in_force"] == "GTC"
    assert rest.called_with["reduce_only"] is True
    assert rest.called_with["symbol"] == "BTCUSDT"
    assert order == {"orderId": 123}


def test_query_cancel_translate_params(adapter: tuple[BinanceUSDS, _RecordingRestAPI]) -> None:
    gateway, rest = adapter
    gateway.query_order(symbol="btcusdt", orderId=42)
    assert rest.called_with == {"symbol": "BTCUSDT", "order_id": 42}
    gateway.cancel_order(symbol="btcusdt", origClientOrderId="abc")
    assert rest.called_with == {"symbol": "BTCUSDT", "orig_client_order_id": "abc"}


def test_wrap_maps_exceptions(adapter: tuple[BinanceUSDS, _RecordingRestAPI]) -> None:
    gateway, _ = adapter

    with pytest.raises(DomainBadRequest):
        gateway._wrap(lambda: (_ for _ in ()).throw(binance_errors.BadRequestError("bad")))

    with pytest.raises(DomainAuthError):
        gateway._wrap(lambda: (_ for _ in ()).throw(binance_errors.UnauthorizedError("no")))

    with pytest.raises(DomainRateLimit):
        gateway._wrap(lambda: (_ for _ in ()).throw(binance_errors.TooManyRequestsError("slow")))

    with pytest.raises(DomainExchangeDown):
        gateway._wrap(lambda: (_ for _ in ()).throw(binance_errors.ServerError("oops", status_code=500)))
