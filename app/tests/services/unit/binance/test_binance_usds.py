from __future__ import annotations

from typing import Any, Dict, List

import pytest

pytestmark = pytest.mark.unit
from binance_common import errors as binance_errors
import requests

from app.services.domain.exceptions import (
    DomainAuthError,
    DomainBadRequest,
    DomainExchangeDown,
    DomainRateLimit,
)
from app.services.infrastructure.binance.binance_usds import BinanceUSDS, BinanceUSDSConfig


class _FakeResponse:
    def __init__(self, payload: Any) -> None:
        self._payload = payload

    def data(self) -> Any:
        return self._payload


class _FakeRest:
    def __init__(self) -> None:
        self.calls: List[tuple[str, Dict[str, Any]]] = []
        self.exchange_information = self._record("exchange_information", {})
        self.get_current_position_mode = self._record("get_position_mode", {"dualSidePosition": False})
        self.change_position_mode = self._record("change_position_mode", {})
        self.position_information_v3 = self._record("position_information_v3", [])
        self.account_information_v3 = self._record("account_information_v3", {"totalInitialMargin": "0"})
        self.futures_account_balance_v3 = self._record("futures_account_balance_v3", [])
        self.new_order = self._record("new_order", {"orderId": 1})
        self.query_order = self._record("query_order", {"status": "FILLED"})
        self.cancel_order = self._record("cancel_order", {"status": "CANCELED"})
        self.change_initial_leverage = self._record("change_initial_leverage", {"leverage": 5})

    def _record(self, name: str, result: Any):
        def handler(**kwargs: Any) -> Any:
            self.calls.append((name, kwargs))
            return _FakeResponse(result)

        return handler


@pytest.fixture()
def adapter() -> BinanceUSDS:
    rest = _FakeRest()
    cfg = BinanceUSDSConfig(api_key="k", api_secret="s")
    return BinanceUSDS(cfg, rest_api=rest)


def test_translate_params_aliases_and_none(adapter: BinanceUSDS) -> None:
    payload = adapter._translate_params(  # type: ignore[attr-defined]
        {
            "timeInForce": "GTC",
            "reduceOnly": True,
            "positionSide": "BOTH",
            "foo": None,
        },
        adapter._ORDER_PARAM_ALIASES,
    )
    assert payload == {
        "time_in_force": "GTC",
        "reduce_only": True,
        "position_side": "BOTH",
    }


def test_normalize_response_handles_data_callable(adapter: BinanceUSDS) -> None:
    resp = _FakeResponse({"foo": 1})
    assert adapter._normalize_response(resp) == {"foo": 1}  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    "error_cls, expected",
    [
        (binance_errors.BadRequestError, DomainBadRequest),
        (binance_errors.RequiredError, DomainBadRequest),
        (binance_errors.ClientError, DomainBadRequest),
        (binance_errors.ForbiddenError, DomainAuthError),
        (binance_errors.TooManyRequestsError, DomainRateLimit),
        (binance_errors.ServerError, DomainExchangeDown),
        (requests.exceptions.ProxyError, DomainExchangeDown),
        (requests.exceptions.Timeout, DomainExchangeDown),
        (requests.exceptions.RequestException, DomainExchangeDown),
    ],
)
def test_map_exception_matches_domain(error_cls: type[Exception], expected: type[Exception], adapter: BinanceUSDS) -> None:
    exc = error_cls("boom")
    mapped = adapter._map_exception(exc)  # type: ignore[attr-defined]
    assert isinstance(mapped, expected)


def test_call_with_retries_retries_then_succeeds(adapter: BinanceUSDS) -> None:
    attempts: List[int] = []

    def flaky_call(**_: Any) -> _FakeResponse:
        if len(attempts) < 2:
            attempts.append(1)
            raise DomainRateLimit("throttle")
        return _FakeResponse({"ok": True})

    result = adapter._call_with_retries(flaky_call)  # type: ignore[attr-defined]
    assert result == {"ok": True}
    assert len(attempts) == 2


def test_new_order_translates_and_calls_rest(adapter: BinanceUSDS) -> None:
    result = adapter.new_order(symbol="btcusdt", timeInForce="GTC", closePosition=True)
    assert result["orderId"] == 1


def test_change_leverage_retries(adapter: BinanceUSDS) -> None:
    adapter.change_leverage("btcusdt", 10)
    # last call should be to change_initial_leverage
    assert adapter._rest.calls[-1][0] == "change_initial_leverage"  # type: ignore[attr-defined]
