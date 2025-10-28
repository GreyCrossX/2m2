from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from typing import Any, Callable, Mapping

import requests
from binance_common import errors as binance_errors
from binance_sdk_derivatives_trading_usds_futures.derivatives_trading_usds_futures import (
    ConfigurationRestAPI,
    DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL,
    DerivativesTradingUsdsFutures,
)

try:  # pragma: no cover - fallback for SDKs missing the constant
    from binance_sdk_derivatives_trading_usds_futures.derivatives_trading_usds_futures import (
        DERIVATIVES_TRADING_USDS_FUTURES_REST_API_TESTNET_URL,
    )
except Exception:  # pragma: no cover
    DERIVATIVES_TRADING_USDS_FUTURES_REST_API_TESTNET_URL = "https://testnet.binancefuture.com"

from app.services.worker.domain.exceptions import (
    DomainAuthError,
    DomainBadRequest,
    DomainExchangeDown,
    DomainRateLimit,
)

log = logging.getLogger("infrastructure.exchange.binance_usds")


@dataclass(slots=True)
class BinanceUSDSConfig:
    api_key: str
    api_secret: str
    testnet: bool = False
    timeout_ms: int = 5000


class BinanceUSDS:
    """Thin synchronous adapter over the modular Binance USDⓈ-M futures SDK."""

    _ORDER_PARAM_ALIASES: Mapping[str, str] = {
        "timeInForce": "time_in_force",
        "reduceOnly": "reduce_only",
        "positionSide": "position_side",
        "newClientOrderId": "new_client_order_id",
        "stopPrice": "stop_price",
        "closePosition": "close_position",
        "activationPrice": "activation_price",
        "callbackRate": "callback_rate",
        "workingType": "working_type",
        "priceProtect": "price_protect",
        "newOrderRespType": "new_order_resp_type",
        "priceMatch": "price_match",
        "selfTradePreventionMode": "self_trade_prevention_mode",
        "goodTillDate": "good_till_date",
        "recvWindow": "recv_window",
    }

    _QUERY_PARAM_ALIASES: Mapping[str, str] = {
        "orderId": "order_id",
        "origClientOrderId": "orig_client_order_id",
        "recvWindow": "recv_window",
        "symbol": "symbol",
    }

    def __init__(
        self,
        config: BinanceUSDSConfig,
        *,
        rest_api: Any | None = None,
        _client: DerivativesTradingUsdsFutures | None = None,
    ) -> None:
        if not config.api_key or not config.api_secret:
            raise ValueError("BinanceUSDSConfig requires api_key and api_secret")

        self._config = config
        base_path = (
            DERIVATIVES_TRADING_USDS_FUTURES_REST_API_TESTNET_URL
            if config.testnet
            else DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL
        )

        if rest_api is not None:
            self._rest = rest_api
        else:
            if _client is None:
                configuration = ConfigurationRestAPI(
                    api_key=config.api_key,
                    api_secret=config.api_secret,
                    base_path=base_path,
                    timeout=config.timeout_ms,
                )
                _client = DerivativesTradingUsdsFutures(config_rest_api=configuration)
            self._rest = _client.rest_api

        self._timeout_ms = config.timeout_ms
        self._base_path = base_path

    # ------------------------------------------------------------------
    # Normalisation helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _normalize_value(value: Any) -> Any:
        if value is None:
            return None
        if hasattr(value, "model_dump"):
            try:
                dumped = value.model_dump(mode="json")
            except TypeError:
                dumped = value.model_dump()
            return BinanceUSDS._normalize_value(dumped)
        if isinstance(value, list):
            return [BinanceUSDS._normalize_value(item) for item in value]
        if isinstance(value, tuple):
            return [BinanceUSDS._normalize_value(item) for item in value]
        if isinstance(value, dict):
            return {k: BinanceUSDS._normalize_value(v) for k, v in value.items()}
        return value

    def _normalize_response(self, resp: Any) -> Any:
        if resp is None:
            return None
        data = resp
        data_attr = getattr(resp, "data", None)
        if callable(data_attr):
            data = data_attr()
        elif data_attr is not None:
            data = data_attr
        return self._normalize_value(data)

    # ------------------------------------------------------------------
    # Error mapping & retries
    # ------------------------------------------------------------------
    def _map_exception(self, exc: Exception) -> Exception:
        if isinstance(exc, (binance_errors.BadRequestError, binance_errors.RequiredError, binance_errors.ClientError)):
            return DomainBadRequest(str(exc))
        if isinstance(exc, (binance_errors.UnauthorizedError, binance_errors.ForbiddenError)):
            return DomainAuthError(str(exc))
        if isinstance(exc, (binance_errors.TooManyRequestsError, binance_errors.RateLimitBanError)):
            return DomainRateLimit(str(exc))
        if isinstance(exc, (binance_errors.ServerError, binance_errors.NetworkError)):
            return DomainExchangeDown(str(exc))
        if isinstance(exc, requests.exceptions.ProxyError):
            return DomainExchangeDown("Proxy error communicating with Binance")
        if isinstance(exc, requests.exceptions.Timeout):
            return DomainExchangeDown("Request to Binance timed out")
        if isinstance(exc, requests.exceptions.RequestException):
            return DomainExchangeDown(str(exc))
        return exc

    def _wrap(self, func: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Any:
        try:
            raw = func(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001 - map all SDK exceptions
            mapped = self._map_exception(exc)
            if mapped is exc:
                raise
            raise mapped from exc
        return self._normalize_response(raw)

    def _call_with_retries(
        self,
        func: Callable[..., Any],
        /,
        *args: Any,
        retries: int = 2,
        delay: float = 0.25,
        **kwargs: Any,
    ) -> Any:
        attempt = 0
        while True:
            try:
                return self._wrap(func, *args, **kwargs)
            except (DomainRateLimit, DomainExchangeDown) as exc:
                if attempt >= retries:
                    raise
                sleep_for = delay * (2**attempt)
                log.warning(
                    "BinanceUSDS retry | func=%s attempt=%s/%s sleep=%.2fs reason=%s",
                    getattr(func, "__name__", str(func)),
                    attempt + 1,
                    retries + 1,
                    sleep_for,
                    exc,
                )
                time.sleep(sleep_for)
                attempt += 1

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def exchange_info(self) -> dict:
        return self._wrap(self._rest.exchange_information)

    def get_position_mode(self) -> dict:
        return self._wrap(self._rest.get_current_position_mode)

    def set_position_mode(self, dual_side: bool) -> dict:
        value = "true" if dual_side else "false"
        return self._wrap(self._rest.change_position_mode, dual_side_position=value)

    def position_information(self, symbol: str | None = None) -> list[dict]:
        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol.upper()
        data = self._wrap(self._rest.position_information_v3, **params)
        if not isinstance(data, list):
            return [] if data is None else [data]
        return data

    def account_information(self) -> dict:
        return self._wrap(self._rest.account_information_v3)

    def account_balance(self) -> list[dict]:
        data = self._wrap(self._rest.futures_account_balance_v3)
        if isinstance(data, list):
            return data
        return [] if data is None else [data]

    # Orders -----------------------------------------------------------------
    def new_order(self, **params: Any) -> dict:
        prepared = self._translate_params(params, self._ORDER_PARAM_ALIASES)
        return self._call_with_retries(self._rest.new_order, **prepared)

    def query_order(self, **params: Any) -> dict:
        prepared = self._translate_params(params, self._QUERY_PARAM_ALIASES)
        return self._call_with_retries(self._rest.query_order, **prepared)

    def cancel_order(self, **params: Any) -> dict:
        prepared = self._translate_params(params, self._QUERY_PARAM_ALIASES)
        return self._call_with_retries(self._rest.cancel_order, **prepared)

    def change_leverage(self, symbol: str, leverage: int) -> dict:
        return self._wrap(
            self._rest.change_initial_leverage,
            symbol=symbol.upper(),
            leverage=int(leverage),
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @classmethod
    def _translate_params(
        cls,
        params: Mapping[str, Any],
        aliases: Mapping[str, str],
    ) -> dict[str, Any]:
        if not params:
            return {}
        out: dict[str, Any] = {}
        for key, value in params.items():
            dest = aliases.get(key, key)
            if dest == "symbol" and isinstance(value, str):
                out[dest] = value.upper()
            else:
                out[dest] = value
        return out

    @property
    def base_path(self) -> str:
        return self._base_path

    @property
    def timeout_ms(self) -> int:
        return self._timeout_ms
