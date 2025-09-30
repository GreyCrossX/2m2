import sys
import types
import pytest

# Contract tests: verify our facade maps to the SDK the way Binance expects,
# without making network calls. We stub the SDK surface our code imports.

@pytest.fixture(autouse=True)
def _isolate_exchange(monkeypatch):
    """
    Install a fake 'binance_sdk_derivatives_trading_usds_futures' package tree
    matching the import path used by app.services.tasks.exchange.
    """
    pkg = "binance_sdk_derivatives_trading_usds_futures"
    root = sys.modules.get(pkg) or types.ModuleType(pkg)
    sys.modules[pkg] = root

    # --- Provide the exact submodule path our code imports from ---------------
    submod_name = f"{pkg}.derivatives_trading_usds_futures"
    submod = types.ModuleType(submod_name)
    sys.modules[submod_name] = submod

    # --- Also provide models enum path used by the facade ---------------------
    models_mod_name = f"{pkg}.rest_api.models"
    models_mod = types.ModuleType(models_mod_name)
    class NewOrderSideEnum:
        BUY = types.SimpleNamespace(value="BUY")
        SELL = types.SimpleNamespace(value="SELL")
    models_mod.NewOrderSideEnum = {"BUY": NewOrderSideEnum.BUY, "SELL": NewOrderSideEnum.SELL}
    sys.modules[models_mod_name] = models_mod

    # --- Config + client stub (lives in the submodule path) -------------------
    class ConfigurationRestAPI:
        def __init__(self, api_key="", api_secret="", base_path=""):
            self.api_key = api_key
            self.api_secret = api_secret
            self.base_path = base_path

    DERIV_URL = "https://fapi.binance.com"

    calls = {
        "new_order": [],
        "cancel_order": [],
        "open_orders": [],
        "positions": [],
        "change_leverage": [],
        "change_margin": [],
    }

    class _Resp:
        def __init__(self, data):
            self._data = data
        def data(self):
            return self._data

    class _Rest:
        def new_order(self, **kw):
            calls["new_order"].append(kw)
            return _Resp({"orderId": 12345, "clientOrderId": kw.get("newClientOrderId")})
        def cancel_order(self, **kw):
            calls["cancel_order"].append(kw)
            return _Resp({"status": "CANCELED", "orderId": kw.get("orderId")})
        def current_all_open_orders(self, **kw):
            calls["open_orders"].append(kw)
            return _Resp([{"symbol": kw.get("symbol", "BTCUSDT"), "orderId": 1}])
        def position_information(self, **kw):
            calls["positions"].append(kw)
            return _Resp([{"symbol": kw.get("symbol", "BTCUSDT"), "positionAmt": "0", "entryPrice": "0"}])
        def change_initial_leverage(self, **kw):
            calls["change_leverage"].append(kw)
            return _Resp({"leverage": kw.get("leverage")})
        def change_margin_type(self, **kw):
            calls["change_margin"].append(kw)
            return _Resp({"marginType": kw.get("marginType")})

    class DerivativesTradingUsdsFutures:
        def __init__(self, config_rest_api=None):
            self.rest_api = _Rest()

    # export into the submodule (exactly where the facade imports from)
    submod.ConfigurationRestAPI = ConfigurationRestAPI
    submod.DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL = DERIV_URL
    submod.DerivativesTradingUsdsFutures = DerivativesTradingUsdsFutures

    # Ensure a fresh import of the facade so it binds to the fake SDK
    sys.modules.pop("app.services.tasks.exchange", None)
    import app.services.tasks.exchange as exchange  # noqa: E402

    yield exchange, calls


@pytest.mark.unit
@pytest.mark.exchange
def test_new_order_maps_fields_correctly(_isolate_exchange):
    exchange, calls = _isolate_exchange
    out = exchange.new_order(
        user_id="u1",
        symbol="BTCUSDT",
        side="BUY",
        order_type="STOP_MARKET",
        quantity=0.01,
        reduce_only=True,
        stop_price=68000.5,
        working_type="MARK_PRICE",
        close_position=False,
        client_order_id="coid-123",
        positionSide="LONG",
        price=None,
        timeInForce=None,
        priceProtect=True,  # passthrough
    )
    assert out["ok"] is True
    assert len(calls["new_order"]) == 1
    sent = calls["new_order"][0]
    assert sent["symbol"] == "BTCUSDT"
    assert sent["side"] == "BUY"
    assert sent["type"] == "STOP_MARKET"
    assert sent["quantity"] == 0.01
    assert sent["reduceOnly"] is True
    assert sent["stopPrice"] == 68000.5
    assert sent["workingType"] == "MARK_PRICE"
    assert sent["closePosition"] is False
    assert sent["newClientOrderId"] == "coid-123"
    assert sent["positionSide"] == "LONG"
    assert sent["priceProtect"] is True


@pytest.mark.unit
@pytest.mark.exchange
def test_cancel_open_positions_leverage_margin(_isolate_exchange):
    exchange, calls = _isolate_exchange
    res = exchange.cancel_order("u1", symbol="BTCUSDT", order_id=123)
    assert res["ok"] is True and calls["cancel_order"][0]["orderId"] == 123
    _ = exchange.get_open_orders("u1")
    assert calls["open_orders"][-1] == {}
    _ = exchange.get_open_orders("u1", symbol="ETHUSDT")
    assert calls["open_orders"][-1]["symbol"] == "ETHUSDT"
    _ = exchange.get_positions("u1", symbol="ETHUSDT")
    assert calls["positions"][-1]["symbol"] == "ETHUSDT"
    _ = exchange.set_leverage("u1", symbol="BTCUSDT", leverage=7)
    assert calls["change_leverage"][-1] == {"symbol": "BTCUSDT", "leverage": 7}
    _ = exchange.set_margin_type("u1", symbol="BTCUSDT", margin_type="isolated")
    assert calls["change_margin"][-1] == {"symbol": "BTCUSDT", "marginType": "ISOLATED"}


@pytest.mark.unit
@pytest.mark.exchange
def test_new_order_error_passthrough(_isolate_exchange, monkeypatch):
    exchange, _calls = _isolate_exchange

    class _BoomRest:
        def new_order(self, **kw):
            raise RuntimeError("sdk exploded")

    class _BoomClient:
        def __init__(self):
            self.rest_api = _BoomRest()

    # Accept any args the facade passes (_client_for_user is cached and called with multiple args)
    monkeypatch.setattr(exchange, "_client_for_user", lambda *_, **__: _BoomClient())

    out = exchange.new_order(
        user_id="u1",
        symbol="BTCUSDT",
        side="SELL",
        order_type="MARKET",
    )
    assert out["ok"] is False
    assert "sdk exploded" in out["error"]


