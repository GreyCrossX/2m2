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


@pytest.mark.exchange
def test_new_order_limit_with_price_and_tif_drops_nones(_isolate_exchange):
    """
    LIMIT orders must pass price + timeInForce and must NOT include any None params.
    """
    exchange, calls = _isolate_exchange

    out = exchange.new_order(
        user_id="u1",
        symbol="ETHUSDT",
        side="SELL",
        order_type="LIMIT",
        quantity=0.5,
        price=2500.10,
        timeInForce="GTC",
        # These are all None on purpose — the facade should drop them
        stop_price=None,
        working_type=None,
        reduce_only=None,
        close_position=None,
        client_order_id="coid-LIM-1",
        positionSide=None,
        priceProtect=None,
    )
    assert out["ok"] is True
    assert len(calls["new_order"]) >= 1
    sent = calls["new_order"][-1]

    # Required fields for LIMIT
    assert sent["symbol"] == "ETHUSDT"
    assert sent["side"] == "SELL"
    assert sent["type"] == "LIMIT"
    assert sent["quantity"] == 0.5
    assert sent["price"] == 2500.10
    assert sent["timeInForce"] == "GTC"

    # None-valued fields should be omitted entirely
    for k in ("stopPrice", "workingType", "reduceOnly", "closePosition",
              "positionSide", "priceProtect"):
        assert k not in sent


@pytest.mark.exchange
def test_new_order_stop_and_take_profit_limit_mapping(_isolate_exchange):
    """
    Non-market STOP/TP should carry stopPrice + price + timeInForce and correct type.
    """
    exchange, calls = _isolate_exchange

    # STOP (stop-limit)
    out1 = exchange.new_order(
        user_id="u1",
        symbol="BTCUSDT",
        side="BUY",
        order_type="STOP",
        quantity=0.01,
        stop_price=101.50,
        price=101.60,
        timeInForce="GTC",
        client_order_id="coid-STOP-LIM",
    )
    assert out1["ok"] is True
    sent1 = calls["new_order"][-1]
    assert sent1["type"] == "STOP"
    assert sent1["stopPrice"] == 101.50
    assert sent1["price"] == 101.60
    assert sent1["timeInForce"] == "GTC"

    # TAKE_PROFIT (tp-limit)
    out2 = exchange.new_order(
        user_id="u1",
        symbol="BTCUSDT",
        side="SELL",
        order_type="TAKE_PROFIT",
        quantity=0.02,
        stop_price=98.40,
        price=98.30,
        timeInForce="GTC",
        client_order_id="coid-TP-LIM",
    )
    assert out2["ok"] is True
    sent2 = calls["new_order"][-1]
    assert sent2["type"] == "TAKE_PROFIT"
    assert sent2["stopPrice"] == 98.40
    assert sent2["price"] == 98.30
    assert sent2["timeInForce"] == "GTC"

@pytest.mark.exchange
def test_market_variants_drop_price_tif_even_if_provided(_isolate_exchange):
    """
    STOP_MARKET / TAKE_PROFIT_MARKET must not send price/timeInForce even if caller supplies them.
    They *should* send stopPrice (+ workingType/priceProtect if given).
    """
    exchange, calls = _isolate_exchange

    # STOP_MARKET
    out1 = exchange.new_order(
        user_id="u1",
        symbol="BTCUSDT",
        side="BUY",
        order_type="STOP_MARKET",
        quantity=0.01,
        stop_price=68000.5,
        # Caller mistakenly supplies these – facade should drop them
        price=68100.0,
        timeInForce="GTC",
        # passthroughs should remain
        working_type="MARK_PRICE",
        priceProtect=True,
    )
    assert out1["ok"] is True
    sent1 = calls["new_order"][-1]
    assert sent1["type"] == "STOP_MARKET"
    assert sent1["stopPrice"] == 68000.5
    assert sent1["workingType"] == "MARK_PRICE"
    assert sent1["priceProtect"] is True
    # ensure dropped
    assert "price" not in sent1
    assert "timeInForce" not in sent1

    # TAKE_PROFIT_MARKET
    out2 = exchange.new_order(
        user_id="u1",
        symbol="BTCUSDT",
        side="SELL",
        order_type="TAKE_PROFIT_MARKET",
        quantity=0.02,
        stop_price=67000.0,
        price=66950.0,          # should be dropped
        timeInForce="IOC",      # should be dropped
        working_type="LAST_PRICE",
    )
    assert out2["ok"] is True
    sent2 = calls["new_order"][-1]
    assert sent2["type"] == "TAKE_PROFIT_MARKET"
    assert sent2["stopPrice"] == 67000.0
    assert sent2["workingType"] == "LAST_PRICE"
    assert "price" not in sent2
    assert "timeInForce" not in sent2


@pytest.mark.exchange
def test_cancel_by_client_order_id_maps_correctly(_isolate_exchange):
    """
    Cancel should map orig_client_order_id -> origClientOrderId and omit orderId.
    """
    exchange, calls = _isolate_exchange

    res = exchange.cancel_order(
        "u1",
        symbol="ETHUSDT",
        orig_client_order_id="cli-123",
    )
    assert res["ok"] is True

    sent = calls["cancel_order"][-1]
    assert sent["symbol"] == "ETHUSDT"
    assert sent["origClientOrderId"] == "cli-123"
    assert "orderId" not in sent

import types
import pytest

@pytest.mark.exchange
def test_limit_includes_price_and_tif_omits_stopprice(_isolate_exchange):
    """
    LIMIT orders must forward price/timeInForce and omit stopPrice.
    """
    exchange, calls = _isolate_exchange

    out = exchange.new_order(
        user_id="u1",
        symbol="BTCUSDT",
        side="SELL",
        order_type="LIMIT",
        quantity=0.02,
        price=12345.6,
        timeInForce="GTC",
        # No stop fields for LIMIT
    )
    assert out["ok"] is True
    sent = calls["new_order"][-1]
    assert sent["type"] == "LIMIT"
    assert sent["symbol"] == "BTCUSDT"
    assert sent["side"] == "SELL"
    assert sent["quantity"] == 0.02
    assert sent["price"] == 12345.6
    assert sent["timeInForce"] == "GTC"
    assert "stopPrice" not in sent


@pytest.mark.exchange
def test_set_creds_lookup_is_used_and_cache_cleared(_isolate_exchange, monkeypatch):
    """
    set_creds_lookup should feed _client_for_user with the resolved creds/base_url,
    and clear any prior cache so subsequent calls use the new values.
    """
    exchange, _calls = _isolate_exchange

    recorded = []

    class _TinyRest:
        def new_order(self, **kw):
            # return shape matching our facade's extractor
            return types.SimpleNamespace(data=lambda: {"orderId": 1, "clientOrderId": kw.get("newClientOrderId")})

    def fake_client_for_user(user_id, api_key, api_secret, base_url):
        recorded.append((user_id, api_key, api_secret, base_url))
        return types.SimpleNamespace(rest_api=_TinyRest())

    # Patch the cached factory so we can assert the creds/base_url that reach it
    monkeypatch.setattr(exchange, "_client_for_user", fake_client_for_user)

    # Install a creds lookup that returns custom creds & base url
    exchange.set_creds_lookup(lambda uid: ("k-NEW", "s-NEW", "https://custom-base"))

    # Fire an order → should go through fake_client_for_user with the new creds
    out = exchange.new_order(
        user_id="u-userX",
        symbol="ETHUSDT",
        side="BUY",
        order_type="MARKET",
        client_order_id="coid-xyz"
    )
    assert out["ok"] is True
    assert recorded, "expected _client_for_user to be called"
    user_id, k, s, b = recorded[-1]
    assert user_id == "u-userX"
    assert k == "k-NEW"
    assert s == "s-NEW"
    assert b == "https://custom-base"
