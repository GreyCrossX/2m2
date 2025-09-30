import pytest
from decimal import Decimal
import importlib
import sys

def _load_domain():
    sys.modules.pop("app.services.tasks.domain", None)
    return importlib.import_module("app.services.tasks.domain")

BASE_CFG = {
    "bot_id": "b1",
    "user_id": "u1",
    "sym": "BTCUSDT",
    "side_mode": "both",
    "status": "active",
    "risk_per_trade": Decimal("0.05"),
    "leverage": Decimal("5"),
    "tp_ratio": Decimal("1.5"),
}

@pytest.mark.unit
def test_build_plan_no_balance_returns_not_ok(fake_redis, monkeypatch):
    d = _load_domain()
    # zero balance
    monkeypatch.setattr(d, "get_free_balance", lambda _uid, asset="USDT": Decimal("0"))
    # simple filters
    monkeypatch.setattr(d, "get_symbol_filters", lambda sym: {"step_size": "0.001", "min_qty": "0", "min_notional": "0"})
    arm = {
        "bot_id": "b1",
        "signal_id": "sid-1",
        "sym": "BTCUSDT",
        "side": "long",
        "trigger": Decimal("100"),
        "stop": Decimal("95"),
        "tp_ratio": Decimal("1.5"),
    }
    plan = d.build_plan(arm=arm, bot_cfg=BASE_CFG)
    assert plan["ok"] is False
    assert "sizing" in plan["diagnostics"]

@pytest.mark.unit
def test_build_plan_happy_path(fake_redis, monkeypatch):
    d = _load_domain()
    # stub balance and filters
    monkeypatch.setattr(d, "get_free_balance", lambda _uid, asset="USDT": Decimal("1000"))
    monkeypatch.setattr(d, "get_symbol_filters", lambda sym: {"step_size": "0.001", "min_qty": "0.01", "min_notional": "10", "tick_size": "0.10"})
    # size returns ok
    from decimal import Decimal as D
    monkeypatch.setattr(d, "compute_position_size", lambda **kw: {"ok": True, "qty": D("0.12")})
    arm = {
        "bot_id": "b1",
        "signal_id": "sid-2",
        "sym": "BTCUSDT",
        "side": "long",
        "trigger": Decimal("100"),
        "stop": Decimal("95"),
        "tp_ratio": Decimal("1.6"),
    }
    plan = d.build_plan(arm=arm, bot_cfg=BASE_CFG)
    assert plan["ok"] is True
    assert plan["sym"] == "BTCUSDT"
    assert plan["side"] == "long"
    assert plan["qty"] == Decimal("0.12")
    assert plan["entry"]["order_type"] == "STOP_MARKET"
    assert "brackets" in plan
    assert plan["tp_price"] is not None

@pytest.mark.unit
def test_build_plan_clamps_to_max_qty(fake_redis, monkeypatch):
    d = _load_domain()
    cfg = dict(BASE_CFG, max_qty=Decimal("0.05"))
    monkeypatch.setattr(d, "get_free_balance", lambda _uid, asset="USDT": Decimal("1000"))
    monkeypatch.setattr(d, "get_symbol_filters", lambda sym: {"step_size": "0.001", "min_qty": "0.001", "min_notional": "0"})
    monkeypatch.setattr(d, "compute_position_size", lambda **kw: {"ok": True, "qty": Decimal("0.12")})
    arm = {
        "bot_id": "b1",
        "signal_id": "sid-3",
        "sym": "BTCUSDT",
        "side": "short",
        "trigger": Decimal("200"),
        "stop": Decimal("210"),
        "tp_ratio": Decimal("1.5"),
    }
    plan = d.build_plan(arm=arm, bot_cfg=cfg)
    assert plan["ok"] is False
# Implementation reports lack of funds via diagnostics.notes
    assert any("exceeds max_qty" in n for n in plan["diagnostics"].get("notes", []))
