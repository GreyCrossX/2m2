import pytest
from decimal import Decimal
import importlib
import sys

BOT_ID = "bot-1"

def _load_state_module():
    sys.modules.pop("app.services.tasks.state", None)
    state_mod = importlib.import_module("app.services.tasks.state")
    return state_mod

@pytest.mark.unit
def test_config_roundtrip(fake_redis):
    state = _load_state_module()
    cfg = {
        "bot_id": BOT_ID,
        "user_id": "user-123",
        "sym": "BTCUSDT",
        "side_mode": "both",
        "status": "active",
        "risk_per_trade": Decimal("0.05"),
        "leverage": Decimal("10"),
        "tp_ratio": Decimal("1.5"),
        "max_qty": Decimal("0.25"),
    }
    state.write_bot_config(BOT_ID, cfg)
    got = state.read_bot_config(BOT_ID)
    assert got is not None
    assert got["bot_id"] == BOT_ID
    assert got["sym"] == "BTCUSDT"
    assert got["risk_per_trade"] == Decimal("0.05")
    assert got["leverage"] == Decimal("10")
    assert got["tp_ratio"] == Decimal("1.5")
    assert got["max_qty"] == Decimal("0.25")

@pytest.mark.unit
def test_state_roundtrip(fake_redis):
    state = _load_state_module()
    st = {
        "last_signal_id": "BTCUSDT:123:long",
        "armed_entry_order_id": "111",
        "bracket_ids": "222,333",
        "position_side": "long",
        "position_qty": Decimal("0.15"),
        "avg_entry_price": Decimal("100.5"),
    }
    state.write_bot_state(BOT_ID, st)
    got = state.read_bot_state(BOT_ID)
    assert got["last_signal_id"] == st["last_signal_id"]
    assert got["armed_entry_order_id"] == st["armed_entry_order_id"]
    assert got["bracket_ids"] == st["bracket_ids"]
    assert got["position_side"] == st["position_side"]
    assert got["position_qty"] == Decimal("0.15")
    assert got["avg_entry_price"] == Decimal("100.5")

@pytest.mark.unit
def test_idempotency_set(fake_redis):
    state = _load_state_module()
    sid = "BTCUSDT:999:long"
    assert not state.is_signal_processed(BOT_ID, sid)
    added = state.mark_signal_processed(BOT_ID, sid)
    assert added == 1
    assert state.is_signal_processed(BOT_ID, sid)
    all_ids = state.list_processed_signals(BOT_ID)
    assert sid in all_ids

@pytest.mark.unit
def test_open_order_tracking(fake_redis):
    state = _load_state_module()
    assert state.track_open_order(BOT_ID, "a1") == 1
    assert state.track_open_order(BOT_ID, "a2") == 1
    ids = set(state.list_tracked_orders(BOT_ID))
    assert ids == {"a1", "a2"}
    assert state.untrack_open_order(BOT_ID, "a1") == 1
    ids2 = set(state.list_tracked_orders(BOT_ID))
    assert ids2 == {"a2"}
