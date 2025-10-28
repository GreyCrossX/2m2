import pytest
import importlib
import sys

def _load_handlers():
    import importlib
    return importlib.import_module("app.services.tasks.handlers")

def _invoke(task, payload):
    """
    Call a Celery shared_task in a robust, version-agnostic way.
    Always use .run(payload) to avoid bind/__wrapped__ signature surprises.
    """
    return task.run(payload)

def test_on_arm_signal_missing_field(fake_redis):
    h = _load_handlers()
    out = _invoke(h.on_arm_signal, {
        "bot_id": "b1",
        # "signal_id" missing
        "sym": "BTCUSDT",
        "side": "long",
        "trigger": "100",
        "stop": "95",
    })
    assert out["ok"] is False
    assert "missing field" in out["error"]

def test_on_arm_signal_duplicate_skips(fake_redis, monkeypatch):
    h = _load_handlers()
    # ensure already_processed short-circuits before reading cfg
    monkeypatch.setattr(h, "already_processed", lambda b, s: True)
    # also make read_bot_config safe if it ever gets called
    monkeypatch.setattr(h, "read_bot_config", lambda bot_id: {"user_id": "u1", "bot_id": bot_id, "sym": "BTCUSDT"})
    out = _invoke(h.on_arm_signal, {
        "bot_id": "b1",
        "signal_id": "sid-1",
        "sym": "BTCUSDT",
        "side": "long",
        "trigger": "100",
        "stop": "95",
    })
    assert out["ok"] is True
    assert out.get("skipped") == "duplicate"

def test_on_arm_signal_missing_bot_config(fake_redis, monkeypatch):
    h = _load_handlers()
    monkeypatch.setattr(h, "already_processed", lambda b, s: False)
    monkeypatch.setattr(h, "read_bot_config", lambda bot_id: None)
    out = _invoke(h.on_arm_signal, {
        "bot_id": "b1",
        "signal_id": "sid-2",
        "sym": "BTCUSDT",
        "side": "long",
        "trigger": "100",
        "stop": "95",
    })
    assert out["ok"] is False
    assert out["error"] == "bot config not found"

def test_on_arm_signal_plan_not_ok(fake_redis, monkeypatch):
    h = _load_handlers()
    monkeypatch.setattr(h, "already_processed", lambda b, s: False)
    monkeypatch.setattr(h, "read_bot_config", lambda bot_id: {"user_id": "u1", "bot_id": "b1", "sym": "BTCUSDT"})
    monkeypatch.setattr(h, "build_plan", lambda **kwargs: {"ok": False, "diagnostics": {"reason": "no balance"}})
    out = _invoke(h.on_arm_signal, {
        "bot_id": "b1",
        "signal_id": "sid-3",
        "sym": "BTCUSDT",
        "side": "long",
        "trigger": "100",
        "stop": "95",
    })
    assert out["ok"] is False
    assert out["error"] == "plan_not_ok"

def test_on_arm_signal_entry_failure(fake_redis, monkeypatch):
    h = _load_handlers()
    monkeypatch.setattr(h, "already_processed", lambda b, s: False)
    monkeypatch.setattr(h, "read_bot_config", lambda bot_id: {"user_id": "u1", "bot_id": "b1", "sym": "BTCUSDT"})
    monkeypatch.setattr(h, "build_plan", lambda **kwargs: {"ok": True, "entry": {}, "preplace_brackets": True})
    monkeypatch.setattr(h, "place_entry_and_track", lambda bot_id, plan: {"ok": False, "error": "exchange down"})
    out = _invoke(h.on_arm_signal, {
        "bot_id": "b1",
        "signal_id": "sid-4",
        "sym": "BTCUSDT",
        "side": "long",
        "trigger": "100",
        "stop": "95",
    })
    assert out["ok"] is False
    assert out["error"] == "entry_failed"

def test_on_arm_signal_brackets_failure_returns_partial(fake_redis, monkeypatch):
    h = _load_handlers()
    monkeypatch.setattr(h, "already_processed", lambda b, s: False)
    monkeypatch.setattr(h, "read_bot_config", lambda bot_id: {"user_id": "u1", "bot_id": "b1", "sym": "BTCUSDT"})
    monkeypatch.setattr(h, "build_plan", lambda **kwargs: {"ok": True, "entry": {}, "preplace_brackets": True})
    monkeypatch.setattr(h, "place_entry_and_track", lambda bot_id, plan: {"ok": True, "entry_id": "111"})
    monkeypatch.setattr(h, "place_brackets_and_track", lambda bot_id, plan: {"ok": False, "error": "tp failed", "placed": ["222"]})
    out = _invoke(h.on_arm_signal, {
        "bot_id": "b1",
        "signal_id": "sid-5",
        "sym": "BTCUSDT",
        "side": "long",
        "trigger": "100",
        "stop": "95",
    })
    assert out["ok"] is False
    assert out["entry_id"] == "111"
    assert out.get("placed") == ["222"]

def test_on_arm_signal_happy_path_marks_processed(fake_redis, monkeypatch):
    h = _load_handlers()
    marked = {"called": False}
    def _mark(b, s):
        marked["called"] = True
        return 1

    monkeypatch.setattr(h, "already_processed", lambda b, s: False)
    monkeypatch.setattr(h, "read_bot_config", lambda bot_id: {"user_id": "u1", "bot_id": "b1", "sym": "BTCUSDT"})
    monkeypatch.setattr(h, "build_plan", lambda **kwargs: {"ok": True, "entry": {}, "preplace_brackets": True})
    monkeypatch.setattr(h, "place_entry_and_track", lambda bot_id, plan: {"ok": True, "entry_id": "777"})
    monkeypatch.setattr(h, "place_brackets_and_track", lambda bot_id, plan: {"ok": True, "sl_tp_ids": ["888", "999"]})
    monkeypatch.setattr(h, "mark_processed", _mark)

    out = _invoke(h.on_arm_signal, {
        "bot_id": "b1",
        "signal_id": "sid-6",
        "sym": "BTCUSDT",
        "side": "long",
        "trigger": "100",
        "stop": "95",
    })
    assert out["ok"] is True
    assert out["entry_id"] == "777"
    assert out["sl_tp_ids"] == ["888", "999"]
    assert marked["called"] is True

def test_on_disarm_signal_happy_path(fake_redis, monkeypatch):
    h = _load_handlers()
    monkeypatch.setattr(h, "disarm", lambda bot_id: {"ok": True, "cancelled": {"entry": True, "brackets": 2}})
    out = _invoke(h.on_disarm_signal, {
        "bot_id": "b1",
        "signal_id": "sid-x",
        "sym": "BTCUSDT",
        "side": "long",
    })
    assert out["ok"] is True
    assert out["result"]["ok"] is True
