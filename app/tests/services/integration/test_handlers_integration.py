import pytest
from typing import Any, Dict
import importlib

pytestmark = [pytest.mark.integration]


def _load_handlers():
    # always import fresh so our monkeypatches bind to the module-level symbols
    if "app.services.tasks.handlers" in globals():
        importlib.reload(globals()["app.services.tasks.handlers"])  # type: ignore
    return importlib.import_module("app.services.tasks.handlers")


@pytest.mark.integration
def test_arm_happy_path_writes_state_and_ids(fake_redis, monkeypatch):
    h = _load_handlers()

    # Use real Redis-backed state helpers
    from app.services.tasks import state as st

    # --- fakes wired into handlers namespace ---
    monkeypatch.setattr(h, "already_processed", lambda b, s: False)
    monkeypatch.setattr(
        h,
        "read_bot_config",
        lambda bot_id: {"user_id": "u1", "bot_id": bot_id, "sym": "BTCUSDT", "status": "active"},
    )
    # keep plan minimal; handlers only checks ok + preplace flag
    monkeypatch.setattr(
        h,
        "build_plan",
        lambda **kw: {"ok": True, "entry": {"symbol": "BTCUSDT"}, "preplace_brackets": True},
    )

    def _place_entry_and_track(bot_id: str, plan: Dict[str, Any]):
        st.track_open_order(bot_id, "E1")
        # record armed entry in state
        cur = st.read_bot_state(bot_id)
        cur["armed_entry_order_id"] = "E1"
        st.write_bot_state(bot_id, cur)
        return {"ok": True, "entry_id": "E1"}

    def _place_brackets_and_track(bot_id: str, plan: Dict[str, Any]):
        st.track_open_order(bot_id, "S1")
        st.track_open_order(bot_id, "T1")
        cur = st.read_bot_state(bot_id)
        cur["bracket_ids"] = "S1,T1"
        st.write_bot_state(bot_id, cur)
        return {"ok": True, "sl_tp_ids": ["S1", "T1"]}

    monkeypatch.setattr(h, "place_entry_and_track", _place_entry_and_track)
    monkeypatch.setattr(h, "place_brackets_and_track", _place_brackets_and_track)

    payload = {
        "bot_id": "b1",
        "signal_id": "BTCUSDT:169...:long",
        "sym": "BTCUSDT",
        "side": "long",
        "trigger": "100",
        "stop": "95",
    }
    out = h.on_arm_signal.run(payload)  # shared_task, use .run()

    assert out["ok"] is True
    assert out["entry_id"] == "E1"
    assert out["sl_tp_ids"] == ["S1", "T1"]

    # Redis state assertions
    s = st.read_bot_state("b1")
    assert s.get("armed_entry_order_id") == "E1"
    assert (s.get("bracket_ids") or "") == "S1,T1"
    tracked = set(st.list_tracked_orders("b1"))
    assert tracked == {"E1", "S1", "T1"}

    # idempotency marked
    processed = st.list_processed_signals("b1")
    assert payload["signal_id"] in processed


@pytest.mark.integration
def test_arm_partial_failure_returns_entry_and_tracks_entry_only(fake_redis, monkeypatch):
    h = _load_handlers()
    from app.services.tasks import state as st

    monkeypatch.setattr(h, "already_processed", lambda b, s: False)
    monkeypatch.setattr(
        h,
        "read_bot_config",
        lambda bot_id: {"user_id": "u1", "bot_id": bot_id, "sym": "BTCUSDT", "status": "active"},
    )
    monkeypatch.setattr(
        h,
        "build_plan",
        lambda **kw: {"ok": True, "entry": {"symbol": "BTCUSDT"}, "preplace_brackets": True},
    )

    def _place_entry_and_track(bot_id: str, plan: Dict[str, Any]):
        st.track_open_order(bot_id, "E1")
        cur = st.read_bot_state(bot_id)
        cur["armed_entry_order_id"] = "E1"
        st.write_bot_state(bot_id, cur)
        return {"ok": True, "entry_id": "E1"}

    # brackets fail after placing SL only (S1), TP fails
    def _place_brackets_and_track(bot_id: str, plan: Dict[str, Any]):
        st.track_open_order(bot_id, "S1")
        # do NOT set bracket_ids in state to emulate partial failure
        return {"ok": False, "error": "tp failed", "placed": ["S1"]}

    monkeypatch.setattr(h, "place_entry_and_track", _place_entry_and_track)
    monkeypatch.setattr(h, "place_brackets_and_track", _place_brackets_and_track)

    payload = {
        "bot_id": "b1",
        "signal_id": "BTCUSDT:169...:long",
        "sym": "BTCUSDT",
        "side": "long",
        "trigger": "100",
        "stop": "95",
    }
    out = h.on_arm_signal.run(payload)

    assert out["ok"] is False
    assert out["error"] == "brackets_failed"
    assert out["entry_id"] == "E1"
    assert out.get("placed") == ["S1"]

    tracked = set(st.list_tracked_orders("b1"))
    # At minimum we expect the entry and the placed SL to be tracked.
    # Some implementations might also briefly track TP before failing.
    assert {"E1", "S1"}.issubset(tracked)
    assert len(tracked) in (2, 3)

    s = st.read_bot_state("b1")
    # On partial failure the handler may either clear bracket_ids or keep the planned ids.
    # In either case it must at least include the actually placed SL ("S1") if set.
    br_ids = s.get("bracket_ids")
    assert (br_ids is None) or (br_ids == "") or ("S1" in br_ids)

    # Marked processed even on partial failure to prevent duplicate re-entry;
    # reconciliation will handle any missing brackets later.
    assert payload["signal_id"] in st.list_processed_signals("b1")




@pytest.mark.integration
def test_disarm_cancels_open_and_clears_state(fake_redis, monkeypatch):
    h = _load_handlers()
    from app.services.tasks import state as st

    # Seed open orders + state
    st.track_open_order("b1", "E1")
    st.track_open_order("b1", "S1")
    st.track_open_order("b1", "T1")
    st.write_bot_state("b1", {"armed_entry_order_id": "E1", "bracket_ids": "S1,T1"})

    def _fake_disarm(bot_id: str):
        # emulate cancellation by clearing tracking + state
        for oid in list(st.list_tracked_orders(bot_id)):
            st.untrack_open_order(bot_id, oid)
        st.write_bot_state(bot_id, {"armed_entry_order_id": "", "bracket_ids": ""})
        return {"ok": True, "cancelled": {"entry": True, "brackets": 2}}

    monkeypatch.setattr(h, "disarm", _fake_disarm)

    out = h.on_disarm_signal.run(
        {"bot_id": "b1", "signal_id": "sid-x", "sym": "BTCUSDT", "side": "long"}
    )
    assert out["ok"] is True
    assert out["result"]["ok"] is True

    assert st.list_tracked_orders("b1") == []
    s = st.read_bot_state("b1")
    assert s.get("armed_entry_order_id") is None
    assert (s.get("bracket_ids") or None) is None


@pytest.mark.integration
def test_arm_idempotent_skip_does_not_double_place(fake_redis, monkeypatch):
    h = _load_handlers()
    from app.services.tasks import state as st

    # Seed idempotency -> handler should short-circuit
    monkeypatch.setattr(h, "already_processed", lambda b, s: True)

    # If called, entry placement should explode (to ensure no call happens)
    def _boom(*a, **k):
        raise AssertionError("place_entry_and_track must not be called")

    monkeypatch.setattr(h, "place_entry_and_track", _boom)

    out = h.on_arm_signal.run(
        {
            "bot_id": "b1",
            "signal_id": "sid-dup",
            "sym": "BTCUSDT",
            "side": "long",
            "trigger": "100",
            "stop": "95",
        }
    )
    assert out["ok"] is True
    assert out.get("skipped") == "duplicate"
    # no state changes
    assert st.list_tracked_orders("b1") == []
