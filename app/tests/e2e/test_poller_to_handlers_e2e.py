import sys
import types
import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.integration]

def _modpath(name: str):
    if name in sys.modules:
        del sys.modules[name]
    return __import__(name, fromlist=["*"])

def _stream_for(sym: str, tf: str) -> str:
    # match app.services.tasks.keys.stream_signal_by_tag(tag_tf(sym, tf))
    tag = f"{sym}|{tf}"
    return f"stream:signal|{{{tag}}}"

def _seed_signal(r, stream: str, *, typ: str, sym: str, tf: str, side: str, ind_ts: str, trigger: str, stop: str):
    r.xadd(stream, {
        "type": typ,
        "sym": sym,
        "tf": tf,
        "ind_ts": ind_ts,
        "side": side,
        "trigger": trigger,
        "stop": stop,
    })

@pytest.fixture
def _load_all(fake_redis):
    """
    Import the poller + handlers fresh so they bind to the fake redis,
    and return handy refs.
    """
    poller = _modpath("app.services.tasks.signal_poller")
    handlers = _modpath("app.services.tasks.handlers")
    state = _modpath("app.services.tasks.state")
    return poller, handlers, state

def test_e2e_arm_then_disarm_flow(fake_redis, monkeypatch, _load_all, caplog):
    poller, handlers, state = _load_all

    # Index one active bot for BTCUSDT
    sym, tf = "BTCUSDT", "2m"
    bot_id = "b1"
    state.index_bot(sym, bot_id)
    state.write_bot_config(bot_id, {
        "bot_id": bot_id, "user_id": "u1", "sym": sym,
        "status": "active", "side_mode": "both"
    })

    # Ensure idempotency starts empty
    monkeypatch.setattr(handlers, "already_processed", lambda b, s: False)

    # Make the handler side-effects deterministic but still go through handler code paths.
    # We stub only the low-level placement helpers.
    placed = {"entry": None, "brackets": []}

    def fake_build_plan(**kw):
        # minimal viable plan structure handler expects
        return {
            "ok": True,
            "preplace_brackets": True,
            "entry": {
                "symbol": sym, "side": "BUY", "order_type": "STOP_MARKET",
                "quantity": "0.01", "stop_price": "100.0", "working_type": "MARK_PRICE",
                "client_order_id": "coid-entry",
            },
            "sl": {"symbol": sym, "side": "SELL", "order_type": "STOP_MARKET", "stop_price": "95.0",
                   "client_order_id": "coid-sl"},
            "tp": {"symbol": sym, "side": "SELL", "order_type": "TAKE_PROFIT_MARKET", "stop_price": "105.0",
                   "client_order_id": "coid-tp"},
        }

    def fake_place_entry_and_track(bot_id_arg, plan):
        placed["entry"] = "E-123"
        state.track_open_order(bot_id_arg, "E-123")
        state.write_bot_state(bot_id_arg, {"armed_entry_order_id": "E-123"})
        return {"ok": True, "entry_id": "E-123"}

    def fake_place_brackets_and_track(bot_id_arg, plan):
        state.track_open_order(bot_id_arg, "S-111")
        state.track_open_order(bot_id_arg, "T-222")
        state.write_bot_state(bot_id_arg, {"bracket_ids": "S-111,T-222"})
        placed["brackets"] = ["S-111", "T-222"]
        return {"ok": True, "sl_tp_ids": ["S-111", "T-222"]}

    monkeypatch.setattr(handlers, "build_plan", fake_build_plan)
    monkeypatch.setattr(handlers, "place_entry_and_track", fake_place_entry_and_track)
    monkeypatch.setattr(handlers, "place_brackets_and_track", fake_place_brackets_and_track)

    # Replace the poller's Celery SEND with a shim that immediately calls the task function
    send_calls = []

    def send_task_stub(task_name, args=None, kwargs=None, queue=None):
        payload = args[0] if args else kwargs
        send_calls.append({"task": task_name, "queue": queue, "payload": payload})
        # Call through the Celery task the same way Celery would when running eagerly
        if task_name.endswith("on_arm_signal"):
            return handlers.on_arm_signal.run(payload)
        elif task_name.endswith("on_disarm_signal"):
            return handlers.on_disarm_signal.run(payload)
        else:
            raise AssertionError(f"unexpected task: {task_name}")

    monkeypatch.setattr(poller, "SEND", send_task_stub)

    # Prepare stream + consumer group
    stream = _stream_for(sym, tf)
    poller.ensure_group(stream)

    # 1) ARM signal end-to-end
    _seed_signal(fake_redis, stream, typ="arm", sym=sym, tf=tf, side="long",
                 ind_ts="1700000000001", trigger="100", stop="95")
    data = fake_redis.xreadgroup(poller.GROUP, poller.CONSUMER, {stream: "0-0"}, count=16, block=10)
    assert data, "should have a pending ARM"
    _, entries = data[0]
    handled = poller._process_batch(sym, tf, stream, entries)
    assert handled == 1, "ARM should be ACKed on success"
    assert any(c["task"].endswith("on_arm_signal") for c in send_calls)

    st = state.read_bot_state(bot_id)
    assert st.get("armed_entry_order_id") == "E-123"
    assert (state.list_tracked_orders(bot_id).count("E-123") == 1)

    # 2) DISARM signal end-to-end
    # Make disarm actually clear tracking/state via real handler call, but stub disarm() low-level impl.
    def fake_disarm(bot_id_arg: str):
        for oid in list(state.list_tracked_orders(bot_id_arg)):
            state.untrack_open_order(bot_id_arg, oid)
        state.write_bot_state(bot_id_arg, {"armed_entry_order_id": None, "bracket_ids": None})
        return {"ok": True, "cancelled": {"entry": True, "brackets": 2}}

    monkeypatch.setattr(handlers, "disarm", fake_disarm)

    _seed_signal(fake_redis, stream, typ="disarm", sym=sym, tf=tf, side="long",
                 ind_ts="1700000000002", trigger="0", stop="0")
    data = fake_redis.xreadgroup(poller.GROUP, poller.CONSUMER, {stream: ">"}, count=16, block=10)
    _, entries = data[0]
    handled2 = poller._process_batch(sym, tf, stream, entries)
    assert handled2 == 1, "DISARM should be ACKed on success"

    st2 = state.read_bot_state(bot_id)
    assert st2.get("armed_entry_order_id") is None
    assert st2.get("bracket_ids") is None
    assert state.list_tracked_orders(bot_id) == []
