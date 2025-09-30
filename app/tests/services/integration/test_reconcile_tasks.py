import pytest
from decimal import Decimal
import importlib

pytestmark = [pytest.mark.integration]


def _import_state():
    return importlib.import_module("app.services.tasks.state")


def _import_reconcile():
    return importlib.import_module("app.services.tasks.reconcile")


def _import_periodic():
    return importlib.import_module("app.services.tasks.periodic")


def test_reconcile_bot_happy_updates_position_and_untracks(fake_redis, monkeypatch):
    st = _import_state()
    rec_mod = _import_reconcile()

    bot_id = "b1"
    sym = "BTCUSDT"

    # Seed config + tracked orders
    st.write_bot_config(
        bot_id,
        {
            "bot_id": bot_id,
            "user_id": "u1",
            "sym": sym,
            "status": "active",
            "side_mode": "both",
        },
    )
    st.track_open_order(bot_id, "E-1")
    st.track_open_order(bot_id, "S-1")
    st.track_open_order(bot_id, "T-1")

    # Exchange stubs: only S-1 remains; position long 0.02 @ 100
    def fake_open_orders(user_id, symbol=None):
        return {"ok": True, "orders": [{"symbol": sym, "orderId": "S-1"}]}

    def fake_positions(user_id, symbol=None):
        return {"ok": True, "positions": [{"symbol": sym, "positionAmt": "0.02", "entryPrice": "100"}]}

    # Patch the names used inside the reconcile module
    monkeypatch.setattr(rec_mod, "get_open_orders", fake_open_orders, raising=True)
    monkeypatch.setattr(rec_mod, "get_positions", fake_positions, raising=True)

    out = rec_mod.reconcile_bot(bot_id)
    assert out["ok"] is True

    # open orders E-1/T-1 should be removed from tracking; S-1 remains
    tracked = set(st.list_tracked_orders(bot_id))
    assert tracked == {"S-1"}

    # state should reflect positions
    st_after = st.read_bot_state(bot_id)
    assert st_after["position_side"] == "long"
    assert st_after["position_qty"] == Decimal("0.02")
    assert st_after["avg_entry_price"] == Decimal("100")


def test_reconcile_bot_inconsistencies_reported(fake_redis, monkeypatch):
    st = _import_state()
    rec_mod = _import_reconcile()

    bot_id = "b2"
    sym = "ETHUSDT"

    st.write_bot_config(
        bot_id,
        {
            "bot_id": bot_id,
            "user_id": "u1",
            "sym": sym,
            "status": "active",
            "side_mode": "both",
        },
    )
    # seed state expecting IDs that WON'T be present on the exchange
    st.write_bot_state(bot_id, {"armed_entry_order_id": "E-X", "bracket_ids": "S-X,T-X"})

    def fake_open_orders(user_id, symbol=None):
        return {"ok": True, "orders": [{"symbol": sym, "orderId": "Q-1"}]}

    def fake_positions(user_id, symbol=None):
        return {"ok": True, "positions": []}

    # Patch the names used inside the reconcile module
    monkeypatch.setattr(rec_mod, "get_open_orders", fake_open_orders, raising=True)
    monkeypatch.setattr(rec_mod, "get_positions", fake_positions, raising=True)

    out = rec_mod.reconcile_bot(bot_id)
    assert out["ok"] is True

    # Should report all three missing
    inc = set(out["inconsistencies"])
    assert "armed_entry_order_id E-X not in open orders" in inc
    assert "bracket S-X not in open orders" in inc
    assert "bracket T-X not in open orders" in inc


def test_periodic_reconcile_tasks_fanout(fake_redis, monkeypatch):
    st = _import_state()
    periodic = _import_periodic()

    # Index two bots on the same symbol
    sym = "BTCUSDT"
    st.index_bot(sym, "b1")
    st.index_bot(sym, "b2")

    # Stub periodic.reconcile_bot (this is what reconcile_symbol_bots calls)
    def fake_rec(bot_id):
        if bot_id == "b1":
            return {"ok": True, "bot": bot_id, "tag": "A"}
        raise RuntimeError("boom")

    monkeypatch.setattr(periodic, "reconcile_bot", fake_rec, raising=True)

    # Directly invoke the Celery task (we call .run since it's a shared_task)
    res = periodic.reconcile_symbol_bots.run(sym)
    assert res["ok"] is True
    assert res["symbol"] == sym
    assert res["count"] == 2

    # results should contain both: one ok, one error
    statuses = {r.get("ok") for r in res["results"]}
    assert statuses == {True, False}
