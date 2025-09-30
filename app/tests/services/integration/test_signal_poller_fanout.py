import sys
import types
import time
import pytest
import uuid 

from decimal import Decimal


pytestmark = pytest.mark.integration

def _unique_sym() -> str:
    return f"S{uuid.uuid4().hex[:10]}USDT"


def _install_fake_celery(send_sink):
    """
    Create a minimal celery_app module with a .celery object exposing send_task.
    - send_sink: a dict with {"calls": list, "raise_for": set of bot_ids to fail}
    """
    modname = "celery_app"
    fake = types.ModuleType(modname)

    class _CeleryApp:
        def __init__(self, sink):
            self._sink = sink

        # Mimic celery signature; poller uses name, args=[{...}], queue="signals"
        def send_task(self, name, args=None, kwargs=None, queue=None):
            payload = args[0] if args else {}
            # Record the call
            self._sink["calls"].append(
                {"name": name, "args": args, "kwargs": kwargs, "queue": queue, "payload": payload}
            )
            # Optional fault injection: raise when targeting a flagged bot_id
            bot_id = payload.get("bot_id")
            if bot_id in self._sink.get("raise_for", set()):
                raise RuntimeError(f"enqueue fail for {bot_id}")
            return {"ok": True}

    fake.celery = _CeleryApp(send_sink)
    sys.modules[modname] = fake
    return fake


def _load_poller(send_sink):
    """
    Ensure our fake celery is present, then import the poller module fresh.
    """
    # Fake celery app first so the poller binds SEND to it at import-time
    _install_fake_celery(send_sink)
    # Clear any cached module so SEND resolves again
    sys.modules.pop("app.services.tasks.signal_poller", None)
    import app.services.tasks.signal_poller as poller
    return poller


def _stream_for(sym: str, tf: str):
    from services.ingestor.keys import tag as tag_tf
    from app.services.tasks.keys import stream_signal_by_tag
    return stream_signal_by_tag(tag_tf(sym, tf))


def _seed_bots_state(sym: str, bots_cfg):
    """
    bots_cfg: list of dicts like
      {"bot_id": "b1", "status": "active", "side_mode": "both", "user_id": "u1", "sym": sym}
    """
    from app.services.tasks import state as st
    for cfg in bots_cfg:
        st.index_bot(sym, cfg["bot_id"])
        st.write_bot_config(cfg["bot_id"], cfg)


def _xread_all(r, group, consumer, stream, start="0-0", count=128, block=1):
    """
    Helper: read a batch from the stream using XREADGROUP and return entries list.
    """
    resp = r.xreadgroup(group, consumer, {stream: start}, count=count, block=block)
    if not resp:
        return []
    _, entries = resp[0]
    return entries


# ───────────────────────────────────────────────────────────────────────────────
# Tests
# ───────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio(scope="function")
async def test_arm_happy_path_fans_out_only_to_eligible(fake_redis):
    """
    ARM message → only eligible bots (active + side_mode) receive a Celery enqueue.
    Message is ACKed when all enqueues succeed.
    """
    send_sink = {"calls": []}
    p = _load_poller(send_sink)

    sym, tf = _unique_sym(), "2m"
    stream = _stream_for(sym, tf)

    # Index: 3 bots, but only b1 eligible for long; b2 paused, b3 short_only
    bots_cfg = [
        {"bot_id": "b1", "user_id": "u1", "sym": sym, "status": "active", "side_mode": "both"},
        {"bot_id": "b2", "user_id": "u1", "sym": sym, "status": "paused", "side_mode": "both"},
        {"bot_id": "b3", "user_id": "u1", "sym": sym, "status": "active", "side_mode": "short_only"},
    ]
    _seed_bots_state(sym, bots_cfg)

    # Create group & message
    p.ensure_group(stream)
    msg_id = fake_redis.xadd(
        stream,
        {
            "type": "arm",
            "sym": sym,
            "tf": tf,
            "ind_ts": "1700000000000",
            "side": "long",
            "trigger": "100",
            "stop": "95",
        },
    )

    # Drain with group start at 0-0 (history)
    entries = _xread_all(fake_redis, p.GROUP, p.CONSUMER, stream, start="0-0")
    assert entries, "expected one entry to be read"
    handled = p._process_batch(sym, tf, stream, entries)
    assert handled == 1

    # Exactly one enqueue (b1)
    assert len(send_sink["calls"]) == 1
    call = send_sink["calls"][0]
    assert call["name"] == "app.services.tasks.handlers.on_arm_signal"
    assert call["queue"] == "signals"
    payload = call["payload"]
    assert payload["bot_id"] == "b1"
    assert payload["sym"] == sym and payload["side"] == "long"
    assert payload["trigger"] == "100" and payload["stop"] == "95"

    # Confirm ACK: no more history at 0-0
    entries2 = _xread_all(fake_redis, p.GROUP, p.CONSUMER, stream, start="0-0")
    assert entries2 == []


@pytest.mark.asyncio(scope="function")
async def test_disarm_message_fans_out_to_active_bots(fake_redis):
    """
    DISARM message → active bots receive the disarm task.
    """
    send_sink = {"calls": []}
    p = _load_poller(send_sink)

    sym, tf = _unique_sym(), "2m"
    stream = _stream_for(sym, tf)

    bots_cfg = [
        {"bot_id": "b1", "user_id": "u1", "sym": sym, "status": "active", "side_mode": "both"},
        {"bot_id": "b2", "user_id": "u1", "sym": sym, "status": "active", "side_mode": "both"},
        {"bot_id": "b3", "user_id": "u1", "sym": sym, "status": "paused", "side_mode": "both"},
    ]
    _seed_bots_state(sym, bots_cfg)

    p.ensure_group(stream)
    fake_redis.xadd(
        stream,
        {
            "type": "disarm",
            "sym": sym,
            "tf": tf,
            "ind_ts": "1700000000001",
            "side": "short",  # side is ignored for disarm eligibility
        },
    )

    entries = _xread_all(fake_redis, p.GROUP, p.CONSUMER, stream, start="0-0")
    handled = p._process_batch(sym, tf, stream, entries)
    assert handled == 1

    # Two active bots should have received disarm
    names = [c["name"] for c in send_sink["calls"]]
    assert names.count("app.services.tasks.handlers.on_disarm_signal") == 2
    bot_ids = {c["payload"]["bot_id"] for c in send_sink["calls"]}
    assert bot_ids == {"b1", "b2"}


@pytest.mark.asyncio(scope="function")
async def test_mismatch_is_acked_no_fanout(fake_redis):
    """
    A message for another timeframe/symbol is ACKed but not fanned out.
    """
    send_sink = {"calls": []}
    p = _load_poller(send_sink)

    sym, tf = _unique_sym(), "2m"
    stream = _stream_for(sym, tf)

    _seed_bots_state(
        sym,
        [{"bot_id": "b1", "user_id": "u1", "sym": sym, "status": "active", "side_mode": "both"}],
    )

    p.ensure_group(stream)
    # Put a mismatched tf
    fake_redis.xadd(
        stream,
        {"type": "arm", "sym": sym, "tf": "1m", "ind_ts": "1700000000002", "side": "long", "trigger": "100", "stop": "95"},
    )

    entries = _xread_all(fake_redis, p.GROUP, p.CONSUMER, stream, start="0-0")
    handled = p._process_batch(sym, tf, stream, entries)
    # mismatch rows are ACKed but not counted as "delivered" by _process_batch
    assert handled == 0
    assert send_sink["calls"] == []

    # Confirm ACK: reading from 0-0 again yields nothing
    entries2 = _xread_all(fake_redis, p.GROUP, p.CONSUMER, stream, start="0-0")
    assert entries2 == []


@pytest.mark.asyncio(scope="function")
async def test_enqueue_failure_leaves_message_unacked_then_reprocessed(fake_redis, monkeypatch):
    """
    If any enqueue fails, the poller leaves the message UNACKed.
    After fixing the issue, reprocessing the same entries should ACK it.
    """
    # First run: make one bot raise on enqueue
    send_sink = {"calls": [], "raise_for": {"b_bad"}}
    p = _load_poller(send_sink)

    sym, tf = _unique_sym(), "2m"
    stream = _stream_for(sym, tf)

    bots_cfg = [
        {"bot_id": "b_good", "user_id": "u1", "sym": sym, "status": "active", "side_mode": "both"},
        {"bot_id": "b_bad", "user_id": "u1", "sym": sym, "status": "active", "side_mode": "both"},
    ]
    _seed_bots_state(sym, bots_cfg)

    p.ensure_group(stream)
    fake_redis.xadd(
        stream,
        {"type": "arm", "sym": sym, "tf": tf, "ind_ts": "1700000000003", "side": "long", "trigger": "100", "stop": "95"},
    )

    # First drain attempt → failure for b_bad → leave unacked
    entries = _xread_all(fake_redis, p.GROUP, p.CONSUMER, stream, start="0-0")
    handled = p._process_batch(sym, tf, stream, entries)
    assert handled == 0, "should not ACK when any enqueue fails"
    # We should have attempted two enqueues (one succeeded, one failed)
    assert len(send_sink["calls"]) == 2

    # Second run: fix enqueue (remove failure)
    send_sink["raise_for"] = set()
    # Reuse the same entries we read earlier (pending to the consumer)
    handled2 = p._process_batch(sym, tf, stream, entries)
    assert handled2 == 1, "now ACKs after successful fan-out"

    # Verify no further history to drain
    entries2 = _xread_all(fake_redis, p.GROUP, p.CONSUMER, stream, start="0-0")
    assert entries2 == []
