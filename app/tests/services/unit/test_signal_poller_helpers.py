import pytest
import importlib
import sys

def _load_poller():
    sys.modules.pop("app.services.tasks.signal_poller", None)
    return importlib.import_module("app.services.tasks.signal_poller")

@pytest.mark.unit
def test__eligible_rules():
    p = _load_poller()
    # inactive
    assert p._eligible({"status": "paused"}, "long") is False
    # long-only mismatch
    assert p._eligible({"status": "active", "side_mode": "long_only"}, "short") is False
    # short-only mismatch
    assert p._eligible({"status": "active", "side_mode": "short_only"}, "long") is False
    # both, active, ok
    assert p._eligible({"status": "active", "side_mode": "both"}, "long") is True

@pytest.mark.unit
def test_ensure_group_idempotent(fake_redis):
    p = _load_poller()
    stream = "stream:signal|{BTCUSDT|2m}"
    # first create should succeed (mkstream)
    p.ensure_group(stream)
    # second create should not raise (BUSYGROUP etc.)
    p.ensure_group(stream)
