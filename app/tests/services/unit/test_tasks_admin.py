from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

from app.services.tasks.admin import available_symbols, list_signal_stream_pairs, sync_bot_runtime
from app.services.tasks.state import read_bot_config, bots_for_symbol


def test_list_signal_stream_pairs_reads_redis(fake_redis):
    fake_redis.xadd("stream:signal|{ETHUSDT:2m}", {"type": "arm", "sym": "ETHUSDT"})
    pairs = list_signal_stream_pairs()
    assert ("ETHUSDT", "2m") in pairs


def test_available_symbols_falls_back_to_settings(fake_redis):
    # With no streams seeded, fallback returns configured pairs (from conftest stub)
    symbols = available_symbols()
    assert "BTCUSDT" in symbols


def test_sync_bot_runtime_persists_config_and_index(fake_redis):
    bot_id = uuid4()
    user_id = uuid4()
    bot = SimpleNamespace(
        id=bot_id,
        user_id=user_id,
        symbol="btcusdt",
        side_mode="both",
        status="active",
        risk_per_trade=Decimal("0.010"),
        leverage=5,
        tp_ratio=Decimal("1.8"),
        max_qty=None,
    )

    sync_bot_runtime(bot)

    cfg = read_bot_config(str(bot_id))
    assert cfg["sym"] == "BTCUSDT"
    assert cfg["risk_per_trade"] == Decimal("0.010")
    assert str(bot_id) in bots_for_symbol("BTCUSDT")


def test_sync_bot_runtime_deindexes_ended_bot(fake_redis):
    bot_id = uuid4()
    user_id = uuid4()
    bot = SimpleNamespace(
        id=bot_id,
        user_id=user_id,
        symbol="btcusdt",
        side_mode="both",
        status="ended",
        risk_per_trade=Decimal("0.010"),
        leverage=5,
        tp_ratio=Decimal("1.8"),
        max_qty=None,
    )

    sync_bot_runtime(bot)

    assert str(bot_id) not in bots_for_symbol("BTCUSDT")
