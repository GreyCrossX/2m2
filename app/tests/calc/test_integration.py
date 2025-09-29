# tests/test_integration_calc_service.py
import asyncio
import time
import pytest

PRICE_TICK = 0.01

def _market_stream(main, sym):
    return main.st_market(sym, "2m")  # from keys stub in conftest

def _indicator_stream(main, sym):
    return f"stream:ind|{{{main.tag_tf(sym, '2m')}}}"

def _signal_stream(main, sym):
    return f"stream:signal|{{{main.tag_tf(sym, '2m')}}}"

@pytest.mark.asyncio
async def test_neutral_to_long_then_neutral_emits_arm_then_disarm(import_main, fake_redis, bar, now_ts):
    main = import_main
    sym = "BTCUSDT"

    # Shorten logging noise if you want:
    main.logging.getLogger("calc").setLevel("WARNING")

    # Monkeypatch choose_regime to avoid waiting 200 bars.
    # Sequence: neutral -> long -> long -> neutral
    regimes = ["neutral", "long", "long", "neutral"]
    idx = {"i": 0}
    def fake_choose_regime(_close, _ma20, _ma200):
        i = min(idx["i"], len(regimes)-1)
        return regimes[i]
    main.choose_regime = fake_choose_regime

    # Start consumer
    task = asyncio.create_task(main.consume_symbol(sym))
    await asyncio.sleep(0.05)

    # Feed bars. For ARM on long, we need an indicator candle: last RED in long.
    # Bar 1 (neutral): anything, indicator cleared
    fake_redis.xadd(_market_stream(main, sym), bar(now_ts+1, 10, 10.5, 9.9, 10.1, "green"))
    idx["i"] = 1  # long
    # Bar 2 (long & RED) -> sets indicator and should ARM
    fake_redis.xadd(_market_stream(main, sym), bar(now_ts+2, 10.1, 10.3, 9.8, 9.9, "red"))
    # Bar 3 (still long, green) -> no new ARM
    fake_redis.xadd(_market_stream(main, sym), bar(now_ts+3, 9.9, 10.2, 9.7, 10.0, "green"))
    idx["i"] = 3  # neutral
    # Bar 4 (neutral) -> should DISARM
    fake_redis.xadd(_market_stream(main, sym), bar(now_ts+4, 10.0, 10.1, 9.8, 9.9, "green"))

    # Let the loop process
    await asyncio.sleep(0.2)

    # Read signals
    sigs = fake_redis.xread({_signal_stream(main, sym): "0-0"}, count=10, block=10)
    assert sigs, "no signal stream found"
    _, entries = sigs[0]
    types = [fields[b"type"].decode() for _, fields in entries]
    assert types == ["arm", "disarm"]

    # Validate ARM payload math
    arm_fields = entries[0][1]
    assert arm_fields[b"side"].decode() == "long"
    ind_high = float(arm_fields[b"ind_high"])
    ind_low  = float(arm_fields[b"ind_low"])
    trigger  = float(arm_fields[b"trigger"])
    stop     = float(arm_fields[b"stop"])
    assert trigger == pytest.approx(ind_high + PRICE_TICK)
    assert stop == pytest.approx(ind_low - PRICE_TICK)

    # Cleanup
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

@pytest.mark.asyncio
async def test_direct_flip_long_to_short_emits_disarm_then_arm(import_main, fake_redis, bar, now_ts):
    main = import_main
    sym = "BTCUSDT"
    main.logging.getLogger("calc").setLevel("WARNING")

    # Regimes: neutral -> long -> short (direct flip)
    regimes = ["neutral", "long", "short"]
    idx = {"i": 0}
    def fake_choose_regime(_close, _ma20, _ma200):
        i = min(idx["i"], len(regimes)-1)
        return regimes[i]
    main.choose_regime = fake_choose_regime

    task = asyncio.create_task(main.consume_symbol(sym))
    await asyncio.sleep(0.05)

    # Bar 1 neutral
    fake_redis.xadd(_market_stream(main, sym), bar(now_ts+1, 10, 10.5, 9.9, 10.2, "green"))
    idx["i"] = 1  # long
    # Bar 2 long with RED -> sets indicator for long and ARMs
    fake_redis.xadd(_market_stream(main, sym), bar(now_ts+2, 10.2, 10.4, 9.8, 9.95, "red"))
    idx["i"] = 2  # short (direct flip)
    # Bar 3 short with GREEN -> flip should DISARM then ARM(short)
    fake_redis.xadd(_market_stream(main, sym), bar(now_ts+3, 9.95, 10.0, 9.6, 9.8, "green"))

    await asyncio.sleep(0.25)

    # Read signals
    sigs = fake_redis.xread({_signal_stream(main, sym): "0-0"}, count=10, block=10)
    assert sigs, "no signal stream found"
    _, entries = sigs[0]
    types = [fields[b"type"].decode() for _, fields in entries]
    # Expect: ARM(long) from bar 2, then DISARM (flip), then ARM(short)
    assert types == ["arm", "disarm", "arm"]

    disarm_fields = entries[1][1]
    assert disarm_fields[b"prev_side"].decode() == "long"
    assert b"(direct-flip)" in disarm_fields[b"reason"]

    arm_short_fields = entries[2][1]
    assert arm_short_fields[b"side"].decode() == "short"
    ind_high = float(arm_short_fields[b"ind_high"])
    ind_low  = float(arm_short_fields[b"ind_low"])
    trigger  = float(arm_short_fields[b"trigger"])
    stop     = float(arm_short_fields[b"stop"])
    # Short math: trigger = ind_low - tick; stop = ind_high + tick
    assert trigger == pytest.approx(ind_low - PRICE_TICK)
    assert stop == pytest.approx(ind_high + PRICE_TICK)

    # Cleanup
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

@pytest.mark.asyncio
async def test_indicator_stream_snapshots_written(import_main, fake_redis, bar, now_ts):
    main = import_main
    sym = "BTCUSDT"
    main.logging.getLogger("calc").setLevel("WARNING")

    regimes = ["neutral", "long"]
    idx = {"i": 0}
    def fake_choose_regime(_close, _ma20, _ma200):
        return regimes[min(idx["i"], len(regimes)-1)]
    main.choose_regime = fake_choose_regime

    task = asyncio.create_task(main.consume_symbol(sym))
    await asyncio.sleep(0.05)

    # Bar 1 neutral
    fake_redis.xadd(_market_stream(main, sym), bar(now_ts+1, 10, 10.4, 9.9, 10.1, "green"))
    idx["i"] = 1
    # Bar 2 long red (indicator present) -> also writes calc snapshot
    fake_redis.xadd(_market_stream(main, sym), bar(now_ts+2, 10.1, 10.3, 9.8, 9.9, "red"))

    await asyncio.sleep(0.2)

    # Indicator/calculation stream should have entries
    ind = fake_redis.xread({_indicator_stream(main, sym): "0-0"}, count=10, block=10)
    assert ind, "no indicator stream found"
    _, entries = ind[0]
    assert len(entries) >= 2
    # Snapshot hash should also be present
    snap = fake_redis.hgetall(main.snapshot_hash(sym))
    assert b"ts" in snap and b"regime" in snap

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
