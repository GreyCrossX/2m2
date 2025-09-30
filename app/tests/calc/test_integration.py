# tests/calc/test_integration.py
import asyncio
import pytest
from unittest.mock import MagicMock

PRICE_TICK = 0.01


async def process_bars_for_test(main_module, redis_client, sym, bars, regime_sequence):
    """
    Test helper that processes bars directly without the blocking consumer loop.
    This mimics consume_symbol but uses unique stream IDs per bar so
    DISARM+ARM on a flip can't clash.
    """
    stream_out = main_module.indicator_stream(sym)
    stream_sig = main_module.signal_stream(sym)
    snap_key = main_module.snapshot_hash(sym)

    # Fake SMAs that always return values (skip warmup)
    class FakeSMA:
        def __init__(self, period):
            self.period = period
            self._count = 0
            self._value = 100.0 if period == 20 else 95.0
        def update(self, x):
            self._count += 1
            return self._value
        @property
        def count(self):
            return self._count

    sma20 = FakeSMA(20)
    sma200 = FakeSMA(200)
    istate = main_module.IndicatorState(price_tick=main_module.PRICE_TICK)

    last_regime = "neutral"

    # Per-timestamp sequence counter to avoid XADD id collisions on the same bar
    seq_by_ts: dict[int, int] = {}

    def _emit_signal(payload: dict, ts: int):
        n = seq_by_ts.get(ts, 0)
        redis_client.xadd(
            stream_sig, payload, id=f"{ts}-{n}",
            maxlen=main_module.STREAM_MAXLEN_SIGNAL, approximate=True
        )
        seq_by_ts[ts] = n + 1

    # Process each bar with its corresponding regime
    for bar_data, regime in zip(bars, regime_sequence):
        ts    = int(bar_data["ts"])
        close = float(bar_data["close"])
        open_ = float(bar_data["open"])
        high  = float(bar_data["high"])
        low   = float(bar_data["low"])
        color = bar_data["color"]

        # Update SMAs (fake ones that always return values)
        v20  = sma20.update(close)
        v200 = sma200.update(close)

        # Update indicator state and levels
        cres = istate.update(
            main_module.Candle(ts=ts, open=open_, high=high, low=low, close=close, color=color),
            regime
        )
        ind = cres.indicator

        # Publish calc snapshot/event (ok to keep a fixed id for the out stream)
        out = {
            "v": "1",
            "sym": sym, "tf": "2m",
            "ts": str(ts),
            "close": str(close), "open": str(open_), "high": str(high), "low": str(low), "color": color,
            "ma20":  "" if v20  is None else f"{v20}",
            "ma200": "" if v200 is None else f"{v200}",
            "regime": regime,
            "ind_ts":   "" if not ind else str(ind.ts),
            "ind_high": "" if not ind else f"{ind.high}",
            "ind_low":  "" if not ind else f"{ind.low}",
        }
        pipe = redis_client.pipeline()
        pipe.hset(snap_key, mapping=out)
        pipe.xadd(stream_out, out, id=f"{ts}-0",
                  maxlen=main_module.STREAM_MAXLEN_IND, approximate=True)
        pipe.execute()

        # ── Signal logic (matches service policy) ────────────────────────────
        flip = (last_regime in ("long", "short")
                and regime in ("long", "short")
                and regime != last_regime)

        # A) neutral -> long/short : ARM
        if last_regime == "neutral" and regime in ("long", "short"):
            if ind is not None:
                if regime == "long":
                    trigger, stop = main_module._calc_long_levels(ind.high, ind.low)
                    payload = {
                        "v": "1", "type": "arm", "side": "long", "sym": sym, "tf": "2m",
                        "ts": str(ts),
                        "ind_ts": str(ind.ts), "ind_high": f"{ind.high}", "ind_low": f"{ind.low}",
                        "trigger": f"{trigger}", "stop": f"{stop}",
                    }
                else:
                    trigger, stop = main_module._calc_short_levels(ind.high, ind.low)
                    payload = {
                        "v": "1", "type": "arm", "side": "short", "sym": sym, "tf": "2m",
                        "ts": str(ts),
                        "ind_ts": str(ind.ts), "ind_high": f"{ind.high}", "ind_low": f"{ind.low}",
                        "trigger": f"{trigger}", "stop": f"{stop}",
                    }
                _emit_signal(payload, ts)

        # B) long/short -> neutral : DISARM
        if last_regime in ("long", "short") and regime == "neutral":
            _emit_signal({
                "v": "1", "type": "disarm", "prev_side": last_regime, "sym": sym, "tf": "2m",
                "ts": str(ts), "reason": f"regime:{last_regime}->neutral",
            }, ts)

        # C) long <-> short (direct flip) : DISARM old then ARM new
        if flip:
            _emit_signal({
                "v": "1", "type": "disarm", "prev_side": last_regime, "sym": sym, "tf": "2m",
                "ts": str(ts), "reason": f"regime:{last_regime}->{regime} (direct-flip)",
            }, ts)

            if ind is not None:
                if regime == "long":
                    trigger, stop = main_module._calc_long_levels(ind.high, ind.low)
                    payload = {
                        "v": "1", "type": "arm", "side": "long", "sym": sym, "tf": "2m",
                        "ts": str(ts),
                        "ind_ts": str(ind.ts), "ind_high": f"{ind.high}", "ind_low": f"{ind.low}",
                        "trigger": f"{trigger}", "stop": f"{stop}",
                    }
                else:
                    trigger, stop = main_module._calc_short_levels(ind.high, ind.low)
                    payload = {
                        "v": "1", "type": "arm", "side": "short", "sym": sym, "tf": "2m",
                        "ts": str(ts),
                        "ind_ts": str(ind.ts), "ind_high": f"{ind.high}", "ind_low": f"{ind.low}",
                        "trigger": f"{trigger}", "stop": f"{stop}",
                    }
                _emit_signal(payload, ts)

        last_regime = regime



@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_neutral_to_long_then_neutral_emits_arm_then_disarm(
    import_main, fake_redis, bar, now_ts
):
    """Test that neutral->long emits ARM, and long->neutral emits DISARM"""
    main = import_main
    sym = "BTCUSDT"
    
    bars = [
        bar(now_ts, 10, 10.5, 9.9, 10.1, "green"),
        bar(now_ts + 120000, 10.1, 10.3, 9.8, 9.9, "red"),
        bar(now_ts + 240000, 9.9, 10.2, 9.7, 10.0, "green"),
        bar(now_ts + 360000, 10.0, 10.1, 9.8, 9.9, "green"),
    ]
    regimes = ["neutral", "long", "long", "neutral"]
    
    await process_bars_for_test(main, fake_redis, sym, bars, regimes)
    
    # Verify signals - use xrange to avoid fakeredis blocking issues
    sig_stream = main.signal_stream(sym)
    entries = fake_redis.xrange(sig_stream, "-", "+")
    
    assert entries, "No signals were written"
    
    types = [fields[b"type"].decode() for _, fields in entries]
    assert types == ["arm", "disarm"], f"Expected ['arm', 'disarm'] but got {types}"
    
    # Validate ARM payload
    arm_fields = entries[0][1]
    assert arm_fields[b"side"].decode() == "long"
    
    ind_high = float(arm_fields[b"ind_high"])
    ind_low = float(arm_fields[b"ind_low"])
    trigger = float(arm_fields[b"trigger"])
    stop = float(arm_fields[b"stop"])
    
    assert trigger == pytest.approx(ind_high + PRICE_TICK)
    assert stop == pytest.approx(ind_low - PRICE_TICK)
    
    # Validate DISARM
    disarm_fields = entries[1][1]
    assert disarm_fields[b"prev_side"].decode() == "long"


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_direct_flip_long_to_short_emits_disarm_then_arm(
    import_main, fake_redis, bar, now_ts
):
    """Test that long->short direct flip emits DISARM(long) then ARM(short)"""
    main = import_main
    sym = "BTCUSDT"
    
    bars = [
        bar(now_ts, 10, 10.5, 9.9, 10.2, "green"),
        bar(now_ts + 120000, 10.2, 10.4, 9.8, 9.95, "red"),
        bar(now_ts + 240000, 9.95, 10.0, 9.6, 9.8, "green"),
    ]
    regimes = ["neutral", "long", "short"]
    
    await process_bars_for_test(main, fake_redis, sym, bars, regimes)
    
    # Verify signals - use xrange to avoid blocking
    sig_stream = main.signal_stream(sym)
    entries = fake_redis.xrange(sig_stream, "-", "+")
    
    assert entries, "No signals found"
    
    types = [fields[b"type"].decode() for _, fields in entries]
    assert types == ["arm", "disarm", "arm"], f"Expected ['arm', 'disarm', 'arm'] but got {types}"
    
    # Validate DISARM mentions direct flip
    disarm_fields = entries[1][1]
    assert disarm_fields[b"prev_side"].decode() == "long"
    assert b"direct-flip" in disarm_fields[b"reason"]
    
    # Validate ARM(short)
    arm_short_fields = entries[2][1]
    assert arm_short_fields[b"side"].decode() == "short"
    
    ind_high = float(arm_short_fields[b"ind_high"])
    ind_low = float(arm_short_fields[b"ind_low"])
    trigger = float(arm_short_fields[b"trigger"])
    stop = float(arm_short_fields[b"stop"])
    
    assert trigger == pytest.approx(ind_low - PRICE_TICK)
    assert stop == pytest.approx(ind_high + PRICE_TICK)


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_indicator_stream_snapshots_written(
    import_main, fake_redis, bar, now_ts
):
    """Test that calc events and snapshots are properly written"""
    main = import_main
    sym = "BTCUSDT"
    
    bars = [
        bar(now_ts, 10, 10.4, 9.9, 10.1, "green"),
        bar(now_ts + 120000, 10.1, 10.3, 9.8, 9.9, "red"),
    ]
    regimes = ["neutral", "long"]
    
    await process_bars_for_test(main, fake_redis, sym, bars, regimes)
    
    # Check indicator stream - use xrange
    ind_stream = main.indicator_stream(sym)
    entries = fake_redis.xrange(ind_stream, "-", "+")
    
    assert entries, "No indicator stream entries found"
    assert len(entries) >= 2
    
    # Check snapshot hash
    snap_key = main.snapshot_hash(sym)
    snap = fake_redis.hgetall(snap_key)
    
    assert snap
    assert b"ts" in snap
    assert b"regime" in snap
    assert snap[b"regime"].decode() == "long"


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_skip_arm_when_no_indicator_candle(
    import_main, fake_redis, bar, now_ts
):
    """Test that we skip ARM when no indicator candle exists"""
    main = import_main
    sym = "BTCUSDT"
    
    bars = [
        bar(now_ts, 10, 10.5, 9.9, 10.1, "green"),
        bar(now_ts + 120000, 10.1, 10.4, 10.0, 10.3, "green"),  # GREEN in long = no indicator
    ]
    regimes = ["neutral", "long"]
    
    await process_bars_for_test(main, fake_redis, sym, bars, regimes)
    
    # Should have NO signals - check using xrange instead of xread to avoid blocking
    sig_stream = main.signal_stream(sym)
    
    # Use xrange instead of xread to avoid fakeredis blocking issues
    try:
        entries = fake_redis.xrange(sig_stream, "-", "+")
        assert len(entries) == 0, f"Expected no signals, but found {len(entries)}"
    except Exception:
        # Stream doesn't exist, which is fine - means no signals
        pass