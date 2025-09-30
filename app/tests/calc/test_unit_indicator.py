# tests/calc/test_unit_indicator.py
import pytest
from app.services.calc.strategy import choose_regime, Candle, IndicatorState
from app.services.calc.indicators import SMA

def test_sma_warmup_and_values():
    """Test SMA warmup period and sliding window calculation"""
    sma = SMA(3)
    assert sma.count == 0
    assert sma.update(1.0) is None
    assert sma.count == 1
    assert sma.update(2.0) is None
    assert sma.count == 2
    v = sma.update(4.0)
    assert v == pytest.approx((1+2+4)/3)
    assert sma.count == 3
    # Now sliding window (drop 1.0, add 7.0)
    v2 = sma.update(7.0)
    assert v2 == pytest.approx((2+4+7)/3)

@pytest.mark.parametrize(
    "close,ma20,ma200,expected",
    [
        (100, None, 90, "neutral"),
        (100, 95, None, "neutral"),
        (100, 90, 80, "long"),      # close > ma20 > ma200
        (70, 75, 90, "short"),      # close < ma20 < ma200
        (100, 100, 90, "neutral"),  # not strictly >
        (90, 90, 95, "neutral"),    # not strictly <
        (95, 100, 90, "neutral"),   # price not > ma20
        (80, 75, 80, "neutral"),    # ma20 not < ma200
    ]
)
def test_choose_regime_truth_table(close, ma20, ma200, expected):
    """Test regime selection logic with various price/MA configurations"""
    assert choose_regime(close, ma20, ma200) == expected

def test_indicatorstate_long_uses_last_red_and_levels():
    """Test that LONG regime uses most recent RED candle as indicator"""
    st = IndicatorState(price_tick=0.01)
    
    # neutral bars do not persist indicator
    st.update(Candle(1, 10, 11, 9, 10.5, "green"), "neutral")
    assert st.current is None

    # Enter long with a red candle -> becomes indicator
    res = st.update(Candle(2, 10.5, 10.8, 10.0, 10.1, "red"), "long")
    assert st.current is not None and st.current.color == "red"
    assert res.trigger_long == pytest.approx(10.8 + 0.01)
    assert res.stop_long == pytest.approx(10.0 - 0.01)
    assert res.trigger_short is None and res.stop_short is None

    # Next green bar in long should keep same indicator
    res2 = st.update(Candle(3, 10.1, 11.0, 10.2, 10.9, "green"), "long")
    assert st.current.ts == 2  # unchanged
    assert res2.trigger_long == pytest.approx(10.8 + 0.01)
    assert res2.stop_long == pytest.approx(10.0 - 0.01)

def test_indicatorstate_short_uses_last_green_and_levels():
    """Test that SHORT regime uses most recent GREEN candle as indicator"""
    st = IndicatorState(price_tick=0.01)

    # Enter short with a green candle -> becomes indicator
    res = st.update(Candle(10, 9.5, 9.8, 9.2, 9.6, "green"), "short")
    assert st.current is not None and st.current.color == "green"
    assert res.trigger_short == pytest.approx(9.2 - 0.01)
    assert res.stop_short == pytest.approx(9.8 + 0.01)

    # Next red bar in short keeps the same indicator
    res2 = st.update(Candle(11, 9.6, 9.7, 9.1, 9.2, "red"), "short")
    assert st.current.ts == 10
    assert res2.trigger_short == pytest.approx(9.2 - 0.01)
    assert res2.stop_short == pytest.approx(9.8 + 0.01)

def test_indicatorstate_neutral_clears():
    """Test that neutral regime clears the indicator"""
    st = IndicatorState(price_tick=0.01)
    st.update(Candle(2, 10.5, 10.8, 10.0, 10.1, "red"), "long")
    assert st.current is not None
    st.update(Candle(3, 10.1, 11.0, 10.2, 10.9, "green"), "neutral")
    assert st.current is None

def test_indicatorstate_long_updates_to_newer_red():
    """Test that subsequent red candles update the indicator in long regime"""
    st = IndicatorState(price_tick=0.01)
    
    # First red candle
    st.update(Candle(1, 10.5, 10.8, 10.0, 10.1, "red"), "long")
    assert st.current.ts == 1
    
    # Green candle keeps previous
    st.update(Candle(2, 10.1, 10.5, 10.0, 10.3, "green"), "long")
    assert st.current.ts == 1
    
    # New red candle should update the indicator
    st.update(Candle(3, 10.3, 10.9, 10.2, 10.4, "red"), "long")
    assert st.current.ts == 3
    assert st.current.high == 10.9
    assert st.current.low == 10.2

def test_indicatorstate_short_updates_to_newer_green():
    """Test that subsequent green candles update the indicator in short regime"""
    st = IndicatorState(price_tick=0.01)
    
    # First green candle
    st.update(Candle(1, 9.5, 9.8, 9.2, 9.6, "green"), "short")
    assert st.current.ts == 1
    
    # Red candle keeps previous
    st.update(Candle(2, 9.6, 9.7, 9.1, 9.2, "red"), "short")
    assert st.current.ts == 1
    
    # New green candle should update the indicator
    st.update(Candle(3, 9.2, 9.5, 9.0, 9.4, "green"), "short")
    assert st.current.ts == 3
    assert st.current.high == 9.5
    assert st.current.low == 9.0