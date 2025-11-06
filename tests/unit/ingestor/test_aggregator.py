import pytest

from app.services.ingestor.aggregator import OneMinute, TwoMinuteAggregator


@pytest.fixture
def aggregator():
    return TwoMinuteAggregator("BTCUSDT")


def test_ingest_requires_even_start(aggregator):
    odd_candle = OneMinute(
        ts=60_000,
        open=100.0,
        high=100.0,
        low=100.0,
        close=100.0,
        volume=1.0,
        trades=1,
    )
    even_candle = OneMinute(
        ts=120_000,
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=5.0,
        trades=3,
    )

    assert aggregator.ingest(odd_candle) is None
    assert aggregator.pending_even is None

    assert aggregator.ingest(even_candle) is None
    assert aggregator.pending_even == even_candle


def test_two_minute_aggregation_produces_expected_values(aggregator):
    even_candle = OneMinute(
        ts=120_000,
        open=100.0,
        high=105.0,
        low=99.0,
        close=104.0,
        volume=20.0,
        trades=10,
    )
    odd_candle = OneMinute(
        ts=180_000,
        open=104.0,
        high=108.0,
        low=103.0,
        close=107.0,
        volume=25.0,
        trades=12,
    )

    aggregator.ingest(even_candle)
    result = aggregator.ingest(odd_candle)

    assert result is not None
    assert result["ts"] == str(odd_candle.ts)
    assert result["sym"] == "BTCUSDT"
    assert result["tf"] == "2m"
    assert result["open"] == f"{even_candle.open}"
    assert result["close"] == f"{odd_candle.close}"
    assert result["high"] == f"{max(even_candle.high, odd_candle.high)}"
    assert result["low"] == f"{min(even_candle.low, odd_candle.low)}"
    assert result["volume"] == f"{even_candle.volume + odd_candle.volume}"
    assert result["trades"] == f"{even_candle.trades + odd_candle.trades}"
    assert result["color"] == "green"
    assert aggregator.pending_even is None


def test_two_minute_aggregation_handles_flat_and_large_volume(aggregator):
    even_candle = OneMinute(
        ts=240_000,
        open=200.0,
        high=200.0,
        low=200.0,
        close=200.0,
        volume=0.0,
        trades=0,
    )
    odd_candle = OneMinute(
        ts=300_000,
        open=200.0,
        high=200.0,
        low=198.0,
        close=195.0,
        volume=10_000.0,
        trades=5_000,
    )

    aggregator.ingest(even_candle)
    result = aggregator.ingest(odd_candle)

    assert result is not None
    assert result["open"] == f"{even_candle.open}"
    assert result["close"] == f"{odd_candle.close}"
    assert result["high"] == f"{max(even_candle.high, odd_candle.high)}"
    assert result["low"] == f"{min(even_candle.low, odd_candle.low)}"
    assert result["volume"] == f"{even_candle.volume + odd_candle.volume}"
    assert result["trades"] == f"{even_candle.trades + odd_candle.trades}"
    assert result["color"] == "red"
