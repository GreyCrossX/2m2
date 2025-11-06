from decimal import Decimal

import pytest

from app.services.calc2.indicators.sma import SMA


def test_sma_returns_none_until_window_full() -> None:
    sma = SMA(window=3)

    assert sma.update(Decimal("1")) is None
    assert sma.update(Decimal("2")) is None

    result = sma.update(Decimal("3"))
    assert result == Decimal("2")


def test_sma_maintains_rolling_window() -> None:
    sma = SMA(window=3)
    for value in ("1", "2", "3"):
        sma.update(Decimal(value))

    result = sma.update(Decimal("4"))
    assert result == Decimal("3")

    result = sma.update(Decimal("5"))
    assert result == Decimal("4")


def test_sma_rejects_invalid_window() -> None:
    with pytest.raises(ValueError):
        SMA(window=0)
