from decimal import Decimal

import pytest

from app.services.calc2.regime.detector import RegimeDetector


def test_regime_detector_long_when_ma20_above_ma200() -> None:
    detector = RegimeDetector()

    regime = detector.decide(
        ma20=Decimal("105"),
        ma200=Decimal("100"),
        close_for_long=Decimal("106"),
        close_for_short=Decimal("95"),
    )

    assert regime == "long"


def test_regime_detector_short_when_ma20_below_ma200() -> None:
    detector = RegimeDetector()

    regime = detector.decide(
        ma20=Decimal("95"),
        ma200=Decimal("100"),
        close_for_long=Decimal("101"),
        close_for_short=Decimal("94"),
    )

    assert regime == "short"


def test_regime_detector_neutral_otherwise() -> None:
    detector = RegimeDetector()

    scenarios = [
        {
            "ma20": None,
            "ma200": Decimal("100"),
            "close_for_long": Decimal("101"),
            "close_for_short": Decimal("99"),
        },
        {
            "ma20": Decimal("100"),
            "ma200": Decimal("100"),
            "close_for_long": Decimal("99"),
            "close_for_short": Decimal("101"),
        },
    ]

    for kwargs in scenarios:
        regime = detector.decide(**kwargs)
        assert regime == "neutral"
