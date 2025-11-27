from decimal import Decimal


from app.services.calc2.regime.detector import RegimeDetector


def test_regime_detector_long_when_price_below_ma20_and_ma20_below_ma200() -> None:
    detector = RegimeDetector()

    regime = detector.decide(
        ma20=Decimal("95"),
        ma200=Decimal("100"),
        close_for_long=Decimal("94"),
        close_for_short=Decimal("110"),
    )

    assert regime == "long"


def test_regime_detector_short_when_price_above_ma20_and_ma20_above_ma200() -> None:
    detector = RegimeDetector()

    regime = detector.decide(
        ma20=Decimal("105"),
        ma200=Decimal("100"),
        close_for_long=Decimal("104"),
        close_for_short=Decimal("106"),
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
