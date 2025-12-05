from decimal import Decimal


from app.services.calc2.regime.detector import RegimeDetector


def test_regime_detector_waits_for_first_crossover() -> None:
    detector = RegimeDetector()

    # First ready values -> seeded, but stay neutral until a flip is observed
    regime1 = detector.decide(
        ma20=Decimal("105"),
        ma200=Decimal("100"),
        close_for_long=Decimal("104"),
        close_for_short=Decimal("110"),
    )
    assert regime1 == "neutral"

    # Same sign -> still neutral
    regime2 = detector.decide(
        ma20=Decimal("106"),
        ma200=Decimal("101"),
        close_for_long=Decimal("105"),
        close_for_short=Decimal("111"),
    )
    assert regime2 == "neutral"


def test_regime_detector_flips_on_bullish_and_bearish_cross() -> None:
    detector = RegimeDetector()

    # Start below -> seed as bearish, still neutral
    assert (
        detector.decide(
            ma20=Decimal("95"),
            ma200=Decimal("100"),
            close_for_long=Decimal("94"),
            close_for_short=Decimal(
                "106"
            ),  # > both -> would allow short once sign flips
        )
        == "neutral"
    )

    # Bullish crossover -> long (price below both MAs)
    assert (
        detector.decide(
            ma20=Decimal("102"),
            ma200=Decimal("99"),
            close_for_long=Decimal("98"),  # below both
            close_for_short=Decimal("103"),
        )
        == "long"
    )

    # Maintain long while above
    assert (
        detector.decide(
            ma20=Decimal("104"),
            ma200=Decimal("100"),
            close_for_long=Decimal("99"),  # still below both
            close_for_short=Decimal("104"),  # irrelevant for long
        )
        == "long"
    )

    # Bearish crossover -> short (price above both MAs)
    assert (
        detector.decide(
            ma20=Decimal("98"),
            ma200=Decimal("101"),
            close_for_long=Decimal("97"),
            close_for_short=Decimal("103"),  # still above both
        )
        == "short"
    )


def test_price_gate_blocks_trend_if_price_on_wrong_side() -> None:
    detector = RegimeDetector()

    # Seed sign positive, but price above MAs -> stay neutral
    detector.decide(
        ma20=Decimal("101"),
        ma200=Decimal("100"),
        close_for_long=Decimal("105"),  # wrong side for long
        close_for_short=Decimal("105"),
    )
    assert (
        detector.decide(
            ma20=Decimal("102"),
            ma200=Decimal("101"),
            close_for_long=Decimal("104"),
            close_for_short=Decimal("104"),
        )
        == "neutral"
    )

    # Flip to short sign and price above both -> short allowed
    assert (
        detector.decide(
            ma20=Decimal("99"),
            ma200=Decimal("101"),
            close_for_long=Decimal("98"),
            close_for_short=Decimal("103"),  # above both
        )
        == "short"
    )
