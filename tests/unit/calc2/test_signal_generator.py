from decimal import Decimal

import pytest

from app.services.calc2.signals.generator import SignalGenerator


@pytest.fixture
def generator() -> SignalGenerator:
    return SignalGenerator(tick_size=Decimal("0.1"))


def _arm_for(
    gen: SignalGenerator,
    *,
    regime: str,
    ind_ts: int,
    ind_high: Decimal,
    ind_low: Decimal,
) -> None:
    sigs = gen.maybe_signals(
        sym="BTCUSDT",
        tf="2m",
        now_ts=ind_ts,
        regime=regime,
        ind_ts=ind_ts,
        ind_high=ind_high,
        ind_low=ind_low,
    )
    assert sigs, "Expected ARM signal to be emitted"
    arm = sigs[-1]
    assert getattr(arm, "type", None) == "arm"


@pytest.mark.parametrize(
    "target_regime,ind_high,ind_low",
    [
        ("long", Decimal("105"), Decimal("100")),
        ("short", Decimal("101"), Decimal("99")),
    ],
)
def test_neutral_to_trending_emits_arm(
    generator: SignalGenerator,
    target_regime: str,
    ind_high: Decimal,
    ind_low: Decimal,
) -> None:
    # Establish neutral baseline so _prev_regime is defined
    assert generator.maybe_signals(
        sym="BTCUSDT",
        tf="2m",
        now_ts=1,
        regime="neutral",
        ind_ts=1,
        ind_high=Decimal("101"),
        ind_low=Decimal("99"),
    ) == []

    sigs = generator.maybe_signals(
        sym="BTCUSDT",
        tf="2m",
        now_ts=2,
        regime=target_regime,
        ind_ts=2,
        ind_high=ind_high,
        ind_low=ind_low,
    )

    assert len(sigs) == 1
    arm = sigs[0]
    assert getattr(arm, "type", None) == "arm"
    assert getattr(arm, "side", None) == target_regime


def test_indicator_update_emits_disarm_then_arm(generator: SignalGenerator) -> None:
    # Prime generator with neutral then short regime (initial ARM)
    assert generator.maybe_signals(
        sym="BTCUSDT",
        tf="2m",
        now_ts=1,
        regime="neutral",
        ind_ts=1,
        ind_high=Decimal("101"),
        ind_low=Decimal("99"),
    ) == []

    sigs = generator.maybe_signals(
        sym="BTCUSDT",
        tf="2m",
        now_ts=2,
        regime="short",
        ind_ts=2,
        ind_high=Decimal("101"),
        ind_low=Decimal("99"),
    )
    assert len(sigs) == 1
    first_arm = sigs[0]
    assert getattr(first_arm, "type", None) == "arm"
    assert getattr(first_arm, "ind_ts", None) == 2

    # Same regime, new indicator candle -> expect DISARM + new ARM
    update_sigs = generator.maybe_signals(
        sym="BTCUSDT",
        tf="2m",
        now_ts=3,
        regime="short",
        ind_ts=3,
        ind_high=Decimal("102"),
        ind_low=Decimal("100"),
    )
    assert len(update_sigs) == 2
    disarm, new_arm = update_sigs
    assert getattr(disarm, "type", None) == "disarm"
    assert getattr(disarm, "reason", None) == "update_pending"
    assert getattr(new_arm, "type", None) == "arm"
    assert getattr(new_arm, "ind_ts", None) == 3


def test_regime_flip_emits_disarm_then_arm(generator: SignalGenerator) -> None:
    # Prime neutral -> long
    assert generator.maybe_signals(
        sym="BTCUSDT",
        tf="2m",
        now_ts=1,
        regime="neutral",
        ind_ts=1,
        ind_high=Decimal("101"),
        ind_low=Decimal("99"),
    ) == []

    long_sigs = generator.maybe_signals(
        sym="BTCUSDT",
        tf="2m",
        now_ts=2,
        regime="long",
        ind_ts=2,
        ind_high=Decimal("105"),
        ind_low=Decimal("100"),
    )
    assert len(long_sigs) == 1

    flip_sigs = generator.maybe_signals(
        sym="BTCUSDT",
        tf="2m",
        now_ts=3,
        regime="short",
        ind_ts=3,
        ind_high=Decimal("102"),
        ind_low=Decimal("98"),
    )

    assert len(flip_sigs) == 2
    disarm, arm = flip_sigs
    assert getattr(disarm, "type", None) == "disarm"
    assert getattr(disarm, "reason", None) == "flip:long->short"
    assert getattr(arm, "type", None) == "arm"
    assert getattr(arm, "side", None) == "short"


def test_same_indicator_no_duplicate_signals(generator: SignalGenerator) -> None:
    # Establish baseline neutral regime so subsequent ARM is emitted on transition
    assert generator.maybe_signals(
        sym="BTCUSDT",
        tf="2m",
        now_ts=9,
        regime="neutral",
        ind_ts=9,
        ind_high=Decimal("104"),
        ind_low=Decimal("99"),
    ) == []

    _arm_for(
        generator,
        regime="long",
        ind_ts=10,
        ind_high=Decimal("105"),
        ind_low=Decimal("100"),
    )

    # Repeat with identical indicator timestamp -> expect no signals
    assert generator.maybe_signals(
        sym="BTCUSDT",
        tf="2m",
        now_ts=11,
        regime="long",
        ind_ts=10,
        ind_high=Decimal("105"),
        ind_low=Decimal("100"),
    ) == []
