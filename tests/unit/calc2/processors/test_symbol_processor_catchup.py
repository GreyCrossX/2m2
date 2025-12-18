from decimal import Decimal
from typing import List


from app.services.calc2.models import ArmSignal
from app.services.calc2.processors.symbol_processor import SymbolProcessor


class DummyPublisher:
    def __init__(self) -> None:
        self.published: List[dict] = []

    async def publish_signal(self, payload: dict) -> str:
        self.published.append(payload)
        return "id-1"


async def test_handle_catchup_publishes_last_signal_only() -> None:
    processor = SymbolProcessor.__new__(SymbolProcessor)
    processor._catchup_mode = True
    processor._candle_count = 5
    processor._signal_count = 0
    processor.sym = "BTCUSDT"
    processor.publisher = DummyPublisher()

    arm = ArmSignal(
        v="1",
        type="arm",
        side="long",
        sym="BTCUSDT",
        tf="2m",
        ts=1000,
        ind_ts=900,
        ind_high=Decimal("105"),
        ind_low=Decimal("100"),
        trigger=Decimal("105.1"),
        stop=Decimal("99.9"),
    )

    processor._last_signal = arm
    processor._is_caught_up = lambda _: True  # type: ignore[assignment]

    await processor._handle_catchup_transition(1000)

    assert processor._catchup_mode is False
    assert processor._last_signal is None
    assert processor._signal_count == 1
    assert processor.publisher.published == [arm.to_stream_map()]


async def test_handle_catchup_no_signal_no_publish() -> None:
    processor = SymbolProcessor.__new__(SymbolProcessor)
    processor._catchup_mode = True
    processor._candle_count = 10
    processor._signal_count = 2
    processor.sym = "ETHUSDT"
    processor.publisher = DummyPublisher()
    processor._last_signal = None
    processor._is_caught_up = lambda _: True  # type: ignore[assignment]

    await processor._handle_catchup_transition(2000)

    assert processor._catchup_mode is False
    assert processor._signal_count == 2
    assert processor.publisher.published == []
