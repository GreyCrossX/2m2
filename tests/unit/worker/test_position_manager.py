import pytest

from dataclasses import replace
from decimal import Decimal
from uuid import uuid4

from app.services.worker.application.position_manager import PositionManager
from app.services.worker.domain.enums import OrderSide, OrderStatus
from app.services.worker.domain.models import OrderState


class BinanceClientStub:
    def __init__(self) -> None:
        self.closed_calls: list[tuple[str, str, Decimal]] = []

    async def close_position_market(self, symbol: str, side: str, quantity: Decimal) -> dict:
        self.closed_calls.append((symbol, side, quantity))
        return {}

    async def get_mark_price(self, symbol: str) -> Decimal:
        raise NotImplementedError

    async def get_open_position_qty(self, symbol: str) -> Decimal:
        raise NotImplementedError


def _filled_state(quantity: Decimal) -> OrderState:
    return OrderState(
        bot_id=uuid4(),
        signal_id="msg",
        status=OrderStatus.FILLED,
        side=OrderSide.LONG,
        symbol="BTCUSDT",
        trigger_price=Decimal("100"),
        stop_price=Decimal("95"),
        quantity=quantity,
    )


@pytest.mark.asyncio
async def test_open_position_tracks_multiple_entries() -> None:
    client = BinanceClientStub()
    manager = PositionManager(client)

    bot_id = uuid4()
    state_one = replace(_filled_state(Decimal("0.1")), bot_id=bot_id)
    state_two = replace(_filled_state(Decimal("0.2")), bot_id=bot_id)

    await manager.open_position(bot_id, state_one)
    await manager.open_position(bot_id, state_two)

    positions = manager.get_positions(bot_id)

    assert len(positions) == 2
    assert manager.get_position(bot_id) == positions[-1]
    assert positions[-1].quantity == Decimal("0.2")


@pytest.mark.asyncio
async def test_close_position_sums_quantities() -> None:
    client = BinanceClientStub()
    manager = PositionManager(client)

    bot_id = uuid4()
    state_one = replace(_filled_state(Decimal("0.1")), bot_id=bot_id)
    state_two = replace(_filled_state(Decimal("0.3")), bot_id=bot_id)

    await manager.open_position(bot_id, state_one)
    await manager.open_position(bot_id, state_two)

    await manager.close_position(bot_id, "exit")

    assert manager.get_positions(bot_id) == []
    assert client.closed_calls == [("BTCUSDT", "SELL", Decimal("0.4"))]
