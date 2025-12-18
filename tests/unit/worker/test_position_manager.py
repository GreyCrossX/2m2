from dataclasses import replace
from decimal import Decimal
from uuid import uuid4

from app.services.worker.application.position_manager import PositionManager
from app.services.worker.domain.enums import OrderSide, OrderStatus
from app.services.worker.domain.models import OrderState


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
        filled_quantity=quantity,
    )


async def test_open_position_tracks_multiple_entries() -> None:
    manager = PositionManager()

    bot_id = uuid4()
    state_one = replace(_filled_state(Decimal("0.1")), bot_id=bot_id)
    state_two = replace(_filled_state(Decimal("0.2")), bot_id=bot_id)

    await manager.open_position(bot_id, state_one)
    await manager.open_position(bot_id, state_two, allow_pyramiding=True)

    positions = manager.get_positions(bot_id)
    aggregate = manager.get_position(bot_id)

    assert aggregate is not None
    assert aggregate.quantity.quantize(Decimal("0.0001")) == Decimal("0.3000")
    assert len(positions) == 2
    assert positions[0].quantity >= Decimal("0.1")
    assert positions[1].quantity == Decimal("0.2")


async def test_close_position_sums_quantities() -> None:
    manager = PositionManager()

    bot_id = uuid4()
    state_one = replace(_filled_state(Decimal("0.1")), bot_id=bot_id)
    state_two = replace(_filled_state(Decimal("0.3")), bot_id=bot_id)

    await manager.open_position(bot_id, state_one)
    await manager.open_position(bot_id, state_two)

    await manager.close_position(bot_id, "exit")

    assert manager.get_positions(bot_id) == []
