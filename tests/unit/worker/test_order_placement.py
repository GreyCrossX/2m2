from decimal import Decimal

from app.services.order_placement import OrderPlacementService
from app.services.worker.domain.enums import OrderSide


class TradingStub:
    def __init__(self) -> None:
        self.cancelled: list[int] = []

    async def create_limit_order(
        self,
        *,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        price: Decimal,
        reduce_only: bool,
        time_in_force: str,
        new_client_order_id: str | None = None,
    ) -> dict:
        return {"order_id": 11}

    async def create_stop_market_order(
        self,
        *,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        stop_price: Decimal,
        reduce_only: bool,
        order_type: str,
        time_in_force: str | None = None,
        new_client_order_id: str | None = None,
    ) -> dict:
        if order_type == "STOP_MARKET":
            return {"order_id": 22}
        return {"order_id": 33}

    async def cancel_order(self, *, symbol: str, order_id: int) -> None:
        self.cancelled.append(int(order_id))


async def test_place_trio_orders_extracts_snake_case_order_ids() -> None:
    placement = OrderPlacementService(TradingStub())
    result = await placement.place_trio_orders(
        symbol="BTCUSDT",
        side=OrderSide.LONG,
        quantity=Decimal("0.001"),
        entry_price=Decimal("100"),
        stop_price=Decimal("95"),
        take_profit_price=Decimal("105"),
        stop_client_order_id="sl",
        tp_client_order_id="tp",
        context={},
    )

    assert result.entry_order_id == 11
    assert result.stop_order_id == 22
    assert result.take_profit_order_id == 33
