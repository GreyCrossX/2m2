from __future__ import annotations

import asyncio
from decimal import Decimal, ROUND_DOWN
from typing import Dict, List, Optional, Tuple

# ✅ Correct modular SDK imports
from binance_sdk_derivatives_trading_usds_futures.rest_api.rest_api import (
    AccountApi as RestAccount,
    TradeApi as RestTrade,
    MarketDataApi as RestMarket,
    UserDataStreamsApi as RestUserStream,
)


class BinanceClient:
    """
    Async facade over Binance's modular USDⓈ-M Futures SDK.
    Methods mirror your Application layer expectations.
    """

    def __init__(self, api_key: str, api_secret: str, testnet: bool = False, timeout: int = 30):
        if not api_key or not api_secret:
            raise ValueError("API key and secret are required")

        base_url = "https://testnet.binancefuture.com" if testnet else "https://fapi.binance.com"

        # Each surface is a small client; the constructors accept base_url, api_key, api_secret, timeout
        self._account = RestAccount(
            api_key=api_key, api_secret=api_secret, base_url=base_url, timeout=timeout
        )
        self._trade = RestTrade(
            api_key=api_key, api_secret=api_secret, base_url=base_url, timeout=timeout
        )
        self._market = RestMarket(base_url=base_url, timeout=timeout)
        self._user_stream = RestUserStream(
            api_key=api_key, api_secret=api_secret, base_url=base_url, timeout=timeout
        )

        self._base_url = base_url
        self._testnet = testnet
        self._symbol_filters: Dict[str, Dict[str, Decimal]] = {}
        self._filters_lock = asyncio.Lock()

    # ---------- Account / Positions ----------

    async def account(self) -> Dict:
        # GET /fapi/v2/account
        return await asyncio.to_thread(self._account.account_information_v2)

    async def balance(self) -> List[Dict]:
        # GET /fapi/v2/balance
        return await asyncio.to_thread(self._account.futures_account_balance)

    async def position_risk(self, symbol: Optional[str] = None) -> List[Dict]:
        # GET /fapi/v2/positionRisk
        return await asyncio.to_thread(self._account.position_risk, symbol=symbol)

    # ---------- Trading ----------

    async def change_leverage(self, symbol: str, leverage: int) -> Dict:
        # POST /fapi/v1/leverage
        return await asyncio.to_thread(self._account.change_initial_leverage, symbol=symbol, leverage=leverage)

    async def new_order(
        self,
        symbol: str,
        side: str,        # "BUY" | "SELL"
        order_type: str,  # "LIMIT" | "MARKET" | "STOP_MARKET" | ...
        quantity: float,
        price: Optional[float] = None,
        stopPrice: Optional[float] = None,
        timeInForce: str = "GTC",
        **kwargs,
    ) -> Dict:
        # POST /fapi/v1/order
        params = dict(symbol=symbol, side=side, type=order_type, quantity=quantity)
        if price is not None:
            params["price"] = price
        if stopPrice is not None:
            params["stopPrice"] = stopPrice
        if timeInForce and order_type == "LIMIT":
            params["timeInForce"] = timeInForce
        params.update(kwargs)
        return await asyncio.to_thread(self._trade.new_order, **params)

    async def cancel_order(self, symbol: str, orderId: int) -> Dict:
        # DELETE /fapi/v1/order
        return await asyncio.to_thread(self._trade.cancel_order, symbol=symbol, orderId=orderId)

    async def query_order(self, symbol: str, orderId: int) -> Dict:
        # GET /fapi/v1/order
        return await asyncio.to_thread(self._trade.query_order, symbol=symbol, orderId=orderId)

    async def get_orders(self, symbol: Optional[str] = None) -> List[Dict]:
        # GET /fapi/v1/openOrders
        return await asyncio.to_thread(self._trade.current_open_orders, symbol=symbol)

    # ---------- Market data ----------

    async def exchange_info(self) -> Dict:
        # GET /fapi/v1/exchangeInfo
        return await asyncio.to_thread(self._market.exchange_information)

    async def ticker_price(self, symbol: str) -> Dict:
        # GET /fapi/v1/ticker/price
        return await asyncio.to_thread(self._market.symbol_price_ticker, symbol=symbol)

    # ---------- Exchange filters / quantization ----------

    async def _get_symbol_filters(self, symbol: str) -> Dict[str, Decimal]:
        sym = symbol.upper()
        cached = self._symbol_filters.get(sym)
        if cached:
            return cached

        async with self._filters_lock:
            cached = self._symbol_filters.get(sym)
            if cached:
                return cached

            info = await self.exchange_info()
            symbols = info.get("symbols", []) if isinstance(info, dict) else []
            filters_map: Dict[str, Dict[str, str]] = {}
            for entry in symbols:
                if str(entry.get("symbol", "")).upper() != sym:
                    continue
                raw_filters = entry.get("filters") or []
                filters_map = {str(f.get("filterType")): f for f in raw_filters if isinstance(f, dict)}
                break

            if not filters_map:
                raise ValueError(f"Exchange info does not contain symbol {sym}")

            lot_filter = filters_map.get("LOT_SIZE") or filters_map.get("MARKET_LOT_SIZE") or {}
            price_filter = filters_map.get("PRICE_FILTER") or {}
            min_notional_filter = filters_map.get("MIN_NOTIONAL") or {}

            def _to_decimal(value: str | float | int | None) -> Decimal:
                if value in (None, ""):
                    return Decimal("0")
                return Decimal(str(value))

            filters = {
                "step_size": _to_decimal(lot_filter.get("stepSize")),
                "min_qty": _to_decimal(lot_filter.get("minQty")),
                "tick_size": _to_decimal(price_filter.get("tickSize")),
                "min_notional": _to_decimal(
                    min_notional_filter.get("notional") or min_notional_filter.get("minNotional")
                ),
            }

            self._symbol_filters[sym] = filters
            return filters

    @staticmethod
    def _round_step(value: Decimal, step: Decimal) -> Decimal:
        if step <= 0:
            return value
        ratio = (value / step).to_integral_value(rounding=ROUND_DOWN)
        quantized = ratio * step
        # Preserve the same exponent as the step to avoid representation drift
        try:
            return quantized.quantize(step)
        except Exception:
            return quantized

    async def quantize(
        self,
        symbol: str,
        quantity: Decimal,
        price: Optional[Decimal] = None,
    ) -> Tuple[Decimal, Optional[Decimal]]:
        filters = await self._get_symbol_filters(symbol)
        qty = quantity if isinstance(quantity, Decimal) else Decimal(str(quantity))
        q_price = price if (price is None or isinstance(price, Decimal)) else Decimal(str(price))

        step_size = filters.get("step_size", Decimal("0"))
        min_qty = filters.get("min_qty", Decimal("0"))
        tick_size = filters.get("tick_size", Decimal("0"))
        min_notional = filters.get("min_notional", Decimal("0"))

        if qty < 0:
            qty = Decimal("0")
        qty = self._round_step(qty, step_size) if step_size > 0 else qty
        if min_qty > 0 and qty < min_qty:
            qty = Decimal("0")

        if q_price is not None and tick_size > 0:
            q_price = self._round_step(q_price, tick_size)

        if min_notional > 0 and q_price is not None and qty > 0:
            notional = qty * q_price
            if notional < min_notional:
                qty = Decimal("0")

        return qty, q_price

    # ---------- User Data Stream (listen key) ----------

    async def new_listen_key(self) -> str:
        data = await asyncio.to_thread(self._user_stream.start_user_data_stream)
        return data.get("listenKey", "")

    async def keepalive_listen_key(self, listen_key: str) -> None:
        await asyncio.to_thread(self._user_stream.keepalive_user_data_stream, listenKey=listen_key)

    async def close_listen_key(self, listen_key: str) -> None:
        await asyncio.to_thread(self._user_stream.close_user_data_stream, listenKey=listen_key)

    # ---------- Properties ----------

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def is_testnet(self) -> bool:
        return self._testnet
