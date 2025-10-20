from __future__ import annotations

import asyncio
from typing import Dict, List, Optional

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
