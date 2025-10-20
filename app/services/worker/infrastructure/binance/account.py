from __future__ import annotations

import asyncio
import re
from decimal import Decimal, ROUND_DOWN
from typing import Dict, List, Optional, Tuple

# ✅ Modular USDⓈ-M SDK (your fixed imports)
from binance_sdk_derivatives_trading_usds_futures.rest_api.rest_api import (
    AccountApi as RestAccount,
    TradeApi as RestTrade,
    MarketDataApi as RestMarket,
    UserDataStreamsApi as RestUserStream,
)


# -------- Errors (optional but handy) --------
class BinanceAPIError(Exception):
    def __init__(self, code: int, message: str):
        super().__init__(f"[{code}] {message}")
        self.code = code
        self.message = message

class InsufficientBalanceError(BinanceAPIError): ...
# You can add more typed errors if useful


class BinanceClient:
    """
    Async facade over the modular USDⓈ-M Futures SDK.
    Adds:
      - _execute(): thread offload + uniform error mapping
      - quantize(): price/qty rounding using exchange filters
    """

    def __init__(self, api_key: str, api_secret: str, testnet: bool = False, timeout: int = 30):
        if not api_key or not api_secret:
            raise ValueError("API key and secret are required")

        base_url = "https://testnet.binancefuture.com" if testnet else "https://fapi.binance.com"

        self._account = RestAccount(api_key=api_key, api_secret=api_secret, base_url=base_url, timeout=timeout)
        self._trade = RestTrade(api_key=api_key, api_secret=api_secret, base_url=base_url, timeout=timeout)
        self._market = RestMarket(base_url=base_url, timeout=timeout)
        self._user_stream = RestUserStream(api_key=api_key, api_secret=api_secret, base_url=base_url, timeout=timeout)

        self._base_url = base_url
        self._testnet = testnet

    # ---------- internal helpers ----------

    async def _execute(self, fn, *args, **kwargs):
        try:
            return await asyncio.to_thread(fn, *args, **kwargs)
        except Exception as e:
            msg = str(e)
            # try to pull a numeric code
            code = getattr(e, "code", None) or getattr(e, "error_code", None)
            if code is None:
                m = re.search(r'"?code"?[:\s"]*(-?\d+)', msg)
                code = int(m.group(1)) if m else -1
            if code == -2019:
                raise InsufficientBalanceError(code, msg)
            if code in (-1111,):
                raise BinanceAPIError(code, f"Precision error: {msg}")
            if code in (-1021,):
                raise BinanceAPIError(code, f"Timestamp sync error: {msg}")
            raise BinanceAPIError(code, msg)

    @staticmethod
    def _step_quantize(value: Decimal, step: Decimal) -> Decimal:
        if step <= 0:
            return value
        # quantize to integer multiple of step with floor
        return (value / step).to_integral_value(rounding=ROUND_DOWN) * step

    # pull filters (cached at caller’s discretion)
    async def get_filters(self, symbol: str) -> Dict[str, Dict]:
        info = await self.exchange_info()
        for s in info.get("symbols", []):
            if s.get("symbol") == symbol:
                return {f["filterType"]: f for f in s.get("filters", [])}
        return {}

    async def quantize(self, symbol: str, qty: Decimal, price: Decimal) -> Tuple[Decimal, Decimal]:
        """
        Returns (qty_q, price_q) snapped to LOT_SIZE.stepSize and PRICE_FILTER.tickSize.
        """
        filters = await self.get_filters(symbol)
        lot = Decimal(str(filters.get("LOT_SIZE", {}).get("stepSize", "0")))
        tick = Decimal(str(filters.get("PRICE_FILTER", {}).get("tickSize", "0")))
        q_qty = self._step_quantize(qty, lot) if lot > 0 else qty
        q_price = self._step_quantize(price, tick) if tick > 0 else price
        return q_qty, q_price

    # ---------- Account / Positions ----------

    async def account(self) -> Dict:
        return await self._execute(self._account.account_information_v2)

    async def balance(self) -> List[Dict]:
        return await self._execute(self._account.futures_account_balance)

    async def position_risk(self, symbol: Optional[str] = None) -> List[Dict]:
        return await self._execute(self._account.position_risk, symbol=symbol)

    # ---------- Trading ----------

    async def change_leverage(self, symbol: str, leverage: int) -> Dict:
        return await self._execute(self._account.change_initial_leverage, symbol=symbol, leverage=leverage)

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
        params = dict(symbol=symbol, side=side, type=order_type, quantity=quantity)
        if price is not None:
            params["price"] = price
        if stopPrice is not None:
            params["stopPrice"] = stopPrice
        if timeInForce and order_type == "LIMIT":
            params["timeInForce"] = timeInForce
        params.update(kwargs)
        return await self._execute(self._trade.new_order, **params)

    async def cancel_order(self, symbol: str, orderId: int) -> Dict:
        return await self._execute(self._trade.cancel_order, symbol=symbol, orderId=orderId)

    async def query_order(self, symbol: str, orderId: int) -> Dict:
        return await self._execute(self._trade.query_order, symbol=symbol, orderId=orderId)

    async def get_orders(self, symbol: Optional[str] = None) -> List[Dict]:
        return await self._execute(self._trade.current_open_orders, symbol=symbol)

    # ---------- Market data ----------

    async def exchange_info(self) -> Dict:
        return await self._execute(self._market.exchange_information)

    async def ticker_price(self, symbol: str) -> Dict:
        return await self._execute(self._market.symbol_price_ticker, symbol=symbol)

    # ---------- User Data Stream ----------

    async def new_listen_key(self) -> str:
        data = await self._execute(self._user_stream.start_user_data_stream)
        return data.get("listenKey", "")

    async def keepalive_listen_key(self, listen_key: str) -> None:
        await self._execute(self._user_stream.keepalive_user_data_stream, listenKey=listen_key)

    async def close_listen_key(self, listen_key: str) -> None:
        await self._execute(self._user_stream.close_user_data_stream, listenKey=listen_key)

    # ---------- Properties ----------

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def is_testnet(self) -> bool:
        return self._testnet
