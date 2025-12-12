from __future__ import annotations

from typing import Any, Dict, Mapping

_VALID_SIDES = {"BUY", "SELL"}
_VALID_TYPES = {
    "LIMIT",
    "MARKET",
    "STOP_MARKET",
    "TAKE_PROFIT_MARKET",
    "TAKE_PROFIT",
    "TAKE_PROFIT_LIMIT",
}
_VALID_TIF = {"GTC", "IOC", "FOK", "GTX", "GTE_GTC"}
_VALID_POSITION_SIDES = {"BOTH", "LONG", "SHORT"}
_VALID_WORKING_TYPES = {"CONTRACT_PRICE", "MARK_PRICE"}


def _upper_if_str(value: Any) -> Any:
    return value.upper() if isinstance(value, str) else value


def _require(payload: Mapping[str, Any], key: str) -> None:
    if payload.get(key) is None:
        raise ValueError(f"Missing required field '{key}' for Binance order")


def _validate_choice(value: Any, valid: set[str], field: str) -> str:
    if value is None:
        raise ValueError(f"Missing required field '{field}' for Binance order")
    normalized = str(value).upper()
    if normalized not in valid:
        raise ValueError(f"Invalid {field} '{value}'")
    return normalized


def validate_new_order_payload(params: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Validate and normalize a Binance futures order payload.

    - Enforces required combos per type (price/timeInForce for LIMIT; stopPrice for STOP/TP market; price+stopPrice+timeInForce for TP LIMIT).
    - Uppercases enum-ish fields.
    - Validates enum membership for side/type/timeInForce/positionSide/workingType.
    - Drops None-valued keys.
    """

    payload: Dict[str, Any] = dict(params)

    order_type = _validate_choice(payload.get("type"), _VALID_TYPES, "type")
    payload["type"] = order_type

    payload["side"] = _validate_choice(payload.get("side"), _VALID_SIDES, "side")
    if "timeInForce" in payload and payload.get("timeInForce") is not None:
        payload["timeInForce"] = _validate_choice(
            payload.get("timeInForce"), _VALID_TIF, "timeInForce"
        )

    if payload.get("positionSide") is not None:
        payload["positionSide"] = _validate_choice(
            payload.get("positionSide"), _VALID_POSITION_SIDES, "positionSide"
        )
    if payload.get("workingType") is not None:
        payload["workingType"] = _validate_choice(
            payload.get("workingType"), _VALID_WORKING_TYPES, "workingType"
        )

    # Normalize symbol casing
    if payload.get("symbol") is not None:
        payload["symbol"] = _upper_if_str(payload["symbol"])

    # Required combos by type
    if order_type == "LIMIT":
        _require(payload, "price")
        _require(payload, "timeInForce")
    elif order_type in {"STOP_MARKET", "TAKE_PROFIT_MARKET"}:
        _require(payload, "stopPrice")
    elif order_type in {"TAKE_PROFIT", "TAKE_PROFIT_LIMIT"}:
        _require(payload, "price")
        _require(payload, "stopPrice")
        _require(payload, "timeInForce")

    # Quantity: required unless closePosition=True (Binance allows omit qty on closePosition STOP/TP market)
    close_position = str(payload.get("closePosition", "")).lower() == "true" or bool(
        payload.get("closePosition", False)
    )
    if order_type not in {"STOP_MARKET", "TAKE_PROFIT_MARKET"} or not close_position:
        if payload.get("quantity") is None:
            raise ValueError("Missing required field 'quantity' for Binance order")

    # Drop None values so signatures match server schema
    cleaned = {k: v for k, v in payload.items() if v is not None}
    return cleaned


def validate_query_or_cancel_payload(params: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Validate payload for query_order/cancel_order/open_orders.

    Rules:
    - symbol required (uppercased).
    - For query/cancel: at least one of orderId or origClientOrderId.
    """
    payload: Dict[str, Any] = dict(params)
    if not payload.get("symbol"):
        raise ValueError(
            "Missing required field 'symbol' for Binance order query/cancel"
        )
    payload["symbol"] = _upper_if_str(payload["symbol"])

    if (
        not payload.get("orderId")
        and not payload.get("origClientOrderId")
        and payload.get("type") != "open_orders"
    ):
        raise ValueError(
            "Provide orderId or origClientOrderId for Binance order query/cancel"
        )

    return {k: v for k, v in payload.items() if v is not None}
