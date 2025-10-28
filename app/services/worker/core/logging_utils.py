from __future__ import annotations

from typing import Any, Mapping, MutableMapping

_CONTEXT_FIELDS = ("symbol", "tf", "type", "msg_id", "prev_side")


def format_log_context(context: Mapping[str, Any]) -> str:
    """Return a stable log-friendly string for the common signal context."""
    parts: list[str] = []
    for field in _CONTEXT_FIELDS:
        value = context.get(field, "") if context else ""
        if value in (None, ""):
            value = "-"
        parts.append(f"{field}={value}")
    return " ".join(parts)


def ensure_log_context(context: Mapping[str, Any] | None, **updates: Any) -> MutableMapping[str, Any]:
    """Copy the provided context and merge additional fields for downstream logs."""
    merged: MutableMapping[str, Any] = dict(context or {})
    for key, value in updates.items():
        if value is None:
            continue
        merged[key] = value
    return merged


__all__ = ["format_log_context", "ensure_log_context"]
