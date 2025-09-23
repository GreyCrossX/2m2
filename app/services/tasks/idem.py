"""
Idempotency helpers (thin wrappers).
"""

from __future__ import annotations

from .state import is_signal_processed, mark_signal_processed


def already_processed(bot_id: str, signal_id: str) -> bool:
    return is_signal_processed(bot_id, signal_id)


def mark_processed(bot_id: str, signal_id: str) -> int:
    return mark_signal_processed(bot_id, signal_id)
