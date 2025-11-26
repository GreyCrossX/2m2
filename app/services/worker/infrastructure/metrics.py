from __future__ import annotations

import logging
from typing import Dict

try:  # pragma: no cover - optional dependency
    from prometheus_client import Counter
except Exception:  # pragma: no cover - fallback when prometheus not installed
    Counter = None  # type: ignore[assignment]


logger = logging.getLogger(__name__)


class WorkerMetrics:
    """Best-effort metrics facade for the worker."""

    def __init__(self) -> None:
        self._use_prom = Counter is not None
        if self._use_prom:
            assert Counter is not None  # satisfy type checker
            self._signals_processed = Counter(
                "signals_processed_total",
                "Total worker signals processed",
                labelnames=("status",),
            )
            self._signals_duplicate = Counter(
                "signals_duplicate_skipped_total",
                "Worker signals skipped because they were duplicates",
            )
            self._signals_ack_failed = Counter(
                "signals_ack_failed_total",
                "Worker signals that failed to ACK",
            )
            self._order_monitor_events = Counter(
                "order_monitor_events_total",
                "Order monitor outcomes",
                labelnames=("event",),
            )
            self._binance_errors = Counter(
                "binance_errors_total",
                "Normalized Binance/API errors surfaced by the worker",
                labelnames=("kind",),
            )
        else:
            self.signals_processed_total: Dict[str, int] = {}
            self.signals_duplicate_skipped_total = 0
            self.signals_ack_failed_total = 0
            self.order_monitor_events: Dict[str, int] = {}
            self.binance_errors: Dict[str, int] = {}
        self.last_signal_lag_ms: float | None = None
        self.max_signal_lag_ms: float | None = None

    def inc_processed(self, status: str = "success") -> None:
        if self._use_prom:
            self._signals_processed.labels(status=status).inc()
            return
        self.signals_processed_total[status] = (
            self.signals_processed_total.get(status, 0) + 1
        )
        logger.debug(
            "signals_processed_total[%s]=%d",
            status,
            self.signals_processed_total[status],
        )

    def inc_duplicate(self) -> None:
        if self._use_prom:
            self._signals_duplicate.inc()
            return
        self.signals_duplicate_skipped_total += 1
        logger.debug(
            "signals_duplicate_skipped_total=%d", self.signals_duplicate_skipped_total
        )

    def inc_ack_failed(self) -> None:
        if self._use_prom:
            self._signals_ack_failed.inc()
            return
        self.signals_ack_failed_total += 1
        logger.debug("signals_ack_failed_total=%d", self.signals_ack_failed_total)

    def observe_signal_lag_ms(self, lag_ms: float) -> None:
        self.last_signal_lag_ms = lag_ms
        if self.max_signal_lag_ms is None or lag_ms > self.max_signal_lag_ms:
            self.max_signal_lag_ms = lag_ms
        logger.debug(
            "signal_lag_ms updated | last=%.1f max=%.1f",
            self.last_signal_lag_ms,
            self.max_signal_lag_ms or 0,
        )

    def inc_order_monitor_event(self, event: str) -> None:
        if self._use_prom:
            self._order_monitor_events.labels(event=event).inc()
            return
        self.order_monitor_events[event] = self.order_monitor_events.get(event, 0) + 1
        logger.debug(
            "order_monitor_events[%s]=%d", event, self.order_monitor_events[event]
        )

    def inc_binance_error(self, kind: str) -> None:
        if self._use_prom:
            self._binance_errors.labels(kind=kind).inc()
            return
        self.binance_errors[kind] = self.binance_errors.get(kind, 0) + 1
        logger.debug("binance_errors[%s]=%d", kind, self.binance_errors[kind])


__all__ = ["WorkerMetrics"]
