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
        else:
            self.signals_processed_total: Dict[str, int] = {}
            self.signals_duplicate_skipped_total = 0
            self.signals_ack_failed_total = 0

    def inc_processed(self, status: str = "success") -> None:
        if self._use_prom:
            self._signals_processed.labels(status=status).inc()
            return
        self.signals_processed_total[status] = self.signals_processed_total.get(status, 0) + 1
        logger.debug("signals_processed_total[%s]=%d", status, self.signals_processed_total[status])

    def inc_duplicate(self) -> None:
        if self._use_prom:
            self._signals_duplicate.inc()
            return
        self.signals_duplicate_skipped_total += 1
        logger.debug("signals_duplicate_skipped_total=%d", self.signals_duplicate_skipped_total)

    def inc_ack_failed(self) -> None:
        if self._use_prom:
            self._signals_ack_failed.inc()
            return
        self.signals_ack_failed_total += 1
        logger.debug("signals_ack_failed_total=%d", self.signals_ack_failed_total)


__all__ = ["WorkerMetrics"]
