# celery_app.py
from __future__ import annotations

import os
import logging
from typing import Dict, Any

from celery import Celery
from kombu import Queue, Exchange

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("CELERY_LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
LOG = logging.getLogger("celery_app")

# ── Helpers ───────────────────────────────────────────────────────────────────
def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

# ── Celery app (named `celery`, not `app`) ────────────────────────────────────
BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/1")
RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/2")
TIMEZONE = os.getenv("TZ", "UTC")

celery = Celery("trading", broker=BROKER_URL, backend=RESULT_BACKEND)

signals_exchange = Exchange("signals", type="direct")
reconcile_exchange = Exchange("reconcile", type="direct")

celery.conf.task_queues = (
    Queue("signals", exchange=signals_exchange, routing_key="signals"),
    Queue("reconcile", exchange=reconcile_exchange, routing_key="reconcile"),
)

celery.conf.task_routes = {
    "app.services.tasks.handlers.*": {"queue": "signals", "routing_key": "signals"},
    "app.services.tasks.periodic.*": {"queue": "reconcile", "routing_key": "reconcile"},
}

celery.conf.update(
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    broker_transport_options={"visibility_timeout": _env_int("CELERY_VISIBILITY_TIMEOUT", 3600)},
    result_expires=_env_int("CELERY_RESULT_EXPIRES", 3600),
    worker_prefetch_multiplier=_env_int("CELERY_PREFETCH", 1),
    worker_max_tasks_per_child=_env_int("CELERY_MAX_TASKS_PER_CHILD", 200),
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone=TIMEZONE,
    enable_utc=True,
    task_default_queue="signals",
)

def _build_beat_schedule() -> Dict[str, Dict[str, Any]]:
    schedule: Dict[str, Dict[str, Any]] = {}
    symbols_csv = os.getenv("RECONCILE_SYMBOLS", "").strip()
    if not symbols_csv:
        return schedule
    interval = _env_int("RECONCILE_INTERVAL_SECONDS", 45)
    for sym in [s for s in (x.strip() for x in symbols_csv.split(",")) if s]:
        schedule[f"reconcile_symbol_bots:{sym}"] = {
            "task": "app.services.tasks.periodic.reconcile_symbol_bots",
            "schedule": interval,
            "args": (sym,),
            "options": {"queue": "reconcile", "routing_key": "reconcile"},
        }
    return schedule

celery.conf.beat_schedule = _build_beat_schedule()
if celery.conf.beat_schedule:
    LOG.info("Beat schedule enabled for symbols: %s", list(celery.conf.beat_schedule.keys()))
else:
    LOG.info("Beat schedule is disabled (set RECONCILE_SYMBOLS to enable).")

# Import tasks so Celery registers them
import app.services.tasks.handlers   # noqa: E402,F401
import app.services.tasks.periodic   # noqa: E402,F401

LOG.info("Celery app initialized. Broker=%s Backend=%s TZ=%s", BROKER_URL, RESULT_BACKEND, TIMEZONE)

# Export only the Celery instance for -A target
__all__ = ["celery"]
