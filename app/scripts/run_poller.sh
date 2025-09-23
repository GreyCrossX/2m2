#!/bin/sh
set -e
unset CELERY_APP
export PYTHONPATH=/app
# $1 is POLLER_SYMBOLS (comma-separated) or falls back to env
SYMS="${1:-$POLLER_SYMBOLS}"
if [ -z "$SYMS" ]; then SYMS="BTCUSDT"; fi
exec python -m app.services.tasks.run_poller $SYMS
