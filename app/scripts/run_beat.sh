#!/bin/sh
set -e
unset CELERY_APP
export PYTHONPATH=/app
exec celery -A app.celery_app:celery beat -l INFO
