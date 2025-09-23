#!/bin/sh
set -e
unset CELERY_APP
export PYTHONPATH=/app
exec celery -A app.celery_app:celery worker -Q signals -l INFO -c 2
