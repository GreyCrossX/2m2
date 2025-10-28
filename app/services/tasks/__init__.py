"""Legacy task-layer facade wiring.

This package exposes synchronous helper functions used by Celery tasks and
scripts.  Modern code paths in ``app.services.worker`` provide the async
implementations; these wrappers keep backwards compatibility for tests and
tooling that still import ``app.services.tasks.*``.
"""
