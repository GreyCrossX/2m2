from typing import Any

class Celery:
    conf: Any
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
    def task(self, *args: Any, **kwargs: Any) -> Any: ...

__all__ = ["Celery"]
