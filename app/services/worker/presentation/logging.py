from __future__ import annotations

import json
import logging
import os
import sys
from typing import Any, Dict


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)

def setup_logging(level: str = "INFO") -> None:
    logger = logging.getLogger()
    logger.setLevel(level.upper())
    handler = logging.StreamHandler(sys.stdout)
    if os.getenv("LOG_JSON", "1") in ("1", "true", "TRUE"):
        handler.setFormatter(JsonFormatter())
    logger.handlers[:] = [handler]
