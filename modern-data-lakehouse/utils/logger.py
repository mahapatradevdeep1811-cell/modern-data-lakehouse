"""
logger.py
~~~~~~~~~
Centralised logging setup with structured JSON output for production
and human-readable output for development.
"""

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Optional


class _JsonFormatter(logging.Formatter):
    """Emit logs as newline-delimited JSON for log aggregation systems."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "line": record.lineno,
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)


class _DevFormatter(logging.Formatter):
    _COLORS = {
        "DEBUG": "\033[36m",
        "INFO": "\033[32m",
        "WARNING": "\033[33m",
        "ERROR": "\033[31m",
        "CRITICAL": "\033[35m",
    }
    _RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        color = self._COLORS.get(record.levelname, "")
        ts = datetime.now().strftime("%H:%M:%S")
        prefix = f"{color}[{record.levelname[0]}]{self._RESET}"
        return f"{ts} {prefix} {record.name}: {record.getMessage()}"


def get_logger(name: str, level: Optional[str] = None) -> logging.Logger:
    """
    Return a named logger.  Uses JSON format in production, coloured
    human-readable format in development/testing.
    """
    from utils.config_loader import get_config
    cfg = get_config()
    env = cfg.get("app", {}).get("env", "development")
    log_level = level or cfg.get("app", {}).get("log_level", "INFO")

    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # Already configured

    logger.setLevel(log_level)
    handler = logging.StreamHandler(sys.stdout)

    if env == "production":
        handler.setFormatter(_JsonFormatter())
    else:
        handler.setFormatter(_DevFormatter())

    logger.addHandler(handler)
    logger.propagate = False
    return logger
