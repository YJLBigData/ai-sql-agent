from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from sql_ai_copilot.config.settings import get_settings


_LOGGING_READY = False


def configure_logging(log_dir: Path) -> None:
    global _LOGGING_READY
    if _LOGGING_READY:
        return

    log_dir.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(
        log_dir / "sql_agent.log",
        maxBytes=64 * 1024 * 1024,
        backupCount=16,
        encoding="utf-8",
    )
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    handler.setFormatter(formatter)

    root_logger = logging.getLogger("sql_agent")
    root_logger.setLevel(logging.INFO)
    root_logger.propagate = False
    if not root_logger.handlers:
        root_logger.addHandler(handler)

    _LOGGING_READY = True


def get_logger(name: str) -> logging.Logger:
    settings = get_settings()
    configure_logging(settings.log_dir)
    if name.startswith("sql_agent"):
        return logging.getLogger(name)
    return logging.getLogger(f"sql_agent.{name}")
