"""
SENTINEL structured logging configuration.
Uses structlog for JSON file logging and pretty console output.
"""
from __future__ import annotations

import functools
import logging
import sys
import time
from pathlib import Path
from typing import Any, Callable

import structlog


def setup_logging(log_level: str = "INFO", data_dir: str = "./data") -> None:
    """
    Configure structlog with JSON file output and pretty console output.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR).
        data_dir: Data directory for log files.
    """
    log_dir = Path(data_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "sentinel.log"

    # Configure stdlib logging for file handler
    file_handler = logging.FileHandler(str(log_file))
    file_handler.setLevel(getattr(logging, log_level.upper()))

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level.upper()))

    logging.basicConfig(
        format="%(message)s",
        level=getattr(logging, log_level.upper()),
        handlers=[file_handler, console_handler],
        force=True,
    )

    # Configure structlog
    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Set formatters on handlers
    json_formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.processors.JSONRenderer(),
    )
    console_formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.dev.ConsoleRenderer(colors=True),
    )

    file_handler.setFormatter(json_formatter)
    console_handler.setFormatter(console_formatter)


def log_duration(func: Callable) -> Callable:
    """
    Decorator to automatically log function execution duration.

    Works with both sync and async functions.
    """
    if asyncio_iscoroutinefunction(func):
        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            logger = structlog.get_logger(func.__module__)
            start = time.perf_counter()
            try:
                result = await func(*args, **kwargs)
                duration_ms = (time.perf_counter() - start) * 1000
                logger.debug(
                    "function_completed",
                    function=func.__qualname__,
                    duration_ms=round(duration_ms, 2),
                )
                return result
            except Exception as e:
                duration_ms = (time.perf_counter() - start) * 1000
                logger.error(
                    "function_failed",
                    function=func.__qualname__,
                    duration_ms=round(duration_ms, 2),
                    error=str(e),
                )
                raise
        return async_wrapper
    else:
        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            logger = structlog.get_logger(func.__module__)
            start = time.perf_counter()
            try:
                result = func(*args, **kwargs)
                duration_ms = (time.perf_counter() - start) * 1000
                logger.debug(
                    "function_completed",
                    function=func.__qualname__,
                    duration_ms=round(duration_ms, 2),
                )
                return result
            except Exception as e:
                duration_ms = (time.perf_counter() - start) * 1000
                logger.error(
                    "function_failed",
                    function=func.__qualname__,
                    duration_ms=round(duration_ms, 2),
                    error=str(e),
                )
                raise
        return sync_wrapper


def asyncio_iscoroutinefunction(func: Callable) -> bool:
    """Check if a function is an async coroutine function."""
    import asyncio
    return asyncio.iscoroutinefunction(func)
