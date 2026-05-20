"""
SENTINEL retry utilities.
Tenacity-based exponential backoff with jitter.
"""
from __future__ import annotations

from typing import Any, Callable

from tenacity import (
    RetryError,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)
import structlog

logger = structlog.get_logger(__name__)


def with_retry(
    max_attempts: int = 3,
    base_wait: float = 1.0,
    max_wait: float = 300.0,
    retry_on: tuple[type[Exception], ...] = (Exception,),
) -> Callable:
    """
    Decorator for retrying functions with exponential backoff.

    Args:
        max_attempts: Maximum number of retry attempts.
        base_wait: Base wait time in seconds.
        max_wait: Maximum wait time in seconds.
        retry_on: Tuple of exception types to retry on.

    Returns:
        Decorated function with retry behavior.
    """
    return retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=base_wait, max=max_wait),
        retry=retry_if_exception_type(retry_on),
        reraise=True,
    )


async def retry_async(
    func: Callable,
    *args: Any,
    max_attempts: int = 3,
    base_wait: float = 1.0,
    max_wait: float = 300.0,
    **kwargs: Any,
) -> Any:
    """
    Retry an async function with exponential backoff.

    Args:
        func: Async function to call.
        *args: Positional arguments.
        max_attempts: Maximum attempts.
        base_wait: Base wait time.
        max_wait: Max wait time.
        **kwargs: Keyword arguments.

    Returns:
        Result of the function call.

    Raises:
        The last exception if all retries are exhausted.
    """
    import asyncio

    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            last_exc = e
            if attempt < max_attempts:
                wait_time = min(base_wait * (2 ** (attempt - 1)), max_wait)
                logger.warning(
                    "retry_attempt",
                    function=func.__qualname__,
                    attempt=attempt,
                    max_attempts=max_attempts,
                    wait_seconds=wait_time,
                    error=str(e),
                )
                await asyncio.sleep(wait_time)
            else:
                logger.error(
                    "retry_exhausted",
                    function=func.__qualname__,
                    attempts=max_attempts,
                    error=str(e),
                )

    if last_exc:
        raise last_exc
