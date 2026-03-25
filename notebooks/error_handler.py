"""
utils/error_handler.py
----------------------
Centralised error handling and retry logic for pipeline stages.
Provides decorators and context managers for consistent error management.
"""

import functools
import time
from typing import Callable, Type, Tuple
from utils.logger import get_logger

logger = get_logger(__name__)


def with_retry(
    max_attempts: int = 3,
    delay_seconds: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,)
) -> Callable:
    """
    Decorator that retries a function on failure.

    Args:
        max_attempts: Maximum number of retry attempts
        delay_seconds: Delay between retries in seconds
        exceptions: Exception types to catch and retry on

    Returns:
        Decorated function with retry logic
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(1, max_attempts + 1):
                try:
                    if attempt > 1:
                        logger.info(f"Retry attempt {attempt}/{max_attempts} for {func.__name__}")
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    logger.warning(
                        f"Attempt {attempt}/{max_attempts} failed for {func.__name__}: {e}"
                    )
                    if attempt < max_attempts:
                        time.sleep(delay_seconds)
            logger.error(f"All {max_attempts} attempts failed for {func.__name__}")
            raise last_exception
        return wrapper
    return decorator


def handle_pipeline_error(stage: str, error: Exception) -> None:
    """
    Standardised pipeline error handler.
    Logs error details and raises with context.

    Args:
        stage: Pipeline stage name where error occurred
        error: The exception that was raised
    """
    logger.error(f"Pipeline failure in stage: {stage.upper()}")
    logger.error(f"Error type: {type(error).__name__}")
    logger.error(f"Error details: {str(error)}")
    raise RuntimeError(f"Pipeline failed at stage [{stage}]: {str(error)}") from error
