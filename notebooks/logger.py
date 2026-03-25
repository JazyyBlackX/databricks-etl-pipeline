"""
utils/logger.py
---------------
Centralised logging utility for the Databricks ETL pipeline.
Provides structured, consistent log output across all pipeline stages.
"""

import logging
import sys
from datetime import datetime


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Returns a configured logger instance.

    Args:
        name: Logger name — typically the module name (__name__)
        level: Logging level (default: INFO)

    Returns:
        Configured Logger instance
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(level)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def log_pipeline_start(logger: logging.Logger, stage: str) -> None:
    """Logs a standardised pipeline stage start message."""
    logger.info("=" * 60)
    logger.info(f"STARTING STAGE: {stage.upper()}")
    logger.info(f"Timestamp: {datetime.utcnow().isoformat()}Z")
    logger.info("=" * 60)


def log_pipeline_end(logger: logging.Logger, stage: str, record_count: int = None) -> None:
    """Logs a standardised pipeline stage completion message."""
    logger.info("-" * 60)
    logger.info(f"COMPLETED STAGE: {stage.upper()}")
    if record_count is not None:
        logger.info(f"Records processed: {record_count:,}")
    logger.info(f"Timestamp: {datetime.utcnow().isoformat()}Z")
    logger.info("-" * 60)
