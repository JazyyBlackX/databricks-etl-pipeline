"""
utils/config_loader.py
----------------------
Loads and validates pipeline configuration from JSON.
Supports environment-specific overrides.
"""

import json
import os
from typing import Any, Dict
from utils.logger import get_logger

logger = get_logger(__name__)


def load_config(config_path: str = "configs/pipeline_config.json") -> Dict[str, Any]:
    """
    Loads pipeline configuration from a JSON file.

    Args:
        config_path: Path to the config JSON file

    Returns:
        Dictionary containing pipeline configuration

    Raises:
        FileNotFoundError: If config file does not exist
        ValueError: If config file is invalid JSON
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Config file not found at: {config_path}. "
            "Please ensure configs/pipeline_config.json exists."
        )

    try:
        with open(config_path, "r") as f:
            config = json.load(f)
        logger.info(f"Configuration loaded successfully from: {config_path}")
        return config
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in config file: {e}")


def get_paths(config: Dict[str, Any]) -> Dict[str, str]:
    """Extracts storage paths from config."""
    return config.get("storage", {})


def get_spark_config(config: Dict[str, Any]) -> Dict[str, str]:
    """Extracts Spark configuration from config."""
    return config.get("spark", {})
