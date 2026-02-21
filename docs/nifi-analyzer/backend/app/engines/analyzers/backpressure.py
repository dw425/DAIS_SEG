"""Backpressure translation: map NiFi connection queue configs to Databricks Auto Loader options.

Extracts NiFi backPressureObjectThreshold and backPressureDataSizeThreshold
from connections and translates them to Databricks Auto Loader equivalents:
- cloudFiles.maxFilesPerTrigger
- cloudFiles.maxBytesPerTrigger
"""

import logging
import re

from app.models.processor import Connection

logger = logging.getLogger(__name__)


def _parse_data_size(size_str: str) -> int | None:
    """Parse a NiFi data size string (e.g., '1 GB', '500 MB') to bytes.

    Returns bytes as int, or None if unparseable.
    """
    if not size_str:
        return None

    size_str = size_str.strip().upper()
    match = re.match(r"^(\d+(?:\.\d+)?)\s*(B|KB|MB|GB|TB)$", size_str)
    if not match:
        logger.debug("Cannot parse data size string: %r", size_str)
        return None

    value = float(match.group(1))
    unit = match.group(2)
    multipliers = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}
    return int(value * multipliers[unit])


def _format_bytes_for_spark(byte_count: int) -> str:
    """Format byte count as a human-readable Spark config value."""
    if byte_count >= 1024**3:
        return f"{byte_count // (1024**3)}g"
    if byte_count >= 1024**2:
        return f"{byte_count // (1024**2)}m"
    if byte_count >= 1024:
        return f"{byte_count // 1024}k"
    return f"{byte_count}b"


_NIFI_DEFAULT_OBJECT_THRESHOLD = 10000
_NIFI_DEFAULT_DATA_SIZE = "1 GB"


def _is_default_backpressure(obj_threshold: int, data_threshold: str) -> bool:
    """Check if backpressure values are just NiFi defaults (10000 objects / 1 GB).

    NiFi sets these defaults on every connection.  Reporting them as custom
    configuration floods the output with noise and produces misleading
    Databricks Auto Loader recommendations.
    """
    obj_is_default = obj_threshold in (0, _NIFI_DEFAULT_OBJECT_THRESHOLD)
    data_is_default = (
        not data_threshold
        or data_threshold.strip().upper() == _NIFI_DEFAULT_DATA_SIZE
    )
    return obj_is_default and data_is_default


def extract_backpressure_configs(connections: list[Connection]) -> list[dict]:
    """Extract and translate backpressure configurations from NiFi connections.

    Filters out NiFi default thresholds (10000 objects / 1 GB) to avoid
    flooding the output with non-custom configurations.

    Returns list of BackpressureConfig-compatible dicts with keys:
        connection_source, connection_destination,
        nifi_object_threshold, nifi_data_size_threshold,
        databricks_max_files_per_trigger, databricks_max_bytes_per_trigger
    """
    configs: list[dict] = []

    for conn in connections:
        obj_threshold = conn.back_pressure_object_threshold
        data_threshold = conn.back_pressure_data_size_threshold

        if not obj_threshold and not data_threshold:
            continue

        # Skip NiFi default backpressure values — they are not custom configuration
        if _is_default_backpressure(obj_threshold, data_threshold):
            continue

        config: dict = {
            "connection_source": conn.source_name,
            "connection_destination": conn.destination_name,
            "nifi_object_threshold": obj_threshold,
            "nifi_data_size_threshold": data_threshold,
            "databricks_max_files_per_trigger": None,
            "databricks_max_bytes_per_trigger": None,
        }

        # Translate object threshold -> maxFilesPerTrigger
        if obj_threshold and obj_threshold > 0:
            config["databricks_max_files_per_trigger"] = obj_threshold

        # Translate data size threshold -> maxBytesPerTrigger
        if data_threshold:
            byte_count = _parse_data_size(data_threshold)
            if byte_count is not None:
                config["databricks_max_bytes_per_trigger"] = _format_bytes_for_spark(byte_count)

        configs.append(config)

    logger.info("Extracted %d custom backpressure config(s) from %d connections", len(configs), len(connections))
    return configs
