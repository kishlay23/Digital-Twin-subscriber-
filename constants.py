"""Sensor type constants and table mapping."""

from typing import Dict, Tuple

ALLOWED_SENSOR_TYPES = {"temperature", "humidity", "light"}

table_map: Dict[str, Tuple[str, str]] = {
    "temperature": ("temperature_sensor_data", "temperature_value"),
    "humidity": ("humidity_sensor_data", "humidity_value"),
    "light": ("light_sensor_data", "light_value"),
}

# Validate mapping at import time
_unknown = set(table_map.keys()) - ALLOWED_SENSOR_TYPES
if _unknown:
    raise RuntimeError(f"table_map contains unknown sensor types: {_unknown}")


def get_table_and_column(sensor_type: str) -> Tuple[str, str]:
    sensor_type = sensor_type.lower()
    return table_map[sensor_type]
