"""Shared coercion helpers for configuration and runtime limits."""

from __future__ import annotations

from typing import Any, Iterable


def coerce_cli_value(raw_value: str) -> Any:
    """Coerce CLI scalar values using existing project semantics."""
    if raw_value.isnumeric():
        return int(raw_value)

    lowered = raw_value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    return raw_value


def coerce_config_file_value(key: str, value: Any, int_keys: Iterable[str]) -> Any:
    """Coerce JSON config values while preserving current behavior."""
    if key in int_keys:
        return int(value)
    return value.lower() if isinstance(value, str) else value


def clamp_min_int(value: Any, minimum: int = 1) -> tuple[int, bool]:
    """Return clamped integer value and whether clamping occurred."""
    parsed = int(value)
    clamped = max(minimum, parsed)
    return clamped, clamped != parsed


def min_int(value: Any, minimum: int = 1) -> int:
    """Clamp a value to an integer minimum."""
    return clamp_min_int(value, minimum=minimum)[0]


def parse_int_field(value: Any, field_name: str) -> int:
    """Parse an integer field with a consistent validation error."""
    try:
        return int(value)
    except (TypeError, ValueError) as err:
        raise ValueError(f"'{field_name}' must be an integer") from err
