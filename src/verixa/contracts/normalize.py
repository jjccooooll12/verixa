"""Normalization helpers for contracts and warehouse types."""

from __future__ import annotations

import re
from typing import Any


_TYPE_ALIASES = {
    "string": "STRING",
    "str": "STRING",
    "bytes": "BYTES",
    "int": "INT64",
    "integer": "INT64",
    "int64": "INT64",
    "float": "FLOAT64",
    "float64": "FLOAT64",
    "numeric": "NUMERIC",
    "bignumeric": "BIGNUMERIC",
    "bool": "BOOL",
    "boolean": "BOOL",
    "timestamp": "TIMESTAMP",
    "datetime": "DATETIME",
    "date": "DATE",
    "time": "TIME",
    "json": "JSON",
}

_DURATION_RE = re.compile(r"^<?\s*(\d+)\s*([smhd])\s*$", re.IGNORECASE)
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class NormalizationError(ValueError):
    """Raised when user-supplied configuration cannot be normalized."""


def normalize_type_name(type_name: str) -> str:
    """Normalize a warehouse type name into a canonical representation."""

    normalized = type_name.strip().lower()
    return _TYPE_ALIASES.get(normalized, type_name.strip().upper())


def parse_duration_to_seconds(raw_value: str) -> int:
    """Parse durations like ``1h`` or ``<30m`` into seconds."""

    match = _DURATION_RE.match(raw_value)
    if match is None:
        raise NormalizationError(
            f"Unsupported duration '{raw_value}'. Use values like '30m', '1h', or '<1d'."
        )
    amount = int(match.group(1))
    unit = match.group(2).lower()
    multiplier = {
        "s": 1,
        "m": 60,
        "h": 60 * 60,
        "d": 60 * 60 * 24,
    }[unit]
    return amount * multiplier


def normalize_schema_mapping(raw_schema: Any) -> dict[str, str]:
    """Support either a mapping or a list of one-item mappings."""

    if isinstance(raw_schema, dict):
        items = raw_schema.items()
    elif isinstance(raw_schema, list):
        items = []
        for index, item in enumerate(raw_schema):
            if not isinstance(item, dict) or len(item) != 1:
                raise NormalizationError(
                    "Schema list items must be one-item mappings like {'column': 'type'}; "
                    f"received invalid item at index {index}."
                )
            items.extend(item.items())
    else:
        raise NormalizationError("Schema must be a mapping or a list of one-item mappings.")

    schema: dict[str, str] = {}
    for column_name, column_type in items:
        if not isinstance(column_name, str) or not column_name:
            raise NormalizationError("Schema column names must be non-empty strings.")
        validate_identifier(column_name, "column name")
        if not isinstance(column_type, str) or not column_type:
            raise NormalizationError(
                f"Schema type for column '{column_name}' must be a non-empty string."
            )
        schema[column_name] = normalize_type_name(column_type)
    return dict(sorted(schema.items()))


def validate_identifier(name: str, kind: str) -> None:
    """Validate simple identifiers used in the MVP's SQL generation."""

    if _IDENTIFIER_RE.match(name) is None:
        raise NormalizationError(
            f"Unsupported {kind} '{name}'. Verixa v1 supports simple identifiers only."
        )
