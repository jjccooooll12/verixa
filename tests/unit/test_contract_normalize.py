from __future__ import annotations

import pytest

from dataguard.contracts.normalize import (
    NormalizationError,
    normalize_schema_mapping,
    normalize_type_name,
    parse_duration_to_seconds,
)


def test_normalize_type_name_maps_common_aliases() -> None:
    assert normalize_type_name("string") == "STRING"
    assert normalize_type_name("float") == "FLOAT64"
    assert normalize_type_name("BOOL") == "BOOL"


def test_parse_duration_to_seconds_supports_common_units() -> None:
    assert parse_duration_to_seconds("30m") == 1800
    assert parse_duration_to_seconds("<1h") == 3600
    assert parse_duration_to_seconds("2d") == 172800


def test_normalize_schema_mapping_rejects_invalid_list_items() -> None:
    with pytest.raises(NormalizationError):
        normalize_schema_mapping([{"amount": "float", "currency": "string"}])


def test_normalize_schema_mapping_rejects_nested_identifiers() -> None:
    with pytest.raises(NormalizationError):
        normalize_schema_mapping({"payload.currency": "string"})
