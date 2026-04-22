"""SQL generation for BigQuery snapshot queries."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

from verixa.contracts.models import AcceptedValuesTest
from verixa.contracts.normalize import validate_identifier
from verixa.connectors.bigquery.types import BigQueryTableRef


@dataclass(frozen=True, slots=True)
class ArrayParameter:
    """Simple representation of an array query parameter."""

    name: str
    values: tuple[str, ...]


def build_stats_query(
    table_ref: BigQueryTableRef,
    null_rate_columns: Iterable[str],
    freshness_column: str | None,
    accepted_values_tests: Sequence[AcceptedValuesTest],
    *,
    include_exact_row_count: bool,
    scan_timestamp_column: str | None = None,
    scan_timestamp_type: str | None = None,
    scan_lookback_seconds: int | None = None,
) -> tuple[str, tuple[ArrayParameter, ...]]:
    """Build one aggregate query for all non-metadata snapshot stats."""

    select_lines: list[str] = []
    parameters: list[ArrayParameter] = []

    if include_exact_row_count:
        select_lines.append("COUNT(*) AS exact_row_count")

    for column in sorted(set(null_rate_columns)):
        quoted = _quote_identifier(column)
        alias = _safe_alias("null_rate", column)
        select_lines.append(
            f"SAFE_DIVIDE(COUNTIF({quoted} IS NULL), COUNT(*)) AS {alias}"
        )

    if freshness_column is not None:
        select_lines.append(
            f"MAX({_quote_identifier(freshness_column)}) AS freshness_latest"
        )

    for test in sorted(accepted_values_tests, key=lambda item: item.column):
        quoted = _quote_identifier(test.column)
        parameter_name = _safe_alias("accepted_values", test.column)
        parameters.append(ArrayParameter(name=parameter_name, values=test.values))
        invalid_condition = (
            f"{quoted} IS NOT NULL AND CAST({quoted} AS STRING) NOT IN UNNEST(@{parameter_name})"
        )
        invalid_value = f"IF({invalid_condition}, CAST({quoted} AS STRING), NULL)"
        select_lines.append(
            "COUNTIF("
            f"{invalid_condition}"
            f") AS {_safe_alias('invalid_count', test.column)}"
        )
        select_lines.append(
            "ARRAY_AGG(DISTINCT "
            f"{invalid_value} IGNORE NULLS ORDER BY {invalid_value} LIMIT 10) AS "
            f"{_safe_alias('invalid_examples', test.column)}"
        )

    query = "SELECT\n  " + ",\n  ".join(select_lines) + f"\nFROM `{table_ref.full_name}`"
    if scan_timestamp_column is not None and scan_lookback_seconds is not None:
        quoted_column = _quote_identifier(scan_timestamp_column)
        query += "\nWHERE " + _build_scan_predicate(
            quoted_column,
            scan_timestamp_type or "TIMESTAMP",
            scan_lookback_seconds,
        )
    return query, tuple(parameters)


def _quote_identifier(identifier: str) -> str:
    validate_identifier(identifier, "column name")
    return f"`{identifier}`"


def _safe_alias(prefix: str, identifier: str) -> str:
    validate_identifier(identifier, "column name")
    return f"{prefix}__{identifier}"


def _build_scan_predicate(
    quoted_column: str,
    column_type: str,
    lookback_seconds: int,
) -> str:
    if column_type == "TIMESTAMP":
        return (
            f"{quoted_column} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), "
            f"INTERVAL {lookback_seconds} SECOND)"
        )
    if column_type == "DATETIME":
        return (
            f"{quoted_column} >= DATETIME_SUB(CURRENT_DATETIME(), "
            f"INTERVAL {lookback_seconds} SECOND)"
        )
    if column_type == "DATE":
        return (
            f"{quoted_column} >= DATE(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), "
            f"INTERVAL {lookback_seconds} SECOND))"
        )
    raise ValueError(f"Unsupported scan column type '{column_type}'.")
