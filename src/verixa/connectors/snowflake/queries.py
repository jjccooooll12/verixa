"""SQL generation for Snowflake snapshot queries."""

from __future__ import annotations

from typing import Sequence

from verixa.connectors.snowflake.types import SnowflakeTableRef, alias_name, quote_column
from verixa.contracts.models import AcceptedValuesTest


def build_stats_query(
    table_ref: SnowflakeTableRef,
    null_rate_columns: tuple[str, ...],
    freshness_column: str | None,
    accepted_values_tests: Sequence[AcceptedValuesTest],
    numeric_summary_columns: tuple[str, ...],
    *,
    include_exact_row_count: bool,
    scan_timestamp_column: str | None = None,
    scan_timestamp_type: str | None = None,
    scan_lookback_seconds: int | None = None,
) -> tuple[str, tuple[object, ...]]:
    """Build one aggregate Snowflake query for current source stats."""

    select_lines: list[str] = []
    parameters: list[object] = []

    if include_exact_row_count:
        select_lines.append("COUNT(*) AS EXACT_ROW_COUNT")

    for column in null_rate_columns:
        quoted = quote_column(column)
        alias = alias_name("null_rate", column)
        select_lines.append(
            f"COUNT_IF({quoted} IS NULL) / NULLIF(COUNT(*), 0) AS {alias}"
        )

    if freshness_column is not None:
        select_lines.append(
            f"MAX({quote_column(freshness_column)}) AS FRESHNESS_LATEST"
        )

    for test in accepted_values_tests:
        quoted = quote_column(test.column)
        placeholders = ", ".join(["%s"] * len(test.values))
        invalid_condition = (
            f"{quoted} IS NOT NULL AND CAST({quoted} AS VARCHAR) NOT IN ({placeholders})"
        )
        invalid_value = f"IFF({invalid_condition}, CAST({quoted} AS VARCHAR), NULL)"
        parameters.extend(test.values)
        select_lines.append(
            f"COUNT_IF({invalid_condition}) AS {alias_name('invalid_count', test.column)}"
        )
        # ``invalid_condition`` appears again inside ``invalid_value`` for both ARRAY_AGG
        # and WITHIN GROUP ORDER BY, so positional parameters must be repeated.
        parameters.extend(test.values)
        parameters.extend(test.values)
        select_lines.append(
            "ARRAY_SLICE("
            "ARRAY_AGG(DISTINCT "
            f"{invalid_value}"
            f") WITHIN GROUP (ORDER BY {invalid_value}), 0, 10"
            f") AS {alias_name('invalid_examples', test.column)}"
        )

    for column in numeric_summary_columns:
        quoted = quote_column(column)
        cast_value = f"CAST({quoted} AS DOUBLE)"
        select_lines.append(f"MIN({cast_value}) AS {alias_name('numeric_min', column)}")
        select_lines.append(f"AVG({cast_value}) AS {alias_name('numeric_mean', column)}")
        select_lines.append(f"MAX({cast_value}) AS {alias_name('numeric_max', column)}")
        select_lines.append(
            f"APPROX_PERCENTILE({cast_value}, 0.5) AS {alias_name('numeric_p50', column)}"
        )
        select_lines.append(
            f"APPROX_PERCENTILE({cast_value}, 0.95) AS {alias_name('numeric_p95', column)}"
        )

    query = "SELECT\n  " + ",\n  ".join(select_lines) + f"\nFROM {table_ref.quoted_name}"
    if scan_timestamp_column is not None and scan_lookback_seconds is not None:
        query += "\nWHERE " + _build_scan_predicate(
            scan_timestamp_column,
            scan_timestamp_type or "TIMESTAMP",
            scan_lookback_seconds,
        )
    return query, tuple(parameters)


def build_columns_query(table_ref: SnowflakeTableRef) -> tuple[str, tuple[object, ...]]:
    """Build an INFORMATION_SCHEMA query for one table's columns."""

    query = (
        "SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE\n"
        f"FROM {table_ref.information_schema_name}.COLUMNS\n"
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s\n"
        "ORDER BY ORDINAL_POSITION"
    )
    return query, (table_ref.schema, table_ref.table)


def build_table_metadata_query(table_ref: SnowflakeTableRef) -> tuple[str, tuple[object, ...]]:
    """Build an INFORMATION_SCHEMA query for table-level metadata."""

    query = (
        "SELECT ROW_COUNT\n"
        f"FROM {table_ref.information_schema_name}.TABLES\n"
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s"
    )
    return query, (table_ref.schema, table_ref.table)


def _build_scan_predicate(
    column_name: str,
    column_type: str,
    lookback_seconds: int,
) -> str:
    quoted_column = quote_column(column_name)
    if column_type == "DATE":
        return (
            f"{quoted_column} >= TO_DATE(DATEADD(SECOND, -{lookback_seconds}, CURRENT_TIMESTAMP()))"
        )
    return f"{quoted_column} >= DATEADD(SECOND, -{lookback_seconds}, CURRENT_TIMESTAMP())"
