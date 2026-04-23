from __future__ import annotations

from verixa.connectors.snowflake.queries import (
    build_columns_query,
    build_stats_query,
    build_table_metadata_query,
)
from verixa.connectors.snowflake.types import SnowflakeTableRef
from verixa.contracts.models import AcceptedValuesTest


def test_build_stats_query_includes_expected_snowflake_aggregates() -> None:
    query, params = build_stats_query(
        SnowflakeTableRef(database="RAW", schema="INGEST", table="ORDERS"),
        null_rate_columns=("amount", "currency"),
        freshness_column="created_at",
        accepted_values_tests=(
            AcceptedValuesTest(column="currency", values=("USD", "EUR")),
        ),
        numeric_summary_columns=("amount",),
        include_exact_row_count=True,
        scan_timestamp_column="created_at",
        scan_timestamp_type="TIMESTAMP",
        scan_lookback_seconds=3600,
    )

    assert 'FROM "RAW"."INGEST"."ORDERS"' in query
    assert 'COUNT(*) AS EXACT_ROW_COUNT' in query
    assert 'COUNT_IF("AMOUNT" IS NULL) / NULLIF(COUNT(*), 0) AS NULL_RATE__AMOUNT' in query
    assert 'MAX("CREATED_AT") AS FRESHNESS_LATEST' in query
    assert 'APPROX_PERCENTILE(CAST("AMOUNT" AS DOUBLE), 0.5) AS NUMERIC_P50__AMOUNT' in query
    assert '"CURRENCY" IS NOT NULL AND CAST("CURRENCY" AS VARCHAR) NOT IN (%s, %s)' in query
    assert 'AS INVALID_EXAMPLES__CURRENCY' in query
    assert 'ARRAY_SLICE(ARRAY_AGG(DISTINCT IFF(' in query
    assert 'WHERE "CREATED_AT" >= DATEADD(SECOND, -3600, CURRENT_TIMESTAMP())' in query
    assert params == ("USD", "EUR", "USD", "EUR", "USD", "EUR")


def test_build_columns_query_targets_information_schema() -> None:
    query, params = build_columns_query(
        SnowflakeTableRef(database="RAW", schema="INGEST", table="ORDERS")
    )

    assert 'FROM "RAW".INFORMATION_SCHEMA.COLUMNS' in query
    assert params == ("INGEST", "ORDERS")


def test_build_table_metadata_query_targets_information_schema() -> None:
    query, params = build_table_metadata_query(
        SnowflakeTableRef(database="RAW", schema="INGEST", table="ORDERS")
    )

    assert 'FROM "RAW".INFORMATION_SCHEMA.TABLES' in query
    assert params == ("INGEST", "ORDERS")


def test_build_stats_query_supports_datetime_scan_windows() -> None:
    query, _ = build_stats_query(
        SnowflakeTableRef(database="RAW", schema="INGEST", table="ORDERS"),
        null_rate_columns=(),
        freshness_column=None,
        accepted_values_tests=(),
        numeric_summary_columns=(),
        include_exact_row_count=True,
        scan_timestamp_column="created_at",
        scan_timestamp_type="DATETIME",
        scan_lookback_seconds=7200,
    )

    assert 'WHERE "CREATED_AT" >= DATEADD(SECOND, -7200, CURRENT_TIMESTAMP())' in query


def test_build_stats_query_supports_date_scan_windows() -> None:
    query, _ = build_stats_query(
        SnowflakeTableRef(database="RAW", schema="INGEST", table="ORDERS"),
        null_rate_columns=(),
        freshness_column=None,
        accepted_values_tests=(),
        numeric_summary_columns=(),
        include_exact_row_count=True,
        scan_timestamp_column="order_date",
        scan_timestamp_type="DATE",
        scan_lookback_seconds=172800,
    )

    assert (
        'WHERE "ORDER_DATE" >= TO_DATE(DATEADD(SECOND, -172800, CURRENT_TIMESTAMP()))'
        in query
    )
