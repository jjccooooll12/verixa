from __future__ import annotations

from verixa.connectors.bigquery.queries import build_stats_query
from verixa.connectors.bigquery.types import BigQueryTableRef
from verixa.contracts.models import AcceptedValuesTest


def test_build_stats_query_includes_all_requested_checks() -> None:
    query, parameters = build_stats_query(
        table_ref=BigQueryTableRef(project="demo", dataset="raw", table="stripe_transactions"),
        null_rate_columns=("amount", "currency"),
        freshness_column="created_at",
        accepted_values_tests=(
            AcceptedValuesTest(column="currency", values=("USD", "EUR")),
        ),
        numeric_summary_columns=("amount",),
        include_exact_row_count=True,
        scan_timestamp_column="created_at",
        scan_lookback_seconds=86400,
    )

    assert "COUNT(*) AS exact_row_count" in query
    assert "SAFE_DIVIDE(COUNTIF(`amount` IS NULL), COUNT(*)) AS null_rate__amount" in query
    assert "SAFE_DIVIDE(COUNTIF(`currency` IS NULL), COUNT(*)) AS null_rate__currency" in query
    assert "MAX(`created_at`) AS freshness_latest" in query
    assert "invalid_count__currency" in query
    assert "invalid_examples__currency" in query
    assert "MIN(SAFE_CAST(`amount` AS FLOAT64)) AS numeric_min__amount" in query
    assert "AVG(SAFE_CAST(`amount` AS FLOAT64)) AS numeric_mean__amount" in query
    assert "APPROX_QUANTILES(SAFE_CAST(`amount` AS FLOAT64), 100 IGNORE NULLS)" in query
    assert "ORDER BY IF(" in query
    assert "WHERE `created_at` >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 86400 SECOND)" in query
    assert "FROM `demo.raw.stripe_transactions`" in query
    assert parameters == (
        type(parameters[0])(name="accepted_values__currency", values=("USD", "EUR")),
    )


def test_build_stats_query_can_skip_exact_row_count() -> None:
    query, _ = build_stats_query(
        table_ref=BigQueryTableRef(project="demo", dataset="raw", table="stripe_transactions"),
        null_rate_columns=(),
        freshness_column="created_at",
        accepted_values_tests=(),
        numeric_summary_columns=("amount",),
        include_exact_row_count=False,
    )

    assert "exact_row_count" not in query
    assert "MAX(`created_at`) AS freshness_latest" in query
    assert "numeric_min__amount" in query


def test_build_stats_query_supports_datetime_scan_windows() -> None:
    query, _ = build_stats_query(
        table_ref=BigQueryTableRef(project="demo", dataset="raw", table="stripe_transactions"),
        null_rate_columns=("amount",),
        freshness_column=None,
        accepted_values_tests=(),
        numeric_summary_columns=(),
        include_exact_row_count=True,
        scan_timestamp_column="created_at",
        scan_timestamp_type="DATETIME",
        scan_lookback_seconds=3600,
    )

    assert "WHERE `created_at` >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 3600 SECOND)" in query


def test_build_stats_query_supports_date_scan_windows() -> None:
    query, _ = build_stats_query(
        table_ref=BigQueryTableRef(project="demo", dataset="raw", table="stripe_transactions"),
        null_rate_columns=("amount",),
        freshness_column=None,
        accepted_values_tests=(),
        numeric_summary_columns=(),
        include_exact_row_count=True,
        scan_timestamp_column="created_date",
        scan_timestamp_type="DATE",
        scan_lookback_seconds=86400,
    )

    assert (
        "WHERE `created_date` >= DATE(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 86400 SECOND))"
        in query
    )
