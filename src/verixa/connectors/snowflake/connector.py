"""Snowflake connector implementation."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import cached_property
from typing import Any

from verixa.connectors.base import ConnectorError, SourceCaptureRequest, WarehouseConnector
from verixa.connectors.snowflake.queries import (
    build_columns_query,
    build_stats_query,
    build_table_metadata_query,
)
from verixa.connectors.snowflake.types import SnowflakeTableRef, alias_name, parse_table_ref
from verixa.contracts.models import SourceContract, WarehouseConfig
from verixa.contracts.normalize import normalize_type_name
from verixa.snapshot.models import (
    AcceptedValuesSnapshot,
    FreshnessSnapshot,
    NumericSummarySnapshot,
    SourceSnapshot,
)


@dataclass(frozen=True, slots=True)
class SnowflakeSessionContext:
    """Resolved runtime session context for diagnostics."""

    current_role: str | None
    current_warehouse: str | None
    current_database: str | None
    current_schema: str | None
    compute_ok: bool
    compute_message: str | None


@dataclass(frozen=True, slots=True)
class SnowflakeQueryUsage:
    """One historical Snowflake query usage record."""

    query_id: str
    query_tag: str | None
    warehouse_name: str | None
    start_time: datetime | None
    total_elapsed_ms: int | None
    bytes_scanned: int | None
    bytes_written: int | None
    rows_produced: int | None


class SnowflakeConnector(WarehouseConnector):
    """Collect snapshot data from Snowflake using the official Python connector."""

    def __init__(self, warehouse: WarehouseConfig, *, query_tag: str | None = None) -> None:
        self._warehouse = warehouse
        self._query_tag = query_tag or "verixa"

    @cached_property
    def _client(self):
        try:
            import snowflake.connector
        except ImportError as exc:  # pragma: no cover - guarded in tests
            raise ConnectorError(
                "snowflake-connector-python is not installed. Install Verixa dependencies first."
            ) from exc

        connect_kwargs: dict[str, Any] = {
            "application": "verixa",
            # Keep timestamp handling deterministic for TIMESTAMP_NTZ values.
            "session_parameters": {
                "TIMEZONE": "UTC",
                "QUERY_TAG": self._query_tag,
            },
        }
        if self._warehouse.connection_name is not None:
            connect_kwargs["connection_name"] = self._warehouse.connection_name
        else:
            connect_kwargs["account"] = self._warehouse.account
            connect_kwargs["user"] = self._warehouse.user

        if self._warehouse.password_env is not None:
            password = os.getenv(self._warehouse.password_env)
            if not password:
                raise ConnectorError(
                    f"Snowflake password env var '{self._warehouse.password_env}' is not set."
                )
            connect_kwargs["password"] = password

        if self._warehouse.authenticator is not None:
            connect_kwargs["authenticator"] = self._warehouse.authenticator
        if self._warehouse.warehouse_name is not None:
            connect_kwargs["warehouse"] = self._warehouse.warehouse_name
        if self._warehouse.database is not None:
            connect_kwargs["database"] = self._warehouse.database
        if self._warehouse.schema is not None:
            connect_kwargs["schema"] = self._warehouse.schema
        if self._warehouse.role is not None:
            connect_kwargs["role"] = self._warehouse.role

        return snowflake.connector.connect(**connect_kwargs)

    def capture_source(
        self,
        source: SourceContract,
        capture_request: SourceCaptureRequest,
    ) -> SourceSnapshot:
        table_ref = self._parse_table_ref(source.table)
        captured_at = datetime.now(timezone.utc)
        schema = self._fetch_schema(table_ref)
        row_count = self._fetch_row_count(table_ref)

        null_rates = {column: None for column in capture_request.null_rate_columns}
        freshness = None
        accepted_values: dict[str, AcceptedValuesSnapshot] = {}
        numeric_summaries: dict[str, NumericSummarySnapshot] = {}

        if capture_request.needs_stats_query:
            row_count, null_rates, freshness, accepted_values, numeric_summaries = (
                self._run_stats_query(
                    source=source,
                    capture_request=capture_request,
                    table_ref=table_ref,
                    current_row_count=row_count,
                    captured_at=captured_at,
                )
            )

        return SourceSnapshot(
            source_name=source.name,
            table=table_ref.full_name,
            schema=schema,
            row_count=row_count,
            null_rates=null_rates,
            freshness=freshness,
            accepted_values=accepted_values,
            numeric_summaries=numeric_summaries,
            captured_at=captured_at,
        )

    def estimate_source_bytes(
        self,
        source: SourceContract,
        capture_request: SourceCaptureRequest,
    ) -> int:
        raise ConnectorError("--estimate-bytes is currently supported only for BigQuery.")

    def check_auth(self) -> tuple[bool, str]:
        try:
            row = self._fetch_one(
                "SELECT CURRENT_ACCOUNT() AS ACCOUNT_NAME",
                (),
            )
        except Exception as exc:  # pragma: no cover - exercised through mocks in tests
            return False, str(exc)
        account_name = row.get("ACCOUNT_NAME") if row else None
        return True, f"authenticated for account '{account_name}'"

    def check_source_access(self, source: SourceContract) -> tuple[bool, str]:
        table_ref = self._parse_table_ref(source.table)
        query, params = build_table_metadata_query(table_ref)
        try:
            row = self._fetch_one(query, params)
        except Exception as exc:  # pragma: no cover - exercised through mocks in tests
            return False, str(exc)
        if row is None:
            return False, f"{table_ref.full_name} was not found"
        return True, table_ref.full_name

    def describe_runtime_environment(self) -> SnowflakeSessionContext:
        row = self._fetch_one(
            "SELECT "
            "CURRENT_ROLE() AS CURRENT_ROLE, "
            "CURRENT_WAREHOUSE() AS CURRENT_WAREHOUSE, "
            "CURRENT_DATABASE() AS CURRENT_DATABASE, "
            "CURRENT_SCHEMA() AS CURRENT_SCHEMA",
            (),
        )
        compute_ok = True
        compute_message: str | None = None
        try:
            self._fetch_one("SELECT 1 AS COMPUTE_OK", ())
        except Exception as exc:  # pragma: no cover - exercised in tests through mocks
            compute_ok = False
            compute_message = str(exc)
        return SnowflakeSessionContext(
            current_role=_maybe_str(row.get("CURRENT_ROLE")) if row else None,
            current_warehouse=_maybe_str(row.get("CURRENT_WAREHOUSE")) if row else None,
            current_database=_maybe_str(row.get("CURRENT_DATABASE")) if row else None,
            current_schema=_maybe_str(row.get("CURRENT_SCHEMA")) if row else None,
            compute_ok=compute_ok,
            compute_message=compute_message,
        )

    def report_query_usage(
        self,
        *,
        query_tag: str,
        lookback_seconds: int,
        result_limit: int = 100,
    ) -> tuple[SnowflakeQueryUsage, ...]:
        query = (
            "SELECT QUERY_ID, QUERY_TAG, WAREHOUSE_NAME, START_TIME, TOTAL_ELAPSED_TIME, "
            "BYTES_SCANNED, BYTES_WRITTEN_TO_RESULT, ROWS_PRODUCED\n"
            "FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(\n"
            f"  END_TIME_RANGE_START=>DATEADD(SECOND, -{lookback_seconds}, CURRENT_TIMESTAMP()),\n"
            "  END_TIME_RANGE_END=>CURRENT_TIMESTAMP(),\n"
            f"  RESULT_LIMIT=>{result_limit},\n"
            "  INCLUDE_CLIENT_GENERATED_STATEMENT=>TRUE))\n"
            "WHERE QUERY_TAG = %s AND USER_NAME = CURRENT_USER()\n"
            "ORDER BY START_TIME DESC"
        )
        rows = self._fetch_all(query, (query_tag,))
        return tuple(
            SnowflakeQueryUsage(
                query_id=str(row["QUERY_ID"]),
                query_tag=_maybe_str(row.get("QUERY_TAG")),
                warehouse_name=_maybe_str(row.get("WAREHOUSE_NAME")),
                start_time=_normalize_datetime(row.get("START_TIME")),
                total_elapsed_ms=_maybe_int(row.get("TOTAL_ELAPSED_TIME")),
                bytes_scanned=_maybe_int(row.get("BYTES_SCANNED")),
                bytes_written=_maybe_int(row.get("BYTES_WRITTEN_TO_RESULT")),
                rows_produced=_maybe_int(row.get("ROWS_PRODUCED")),
            )
            for row in rows
        )

    def _fetch_schema(self, table_ref: SnowflakeTableRef) -> dict[str, str]:
        query, params = build_columns_query(table_ref)
        try:
            rows = self._fetch_all(query, params)
        except Exception as exc:  # pragma: no cover - exercised through mocks in tests
            raise ConnectorError(
                f"Failed to fetch Snowflake schema for '{table_ref.full_name}': {exc}"
            ) from exc

        if not rows:
            raise ConnectorError(
                f"Failed to fetch Snowflake schema for '{table_ref.full_name}': no columns returned."
            )

        schema: dict[str, str] = {}
        for row in rows:
            column_name = str(row["COLUMN_NAME"]).lower()
            schema[column_name] = _normalize_snowflake_type(
                str(row["DATA_TYPE"]),
                row.get("NUMERIC_PRECISION"),
                row.get("NUMERIC_SCALE"),
            )
        return dict(sorted(schema.items()))

    def _fetch_row_count(self, table_ref: SnowflakeTableRef) -> int | None:
        query, params = build_table_metadata_query(table_ref)
        try:
            row = self._fetch_one(query, params)
        except Exception as exc:  # pragma: no cover - exercised through mocks in tests
            raise ConnectorError(
                f"Failed to fetch Snowflake metadata for '{table_ref.full_name}': {exc}"
            ) from exc
        if row is None:
            raise ConnectorError(
                f"Failed to fetch Snowflake metadata for '{table_ref.full_name}': table was not found."
            )
        row_count = row.get("ROW_COUNT")
        return int(row_count) if row_count is not None else None

    def _run_stats_query(
        self,
        source: SourceContract,
        capture_request: SourceCaptureRequest,
        table_ref: SnowflakeTableRef,
        current_row_count: int | None,
        captured_at: datetime,
    ) -> tuple[
        int | None,
        dict[str, float | None],
        FreshnessSnapshot | None,
        dict[str, AcceptedValuesSnapshot],
        dict[str, NumericSummarySnapshot],
    ]:
        query, params = build_stats_query(
            table_ref,
            capture_request.null_rate_columns,
            capture_request.freshness_column,
            capture_request.accepted_values_tests,
            capture_request.numeric_summary_columns,
            include_exact_row_count=capture_request.include_exact_row_count,
            scan_timestamp_column=capture_request.scan_timestamp_column,
            scan_timestamp_type=capture_request.scan_timestamp_type,
            scan_lookback_seconds=capture_request.scan_lookback_seconds,
        )
        try:
            row = self._fetch_one(query, params)
        except Exception as exc:  # pragma: no cover - exercised through mocks in tests
            raise ConnectorError(
                f"Failed to query Snowflake stats for '{table_ref.full_name}': {exc}"
            ) from exc

        if row is None:
            raise ConnectorError(
                f"Failed to query Snowflake stats for '{table_ref.full_name}': query returned no rows."
            )

        row_count = row.get("EXACT_ROW_COUNT", current_row_count)
        if row_count is not None:
            row_count = int(row_count)

        null_rates = {
            column: _maybe_float(row.get(alias_name("null_rate", column)))
            for column in capture_request.null_rate_columns
        }

        freshness = None
        if capture_request.freshness_column is not None and source.freshness is not None:
            latest_value = row.get("FRESHNESS_LATEST")
            freshness = FreshnessSnapshot(
                column=source.freshness.column,
                max_age_seconds=source.freshness.max_age_seconds,
                latest_value=_normalize_datetime(latest_value),
                age_seconds=_age_in_seconds(latest_value, captured_at),
            )

        accepted_values: dict[str, AcceptedValuesSnapshot] = {}
        for test in capture_request.accepted_values_tests:
            accepted_values[test.column] = AcceptedValuesSnapshot(
                column=test.column,
                invalid_count=int(row.get(alias_name("invalid_count", test.column), 0) or 0),
                invalid_examples=_normalize_invalid_examples(
                    row.get(alias_name("invalid_examples", test.column))
                ),
            )

        numeric_summaries: dict[str, NumericSummarySnapshot] = {}
        for column in capture_request.numeric_summary_columns:
            numeric_summaries[column] = NumericSummarySnapshot(
                column=column,
                min_value=_maybe_float(row.get(alias_name("numeric_min", column))),
                p50_value=_maybe_float(row.get(alias_name("numeric_p50", column))),
                p95_value=_maybe_float(row.get(alias_name("numeric_p95", column))),
                max_value=_maybe_float(row.get(alias_name("numeric_max", column))),
                mean_value=_maybe_float(row.get(alias_name("numeric_mean", column))),
            )

        return row_count, null_rates, freshness, accepted_values, numeric_summaries

    def _fetch_all(self, query: str, params: tuple[object, ...]) -> list[dict[str, object]]:
        try:
            import snowflake.connector
        except ImportError as exc:  # pragma: no cover - guarded in _client too
            raise ConnectorError(
                "snowflake-connector-python is not installed. Install Verixa dependencies first."
            ) from exc

        cursor = self._client.cursor(snowflake.connector.DictCursor)
        try:
            cursor.execute(query, params)
            rows = cursor.fetchall()
        finally:
            cursor.close()
        return list(rows)

    def _fetch_one(self, query: str, params: tuple[object, ...]) -> dict[str, object] | None:
        rows = self._fetch_all(query, params)
        if not rows:
            return None
        return rows[0]

    def _parse_table_ref(self, raw_table: str) -> SnowflakeTableRef:
        return parse_table_ref(
            raw_table,
            default_database=self._warehouse.database,
            default_schema=self._warehouse.schema,
        )


def _normalize_snowflake_type(
    raw_type: str,
    precision: object,
    scale: object,
) -> str:
    normalized = normalize_type_name(raw_type)
    if normalized == "NUMERIC":
        resolved_scale = int(scale) if scale is not None else None
        resolved_precision = int(precision) if precision is not None else None
        if resolved_scale == 0 and resolved_precision is not None and resolved_precision <= 18:
            return "INT64"
    return normalized


def _maybe_float(value: object) -> float | None:
    if value is None:
        return None
    return float(value)


def _maybe_int(value: object) -> int | None:
    if value is None:
        return None
    return int(value)


def _maybe_str(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _normalize_invalid_examples(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return ()
        if stripped.startswith("["):
            try:
                parsed = json.loads(stripped)
            except json.JSONDecodeError:
                return (value,)
            if isinstance(parsed, list):
                return tuple(str(item) for item in parsed if item is not None)
        return (value,)
    if isinstance(value, (list, tuple)):
        return tuple(str(item) for item in value if item is not None)
    return (str(value),)


def _normalize_datetime(value: object) -> datetime | None:
    if value is None or not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _age_in_seconds(value: object, captured_at: datetime) -> int | None:
    normalized = _normalize_datetime(value)
    if normalized is None:
        return None
    delta = captured_at - normalized
    return max(int(delta.total_seconds()), 0)
