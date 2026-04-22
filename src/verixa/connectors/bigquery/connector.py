"""BigQuery connector implementation."""

from __future__ import annotations

from datetime import datetime, timezone
from functools import cached_property

from verixa.connectors.base import (
    ConnectorError,
    SourceCaptureRequest,
    WarehouseConnector,
)
from verixa.connectors.bigquery.queries import build_stats_query
from verixa.connectors.bigquery.types import parse_table_ref
from verixa.contracts.models import SourceContract, WarehouseConfig
from verixa.contracts.normalize import normalize_type_name
from verixa.snapshot.models import (
    AcceptedValuesSnapshot,
    FreshnessSnapshot,
    SourceSnapshot,
)


class BigQueryConnector(WarehouseConnector):
    """Collect snapshot data from BigQuery using the official client library."""

    def __init__(
        self,
        warehouse: WarehouseConfig,
        *,
        max_bytes_billed: int | None = None,
    ) -> None:
        self._warehouse = warehouse
        self._max_bytes_billed = max_bytes_billed

    @cached_property
    def _client(self):
        try:
            from google.cloud import bigquery
        except ImportError as exc:
            raise ConnectorError(
                "google-cloud-bigquery is not installed. Install Verixa dependencies first."
            ) from exc

        return bigquery.Client(
            project=self._warehouse.project,
            location=self._warehouse.location,
        )

    def capture_source(
        self,
        source: SourceContract,
        capture_request: SourceCaptureRequest,
    ) -> SourceSnapshot:
        table_ref = parse_table_ref(source.table, default_project=self._warehouse.project)
        captured_at = datetime.now(timezone.utc)

        try:
            table = self._client.get_table(table_ref.full_name)
        except Exception as exc:  # pragma: no cover - exercised through mocks in tests
            raise ConnectorError(
                f"Failed to fetch BigQuery metadata for '{table_ref.full_name}': {exc}"
            ) from exc

        schema = {
            field.name: normalize_type_name(field.field_type)
            for field in sorted(table.schema, key=lambda item: item.name)
        }
        row_count = int(table.num_rows) if table.num_rows is not None else None

        null_rates = {column: None for column in capture_request.null_rate_columns}
        freshness = None
        accepted_values: dict[str, AcceptedValuesSnapshot] = {}

        if capture_request.needs_stats_query:
            row_count, null_rates, freshness, accepted_values = self._run_stats_query(
                source=source,
                capture_request=capture_request,
                table_ref=table_ref.full_name,
                current_row_count=row_count,
                captured_at=captured_at,
            )

        return SourceSnapshot(
            source_name=source.name,
            table=table_ref.full_name,
            schema=schema,
            row_count=row_count,
            null_rates=null_rates,
            freshness=freshness,
            accepted_values=accepted_values,
            captured_at=captured_at,
        )

    def _run_stats_query(
        self,
        source: SourceContract,
        capture_request: SourceCaptureRequest,
        table_ref: str,
        current_row_count: int | None,
        captured_at: datetime,
    ) -> tuple[
        int | None,
        dict[str, float | None],
        FreshnessSnapshot | None,
        dict[str, AcceptedValuesSnapshot],
    ]:
        try:
            from google.cloud import bigquery
        except ImportError as exc:  # pragma: no cover - guarded in _client too
            raise ConnectorError(
                "google-cloud-bigquery is not installed. Install Verixa dependencies first."
            ) from exc

        query, parameters = build_stats_query(
            table_ref=parse_table_ref(table_ref),
            null_rate_columns=capture_request.null_rate_columns,
            freshness_column=capture_request.freshness_column,
            accepted_values_tests=capture_request.accepted_values_tests,
            include_exact_row_count=capture_request.include_exact_row_count,
            scan_timestamp_column=capture_request.scan_timestamp_column,
            scan_timestamp_type=capture_request.scan_timestamp_type,
            scan_lookback_seconds=capture_request.scan_lookback_seconds,
        )

        query_parameters = [
            bigquery.ArrayQueryParameter(parameter.name, "STRING", list(parameter.values))
            for parameter in parameters
        ]
        job_config = bigquery.QueryJobConfig(
            query_parameters=query_parameters,
            maximum_bytes_billed=self._effective_max_bytes_billed,
        )

        try:
            query_job = self._client.query(query, job_config=job_config)
            row = next(iter(query_job.result()))
        except Exception as exc:  # pragma: no cover - exercised through mocks in tests
            suffix = ""
            if self._effective_max_bytes_billed is not None:
                suffix = (
                    " "
                    f"(max_bytes_billed={_format_bytes(self._effective_max_bytes_billed)})"
                )
            raise ConnectorError(
                f"Failed to query BigQuery stats for '{table_ref}'{suffix}: {exc}"
            ) from exc

        row_count = row.get("exact_row_count", current_row_count)
        if row_count is not None:
            row_count = int(row_count)

        null_rates = {
            column: _maybe_float(row.get(f"null_rate__{column}"))
            for column in capture_request.null_rate_columns
        }

        freshness = None
        if capture_request.freshness_column is not None and source.freshness is not None:
            latest_value = row.get("freshness_latest")
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
                invalid_count=int(row.get(f"invalid_count__{test.column}", 0) or 0),
                invalid_examples=tuple(row.get(f"invalid_examples__{test.column}") or ()),
            )

        return row_count, null_rates, freshness, accepted_values

    def estimate_source_bytes(
        self,
        source: SourceContract,
        capture_request: SourceCaptureRequest,
    ) -> int:
        if not capture_request.needs_stats_query:
            return 0

        try:
            from google.cloud import bigquery
        except ImportError as exc:  # pragma: no cover - guarded in _client too
            raise ConnectorError(
                "google-cloud-bigquery is not installed. Install Verixa dependencies first."
            ) from exc

        table_ref = parse_table_ref(source.table, default_project=self._warehouse.project)
        query, parameters = build_stats_query(
            table_ref=table_ref,
            null_rate_columns=capture_request.null_rate_columns,
            freshness_column=capture_request.freshness_column,
            accepted_values_tests=capture_request.accepted_values_tests,
            include_exact_row_count=capture_request.include_exact_row_count,
            scan_timestamp_column=capture_request.scan_timestamp_column,
            scan_timestamp_type=capture_request.scan_timestamp_type,
            scan_lookback_seconds=capture_request.scan_lookback_seconds,
        )
        query_parameters = [
            bigquery.ArrayQueryParameter(parameter.name, "STRING", list(parameter.values))
            for parameter in parameters
        ]
        job_config = bigquery.QueryJobConfig(
            query_parameters=query_parameters,
            dry_run=True,
            use_query_cache=False,
        )

        try:
            query_job = self._client.query(query, job_config=job_config)
        except Exception as exc:  # pragma: no cover - exercised through mocks in tests
            raise ConnectorError(
                f"Failed to estimate BigQuery query bytes for '{table_ref.full_name}': {exc}"
            ) from exc

        return int(query_job.total_bytes_processed or 0)

    def check_auth(self) -> tuple[bool, str]:
        try:
            from google.cloud import bigquery
        except ImportError as exc:  # pragma: no cover - guarded in _client too
            raise ConnectorError(
                "google-cloud-bigquery is not installed. Install Verixa dependencies first."
            ) from exc

        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        try:
            self._client.query("SELECT 1", job_config=job_config)
        except Exception as exc:  # pragma: no cover - exercised through mocks in tests
            return False, str(exc)
        return True, f"authenticated for project '{self._client.project}'"

    def check_source_access(self, source: SourceContract) -> tuple[bool, str]:
        table_ref = parse_table_ref(source.table, default_project=self._warehouse.project)
        try:
            self._client.get_table(table_ref.full_name)
        except Exception as exc:  # pragma: no cover - exercised through mocks in tests
            return False, str(exc)
        return True, table_ref.full_name

    @property
    def _effective_max_bytes_billed(self) -> int | None:
        if self._max_bytes_billed is not None:
            return self._max_bytes_billed
        return self._warehouse.max_bytes_billed


def _maybe_float(value: object) -> float | None:
    if value is None:
        return None
    return float(value)


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


def _format_bytes(value: int) -> str:
    units = ("B", "KB", "MB", "GB", "TB")
    size = float(value)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{int(value)} B"
