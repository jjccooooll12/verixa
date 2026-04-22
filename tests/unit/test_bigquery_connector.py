from __future__ import annotations

import sys
import types
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from verixa.connectors.base import ConnectorError, SourceCaptureRequest
from verixa.connectors.bigquery.connector import BigQueryConnector
from verixa.contracts.models import (
    AcceptedValuesTest,
    FreshnessConfig,
    NoNullsTest,
    ScanConfig,
    SourceContract,
    WarehouseConfig,
)


class _FakeField:
    def __init__(self, name: str, field_type: str) -> None:
        self.name = name
        self.field_type = field_type


class _FakeTable:
    def __init__(self) -> None:
        self.schema = [_FakeField("currency", "STRING"), _FakeField("amount", "FLOAT")]
        self.num_rows = 100


class _FakeQueryJob:
    def __init__(self, row: dict[str, object], *, total_bytes_processed: int = 0) -> None:
        self._row = row
        self.total_bytes_processed = total_bytes_processed

    def result(self) -> list[dict[str, object]]:
        return [self._row]


class _FakeClient:
    def __init__(
        self,
        *,
        row: dict[str, object] | None = None,
        query_error: Exception | None = None,
        total_bytes_processed: int = 0,
    ) -> None:
        self.table = _FakeTable()
        self.row = row or {}
        self.query_error = query_error
        self.total_bytes_processed = total_bytes_processed
        self.last_query: str | None = None
        self.last_job_config = None

    def get_table(self, full_name: str) -> _FakeTable:
        assert full_name == "demo.raw.stripe_transactions"
        return self.table

    def query(self, query: str, job_config) -> _FakeQueryJob:  # noqa: ANN001
        self.last_query = query
        self.last_job_config = job_config
        if self.query_error is not None:
            raise self.query_error
        return _FakeQueryJob(self.row, total_bytes_processed=self.total_bytes_processed)


class _FakeArrayQueryParameter:
    def __init__(self, name: str, parameter_type: str, values: list[str]) -> None:
        self.name = name
        self.parameter_type = parameter_type
        self.values = values


_MISSING = object()


class _FakeQueryJobConfig:
    def __init__(
        self,
        *,
        query_parameters: list[object],
        dry_run: bool = False,
        use_query_cache: bool = True,
        maximum_bytes_billed: object = _MISSING,
    ) -> None:
        self.query_parameters = query_parameters
        self.dry_run = dry_run
        self.use_query_cache = use_query_cache
        self.maximum_bytes_billed_provided = maximum_bytes_billed is not _MISSING
        self.maximum_bytes_billed = (
            None if maximum_bytes_billed is _MISSING else maximum_bytes_billed
        )


@pytest.fixture
def source_contract() -> SourceContract:
    return SourceContract(
        name="stripe.transactions",
        table="demo.raw.stripe_transactions",
        schema={"amount": "FLOAT64", "currency": "STRING", "created_at": "TIMESTAMP"},
        freshness=FreshnessConfig(column="created_at", max_age="1h", max_age_seconds=3600),
        tests=(
            NoNullsTest(column="amount"),
            AcceptedValuesTest(column="currency", values=("USD", "EUR")),
        ),
    )


@pytest.fixture
def fake_bigquery_modules(monkeypatch) -> None:  # noqa: ANN001
    google_module = types.ModuleType("google")
    cloud_module = types.ModuleType("google.cloud")
    bigquery_module = types.ModuleType("google.cloud.bigquery")
    bigquery_module.ArrayQueryParameter = _FakeArrayQueryParameter
    bigquery_module.QueryJobConfig = _FakeQueryJobConfig
    cloud_module.bigquery = bigquery_module
    google_module.cloud = cloud_module

    monkeypatch.setitem(sys.modules, "google", google_module)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud_module)
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", bigquery_module)


def test_bigquery_connector_requires_google_client_library() -> None:
    connector = BigQueryConnector(WarehouseConfig(kind="bigquery", project="demo", location="US"))

    real_import = __import__

    def _missing_bigquery(name, globals=None, locals=None, fromlist=(), level=0):  # noqa: ANN001
        if name == "google.cloud":
            raise ImportError("google-cloud-bigquery missing")
        return real_import(name, globals, locals, fromlist, level)

    with patch("builtins.__import__", side_effect=_missing_bigquery):
        with pytest.raises(ConnectorError, match="google-cloud-bigquery"):
            _ = connector._client


def test_run_stats_query_maps_results_and_query_parameters(
    fake_bigquery_modules,  # noqa: ARG001
    source_contract: SourceContract,
) -> None:
    connector = BigQueryConnector(WarehouseConfig(kind="bigquery", project="demo", location="US"))
    fake_client = _FakeClient(
        row={
            "exact_row_count": 125,
            "null_rate__amount": 0.08,
            "null_rate__currency": 0.0,
            "null_rate__created_at": 0.0,
            "numeric_min__amount": 1.0,
            "numeric_mean__amount": 7.5,
            "numeric_max__amount": 22.0,
            "numeric_quantiles__amount": [float(index) for index in range(101)],
            "freshness_latest": datetime(2026, 4, 22, 11, 30, tzinfo=timezone.utc),
            "invalid_count__currency": 2,
            "invalid_examples__currency": ["AUD", "CAD"],
        }
    )
    connector.__dict__["_client"] = fake_client

    row_count, null_rates, freshness, accepted_values, numeric_summaries = connector._run_stats_query(
        source=source_contract,
        capture_request=SourceCaptureRequest.for_snapshot(source_contract),
        table_ref="demo.raw.stripe_transactions",
        current_row_count=100,
        captured_at=datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc),
    )

    assert row_count == 125
    assert null_rates == {"amount": 0.08, "created_at": 0.0, "currency": 0.0}
    assert freshness is not None
    assert freshness.latest_value == datetime(2026, 4, 22, 11, 30, tzinfo=timezone.utc)
    assert freshness.age_seconds == 1800
    assert accepted_values["currency"].invalid_count == 2
    assert accepted_values["currency"].invalid_examples == ("AUD", "CAD")
    assert numeric_summaries["amount"].min_value == 1.0
    assert numeric_summaries["amount"].mean_value == 7.5
    assert numeric_summaries["amount"].p50_value == 50.0
    assert numeric_summaries["amount"].p95_value == 95.0
    assert numeric_summaries["amount"].max_value == 22.0
    assert fake_client.last_query is not None
    assert "FROM `demo.raw.stripe_transactions`" in fake_client.last_query
    query_parameters = fake_client.last_job_config.query_parameters
    assert len(query_parameters) == 1
    assert query_parameters[0].name == "accepted_values__currency"
    assert query_parameters[0].parameter_type == "STRING"
    assert query_parameters[0].values == ["USD", "EUR"]


def test_capture_source_wraps_bigquery_metadata_errors(source_contract: SourceContract) -> None:
    connector = BigQueryConnector(WarehouseConfig(kind="bigquery", project="demo", location="US"))

    class _ExplodingClient:
        def get_table(self, full_name: str):  # noqa: ANN201
            raise RuntimeError(f"boom for {full_name}")

    connector.__dict__["_client"] = _ExplodingClient()

    with pytest.raises(ConnectorError, match="Failed to fetch BigQuery metadata"):
        connector.capture_source(source_contract, SourceCaptureRequest.for_snapshot(source_contract))


def test_run_stats_query_wraps_bigquery_query_errors(
    fake_bigquery_modules,  # noqa: ARG001
    source_contract: SourceContract,
) -> None:
    connector = BigQueryConnector(WarehouseConfig(kind="bigquery", project="demo", location="US"))
    connector.__dict__["_client"] = _FakeClient(query_error=RuntimeError("query failed"))

    with pytest.raises(ConnectorError, match="Failed to query BigQuery stats"):
        connector._run_stats_query(
            source=source_contract,
            capture_request=SourceCaptureRequest.for_snapshot(source_contract),
            table_ref="demo.raw.stripe_transactions",
            current_row_count=100,
            captured_at=datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc),
        )


def test_capture_source_uses_contract_only_stats_for_test_mode(
    fake_bigquery_modules,  # noqa: ARG001
    source_contract: SourceContract,
) -> None:
    connector = BigQueryConnector(WarehouseConfig(kind="bigquery", project="demo", location="US"))
    fake_client = _FakeClient(
        row={
            "null_rate__amount": 0.08,
            "freshness_latest": datetime(2026, 4, 22, 11, 30, tzinfo=timezone.utc),
            "invalid_count__currency": 2,
            "invalid_examples__currency": ["AUD", "CAD"],
        }
    )
    connector.__dict__["_client"] = fake_client

    snapshot = connector.capture_source(
        source_contract,
        SourceCaptureRequest.for_test(source_contract),
    )

    assert snapshot.row_count == 100
    assert snapshot.null_rates == {"amount": 0.08}
    assert "exact_row_count" not in (fake_client.last_query or "")
    assert "null_rate__created_at" not in (fake_client.last_query or "")
    assert "null_rate__currency" not in (fake_client.last_query or "")
    assert "numeric_min__amount" not in (fake_client.last_query or "")
    assert snapshot.numeric_summaries == {}


def test_capture_source_skips_stats_query_when_only_schema_is_needed(
    fake_bigquery_modules,  # noqa: ARG001
) -> None:
    connector = BigQueryConnector(WarehouseConfig(kind="bigquery", project="demo", location="US"))
    fake_client = _FakeClient()
    connector.__dict__["_client"] = fake_client
    source_contract = SourceContract(
        name="stripe.transactions",
        table="demo.raw.stripe_transactions",
        schema={"amount": "FLOAT64"},
        freshness=None,
        tests=(),
    )

    snapshot = connector.capture_source(
        source_contract,
        SourceCaptureRequest.for_test(source_contract),
    )

    assert snapshot.row_count == 100
    assert snapshot.null_rates == {}
    assert fake_client.last_query is None


def test_capture_source_applies_scan_window_to_bigquery_query(
    fake_bigquery_modules,  # noqa: ARG001
    source_contract: SourceContract,
) -> None:
    connector = BigQueryConnector(WarehouseConfig(kind="bigquery", project="demo", location="US"))
    fake_client = _FakeClient(
        row={
            "exact_row_count": 12,
            "null_rate__amount": 0.0,
            "null_rate__currency": 0.0,
            "null_rate__created_at": 0.0,
            "numeric_min__amount": 1.0,
            "numeric_mean__amount": 7.0,
            "numeric_max__amount": 20.0,
            "numeric_quantiles__amount": [float(index) for index in range(101)],
            "freshness_latest": datetime(2026, 4, 22, 11, 30, tzinfo=timezone.utc),
            "invalid_count__currency": 0,
            "invalid_examples__currency": [],
        }
    )
    connector.__dict__["_client"] = fake_client
    windowed_source = SourceContract(
        name=source_contract.name,
        table=source_contract.table,
        schema=source_contract.schema,
        freshness=source_contract.freshness,
        tests=source_contract.tests,
        scan=ScanConfig(
            timestamp_column="created_at",
            column_type="TIMESTAMP",
            lookback="7d",
            lookback_seconds=7 * 24 * 3600,
        ),
    )

    connector.capture_source(
        windowed_source,
        SourceCaptureRequest.for_plan(windowed_source),
    )

    assert "WHERE `created_at` >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 604800 SECOND)" in (
        fake_client.last_query or ""
    )
    assert "numeric_quantiles__amount" in (fake_client.last_query or "")


def test_capture_source_applies_max_bytes_billed_to_live_queries(
    fake_bigquery_modules,  # noqa: ARG001
    source_contract: SourceContract,
) -> None:
    connector = BigQueryConnector(
        WarehouseConfig(
            kind="bigquery",
            project="demo",
            location="US",
            max_bytes_billed=5 * 1024 * 1024,
        )
    )
    fake_client = _FakeClient(
        row={
            "null_rate__amount": 0.0,
            "freshness_latest": datetime(2026, 4, 22, 11, 30, tzinfo=timezone.utc),
            "invalid_count__currency": 0,
            "invalid_examples__currency": [],
        }
    )
    connector.__dict__["_client"] = fake_client

    connector.capture_source(
        source_contract,
        SourceCaptureRequest.for_test(source_contract),
    )

    assert fake_client.last_job_config.maximum_bytes_billed == 5 * 1024 * 1024


def test_capture_source_omits_maximum_bytes_billed_when_unset(
    fake_bigquery_modules,  # noqa: ARG001
    source_contract: SourceContract,
) -> None:
    connector = BigQueryConnector(WarehouseConfig(kind="bigquery", project="demo", location="US"))
    fake_client = _FakeClient(
        row={
            "null_rate__amount": 0.0,
            "freshness_latest": datetime(2026, 4, 22, 11, 30, tzinfo=timezone.utc),
            "invalid_count__currency": 0,
            "invalid_examples__currency": [],
        }
    )
    connector.__dict__["_client"] = fake_client

    connector.capture_source(
        source_contract,
        SourceCaptureRequest.for_test(source_contract),
    )

    assert fake_client.last_job_config.maximum_bytes_billed is None
    assert fake_client.last_job_config.maximum_bytes_billed_provided is False


def test_estimate_source_bytes_uses_bigquery_dry_run(
    fake_bigquery_modules,  # noqa: ARG001
    source_contract: SourceContract,
) -> None:
    connector = BigQueryConnector(WarehouseConfig(kind="bigquery", project="demo", location="US"))
    fake_client = _FakeClient(total_bytes_processed=123456)
    connector.__dict__["_client"] = fake_client

    estimated = connector.estimate_source_bytes(
        source_contract,
        SourceCaptureRequest.for_test(source_contract),
    )

    assert estimated == 123456
    assert fake_client.last_job_config.dry_run is True
    assert fake_client.last_job_config.use_query_cache is False


def test_estimate_source_bytes_returns_zero_when_no_stats_query_needed(
    fake_bigquery_modules,  # noqa: ARG001
) -> None:
    connector = BigQueryConnector(WarehouseConfig(kind="bigquery", project="demo", location="US"))
    connector.__dict__["_client"] = _FakeClient(total_bytes_processed=999)
    source_contract = SourceContract(
        name="stripe.transactions",
        table="demo.raw.stripe_transactions",
        schema={"amount": "FLOAT64"},
        freshness=None,
        tests=(),
    )

    estimated = connector.estimate_source_bytes(
        source_contract,
        SourceCaptureRequest.for_test(source_contract),
    )

    assert estimated == 0
