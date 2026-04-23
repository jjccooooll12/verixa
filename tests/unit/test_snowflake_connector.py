from __future__ import annotations

import sys
import types
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from verixa.connectors.base import ConnectorError, SourceCaptureRequest
from verixa.connectors.snowflake.connector import SnowflakeConnector
from verixa.contracts.models import (
    AcceptedValuesTest,
    FreshnessConfig,
    NoNullsTest,
    SourceContract,
    WarehouseConfig,
)


class _FakeCursor:
    def __init__(self, connection) -> None:  # noqa: ANN001
        self._connection = connection
        self._rows: list[dict[str, object]] = []

    def execute(self, query: str, params: tuple[object, ...] = ()) -> "_FakeCursor":
        self._connection.executed.append((query, params))
        if not self._connection.responses:
            self._rows = []
            return self
        response = self._connection.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        self._rows = list(response)
        return self

    def fetchall(self) -> list[dict[str, object]]:
        return list(self._rows)

    def close(self) -> None:
        return None


class _FakeConnection:
    def __init__(self, responses: list[object] | None = None) -> None:
        self.responses = list(responses or [])
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    def cursor(self, cursor_class=None):  # noqa: ANN001
        return _FakeCursor(self)


@pytest.fixture
def source_contract() -> SourceContract:
    return SourceContract(
        name="stripe.transactions",
        table="raw.ingest.orders",
        schema={
            "amount": "FLOAT64",
            "currency": "STRING",
            "created_at": "TIMESTAMP",
        },
        freshness=FreshnessConfig(column="created_at", max_age="1h", max_age_seconds=3600),
        tests=(
            NoNullsTest(column="amount"),
            AcceptedValuesTest(column="currency", values=("USD", "EUR")),
        ),
    )


@pytest.fixture
def fake_snowflake_modules(monkeypatch) -> None:  # noqa: ANN001
    snowflake_module = types.ModuleType("snowflake")
    connector_module = types.ModuleType("snowflake.connector")
    connector_module.DictCursor = object()
    connector_module.connect = lambda **kwargs: _FakeConnection()  # noqa: ARG005
    snowflake_module.connector = connector_module
    monkeypatch.setitem(sys.modules, "snowflake", snowflake_module)
    monkeypatch.setitem(sys.modules, "snowflake.connector", connector_module)


def test_snowflake_connector_requires_library() -> None:
    connector = SnowflakeConnector(
        WarehouseConfig(kind="snowflake", account="demo", user="analyst")
    )
    real_import = __import__

    def _missing_snowflake(name, globals=None, locals=None, fromlist=(), level=0):  # noqa: ANN001
        if name == "snowflake.connector":
            raise ImportError("snowflake missing")
        return real_import(name, globals, locals, fromlist, level)

    with patch("builtins.__import__", side_effect=_missing_snowflake):
        with pytest.raises(ConnectorError, match="snowflake-connector-python"):
            _ = connector._client


def test_capture_source_maps_metadata_and_stats(
    fake_snowflake_modules,  # noqa: ARG001
    source_contract: SourceContract,
) -> None:
    latest_value = datetime.now(timezone.utc) - timedelta(minutes=30)
    connector = SnowflakeConnector(
        WarehouseConfig(kind="snowflake", account="demo", user="analyst", database="raw")
    )
    connector.__dict__["_client"] = _FakeConnection(
        responses=[
            [
                {
                    "COLUMN_NAME": "AMOUNT",
                    "DATA_TYPE": "FLOAT",
                    "NUMERIC_PRECISION": None,
                    "NUMERIC_SCALE": None,
                },
                {
                    "COLUMN_NAME": "CURRENCY",
                    "DATA_TYPE": "TEXT",
                    "NUMERIC_PRECISION": None,
                    "NUMERIC_SCALE": None,
                },
                {
                    "COLUMN_NAME": "CREATED_AT",
                    "DATA_TYPE": "TIMESTAMP_NTZ",
                    "NUMERIC_PRECISION": None,
                    "NUMERIC_SCALE": None,
                },
            ],
            [{"ROW_COUNT": 125}],
            [
                {
                    "EXACT_ROW_COUNT": 126,
                    "NULL_RATE__AMOUNT": 0.08,
                    "NULL_RATE__CURRENCY": 0.0,
                    "NULL_RATE__CREATED_AT": 0.0,
                    "FRESHNESS_LATEST": latest_value,
                    "INVALID_COUNT__CURRENCY": 2,
                    "INVALID_EXAMPLES__CURRENCY": ["AUD", "CAD"],
                    "NUMERIC_MIN__AMOUNT": 1.0,
                    "NUMERIC_P50__AMOUNT": 10.0,
                    "NUMERIC_P95__AMOUNT": 22.0,
                    "NUMERIC_MAX__AMOUNT": 25.0,
                    "NUMERIC_MEAN__AMOUNT": 12.5,
                }
            ],
        ]
    )

    snapshot = connector.capture_source(
        source_contract,
        SourceCaptureRequest.for_snapshot(source_contract),
    )

    assert snapshot.schema == {
        "amount": "FLOAT64",
        "created_at": "TIMESTAMP",
        "currency": "STRING",
    }
    assert snapshot.row_count == 126
    assert snapshot.null_rates == {"amount": 0.08, "created_at": 0.0, "currency": 0.0}
    assert snapshot.freshness is not None
    assert snapshot.freshness.latest_value == latest_value
    assert 1790 <= snapshot.freshness.age_seconds <= 1810
    assert snapshot.accepted_values["currency"].invalid_count == 2
    assert snapshot.accepted_values["currency"].invalid_examples == ("AUD", "CAD")
    assert snapshot.numeric_summaries["amount"].p50_value == 10.0
    assert snapshot.numeric_summaries["amount"].p95_value == 22.0


def test_capture_source_normalizes_number_scale_zero_to_int64(
    fake_snowflake_modules,  # noqa: ARG001
) -> None:
    connector = SnowflakeConnector(
        WarehouseConfig(kind="snowflake", account="demo", user="analyst", database="raw")
    )
    connector.__dict__["_client"] = _FakeConnection(
        responses=[
            [
                {
                    "COLUMN_NAME": "ORDER_ID",
                    "DATA_TYPE": "NUMBER",
                    "NUMERIC_PRECISION": 18,
                    "NUMERIC_SCALE": 0,
                }
            ],
            [{"ROW_COUNT": 5}],
        ]
    )
    source_contract = SourceContract(
        name="orders",
        table="raw.ingest.orders",
        schema={"order_id": "INT64"},
        freshness=None,
        tests=(),
    )

    snapshot = connector.capture_source(
        source_contract,
        SourceCaptureRequest.for_test(source_contract),
    )

    assert snapshot.schema == {"order_id": "INT64"}


def test_capture_source_parses_json_array_example_strings(
    fake_snowflake_modules,  # noqa: ARG001
    source_contract: SourceContract,
) -> None:
    connector = SnowflakeConnector(
        WarehouseConfig(kind="snowflake", account="demo", user="analyst", database="raw")
    )
    connector.__dict__["_client"] = _FakeConnection(
        responses=[
            [
                {
                    "COLUMN_NAME": "AMOUNT",
                    "DATA_TYPE": "FLOAT",
                    "NUMERIC_PRECISION": None,
                    "NUMERIC_SCALE": None,
                },
                {
                    "COLUMN_NAME": "CURRENCY",
                    "DATA_TYPE": "TEXT",
                    "NUMERIC_PRECISION": None,
                    "NUMERIC_SCALE": None,
                },
                {
                    "COLUMN_NAME": "CREATED_AT",
                    "DATA_TYPE": "TIMESTAMP_NTZ",
                    "NUMERIC_PRECISION": None,
                    "NUMERIC_SCALE": None,
                },
            ],
            [{"ROW_COUNT": 3}],
            [
                {
                    "NULL_RATE__AMOUNT": 0.0,
                    "NULL_RATE__CREATED_AT": 0.0,
                    "INVALID_COUNT__CURRENCY": 2,
                    "INVALID_EXAMPLES__CURRENCY": '[\n  "AUD",\n  "CAD"\n]',
                }
            ],
        ]
    )

    snapshot = connector.capture_source(
        source_contract,
        SourceCaptureRequest.for_test(source_contract),
    )

    assert snapshot.accepted_values["currency"].invalid_examples == ("AUD", "CAD")


def test_check_auth_uses_current_account(
    fake_snowflake_modules,  # noqa: ARG001
) -> None:
    connector = SnowflakeConnector(
        WarehouseConfig(kind="snowflake", account="demo", user="analyst")
    )
    connector.__dict__["_client"] = _FakeConnection(
        responses=[[{"ACCOUNT_NAME": "DEMO-ACCOUNT"}]]
    )

    ok, message = connector.check_auth()

    assert ok is True
    assert message == "authenticated for account 'DEMO-ACCOUNT'"


def test_estimate_source_bytes_is_unsupported(
    fake_snowflake_modules,  # noqa: ARG001
    source_contract: SourceContract,
) -> None:
    connector = SnowflakeConnector(
        WarehouseConfig(kind="snowflake", account="demo", user="analyst")
    )

    with pytest.raises(ConnectorError, match="supported only for BigQuery"):
        connector.estimate_source_bytes(
            source_contract,
            SourceCaptureRequest.for_test(source_contract),
        )


def test_client_uses_password_env_when_configured(
    fake_snowflake_modules,  # noqa: ARG001
    monkeypatch,  # noqa: ANN001
) -> None:
    captured_kwargs: dict[str, object] = {}

    def _connect(**kwargs):  # noqa: ANN001
        captured_kwargs.update(kwargs)
        return _FakeConnection()

    sys.modules["snowflake.connector"].connect = _connect
    monkeypatch.setenv("VERIXA_SNOWFLAKE_PASSWORD", "secret")

    connector = SnowflakeConnector(
        WarehouseConfig(
            kind="snowflake",
            account="demo",
            user="analyst",
            password_env="VERIXA_SNOWFLAKE_PASSWORD",
            warehouse_name="ANALYTICS",
            database="RAW",
            schema="INGEST",
        )
    )

    _ = connector._client

    assert captured_kwargs["account"] == "demo"
    assert captured_kwargs["user"] == "analyst"
    assert captured_kwargs["password"] == "secret"
    assert captured_kwargs["warehouse"] == "ANALYTICS"
    assert captured_kwargs["database"] == "RAW"
    assert captured_kwargs["schema"] == "INGEST"
    assert captured_kwargs["session_parameters"] == {
        "TIMEZONE": "UTC",
        "QUERY_TAG": "verixa",
    }


def test_client_uses_explicit_query_tag(
    fake_snowflake_modules,  # noqa: ARG001
) -> None:
    captured_kwargs: dict[str, object] = {}

    def _connect(**kwargs):  # noqa: ANN001
        captured_kwargs.update(kwargs)
        return _FakeConnection()

    sys.modules["snowflake.connector"].connect = _connect

    connector = SnowflakeConnector(
        WarehouseConfig(kind="snowflake", connection_name="verixa"),
        query_tag="verixa:diff",
    )

    _ = connector._client

    assert captured_kwargs["session_parameters"] == {
        "TIMEZONE": "UTC",
        "QUERY_TAG": "verixa:diff",
    }


def test_describe_runtime_environment_returns_context(
    fake_snowflake_modules,  # noqa: ARG001
) -> None:
    connector = SnowflakeConnector(
        WarehouseConfig(kind="snowflake", connection_name="verixa")
    )
    connector.__dict__["_client"] = _FakeConnection(
        responses=[
            [
                {
                    "CURRENT_ROLE": "ACCOUNTADMIN",
                    "CURRENT_WAREHOUSE": "VERIXA_WH",
                    "CURRENT_DATABASE": "VERIXA_DB",
                    "CURRENT_SCHEMA": "RAW",
                }
            ],
            [{"COMPUTE_OK": 1}],
        ]
    )

    context = connector.describe_runtime_environment()

    assert context.current_role == "ACCOUNTADMIN"
    assert context.current_warehouse == "VERIXA_WH"
    assert context.current_database == "VERIXA_DB"
    assert context.current_schema == "RAW"
    assert context.compute_ok is True
    assert context.compute_message is None


def test_report_query_usage_maps_history_rows(
    fake_snowflake_modules,  # noqa: ARG001
) -> None:
    started_at = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)
    connector = SnowflakeConnector(
        WarehouseConfig(kind="snowflake", connection_name="verixa")
    )
    fake_connection = _FakeConnection(
        responses=[
            [
                {
                    "QUERY_ID": "01a",
                    "QUERY_TAG": "verixa:diff",
                    "WAREHOUSE_NAME": "VERIXA_WH",
                    "START_TIME": started_at,
                    "TOTAL_ELAPSED_TIME": 123,
                    "BYTES_SCANNED": 2048,
                    "BYTES_WRITTEN_TO_RESULT": 512,
                    "ROWS_PRODUCED": 7,
                }
            ]
        ]
    )
    connector.__dict__["_client"] = fake_connection

    usage = connector.report_query_usage(query_tag="verixa:diff", lookback_seconds=1800)

    assert usage[0].query_id == "01a"
    assert usage[0].query_tag == "verixa:diff"
    assert usage[0].warehouse_name == "VERIXA_WH"
    assert usage[0].start_time == started_at
    assert usage[0].total_elapsed_ms == 123
    assert usage[0].bytes_scanned == 2048
    assert usage[0].bytes_written == 512
    assert usage[0].rows_produced == 7
    assert fake_connection.executed[0][1] == ("verixa:diff",)
