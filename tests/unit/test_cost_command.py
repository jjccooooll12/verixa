from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from verixa.cli.cost import CostReport, CostUsageRecord, run_cost
from verixa.connectors.base import ConnectorError


class _FakeSnapshotService:
    def __init__(self, connector) -> None:  # noqa: ANN001
        self.connector = connector
        self.mode_seen = None

    def estimate_bytes(self, config, *, mode="snapshot") -> dict[str, int]:  # noqa: ANN001
        self.mode_seen = mode
        return {"stripe.transactions": 2048}


class _FakeBigQueryConnector:
    def __init__(self, warehouse, *, max_bytes_billed=None, query_tag=None) -> None:  # noqa: ANN001
        self.warehouse = warehouse
        self.max_bytes_billed = max_bytes_billed
        self.query_tag = query_tag


class _FakeSnowflakeConnector:
    def __init__(self, warehouse, *, max_bytes_billed=None, query_tag=None) -> None:  # noqa: ANN001
        self.warehouse = warehouse
        self.max_bytes_billed = max_bytes_billed
        self.query_tag = query_tag
        self.calls: list[tuple[str, int]] = []

    def report_query_usage(self, *, query_tag: str, lookback_seconds: int, result_limit: int = 100):
        self.calls.append((query_tag, lookback_seconds))
        return (
            CostUsageRecord(
                query_id="01a",
                query_tag=query_tag,
                warehouse_name="VERIXA_WH",
                start_time=datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc),
                total_elapsed_ms=321,
                bytes_scanned=1024,
                bytes_written=128,
                rows_produced=10,
            ),
        )


def test_run_cost_estimates_bigquery_bytes(tmp_path: Path) -> None:
    config_path = tmp_path / "verixa.yaml"
    config_path.write_text(
        """
warehouse:
  kind: bigquery
  project: demo
sources:
  stripe.transactions:
    table: raw.stripe_transactions
    schema:
      amount: float
""".strip()
        + "\n",
        encoding="utf-8",
    )

    created_services: list[_FakeSnapshotService] = []
    created_connectors: list[_FakeBigQueryConnector] = []

    def _connector_factory(warehouse, *, max_bytes_billed=None, query_tag=None):  # noqa: ANN001
        connector = _FakeBigQueryConnector(
            warehouse,
            max_bytes_billed=max_bytes_billed,
            query_tag=query_tag,
        )
        created_connectors.append(connector)
        return connector

    def _service_factory(connector):  # noqa: ANN001
        service = _FakeSnapshotService(connector)
        created_services.append(service)
        return service

    report = run_cost(
        config_path,
        command="diff",
        max_bytes_billed=1024,
        connector_factory=_connector_factory,
        snapshot_service_factory=_service_factory,
    )

    assert report == CostReport(
        command="diff",
        mode="estimate",
        estimates={"stripe.transactions": 2048},
        max_bytes_billed=1024,
        query_tag="verixa:diff",
    )
    assert created_services[0].mode_seen == "plan"
    assert created_connectors[0].query_tag == "verixa:diff"


def test_run_cost_reports_recent_snowflake_usage(tmp_path: Path) -> None:
    config_path = tmp_path / "verixa.yaml"
    config_path.write_text(
        """
warehouse:
  kind: snowflake
  connection_name: verixa
sources:
  stripe.transactions:
    table: RAW.INGEST.ORDERS
    schema:
      amount: float
""".strip()
        + "\n",
        encoding="utf-8",
    )

    created_connectors: list[_FakeSnowflakeConnector] = []

    def _connector_factory(warehouse, *, max_bytes_billed=None, query_tag=None):  # noqa: ANN001
        connector = _FakeSnowflakeConnector(
            warehouse,
            max_bytes_billed=max_bytes_billed,
            query_tag=query_tag,
        )
        created_connectors.append(connector)
        return connector

    report = run_cost(
        config_path,
        command="check",
        history_window_seconds=1800,
        connector_factory=_connector_factory,
    )

    assert report.mode == "history"
    assert report.command == "check"
    assert report.query_tag == "verixa:check"
    assert report.history_window_seconds == 1800
    assert report.total_bytes == 1024
    assert report.total_bytes_written == 128
    assert report.total_elapsed_ms == 321
    assert created_connectors[0].query_tag == "verixa:check"
    assert created_connectors[0].calls == [("verixa:check", 1800)]


def test_run_cost_rejects_estimate_mode_for_snowflake(tmp_path: Path) -> None:
    config_path = tmp_path / "verixa.yaml"
    config_path.write_text(
        """
warehouse:
  kind: snowflake
  connection_name: verixa
sources:
  stripe.transactions:
    table: RAW.INGEST.ORDERS
    schema:
      amount: float
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ConnectorError, match="supported only for BigQuery"):
        run_cost(
            config_path,
            command="validate",
            mode="estimate",
            connector_factory=lambda warehouse, **kwargs: _FakeSnowflakeConnector(warehouse, **kwargs),
        )
