from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from verixa.cli.plan import run_plan
from verixa.output.console import render_diff_result
from verixa.snapshot.models import ProjectSnapshot, SourceSnapshot
from verixa.storage.filesystem import SnapshotStore


class _FakeConnector:
    def __init__(self, warehouse, *, max_bytes_billed=None, query_tag=None) -> None:  # noqa: ANN001
        self.warehouse = warehouse
        self.max_bytes_billed = max_bytes_billed
        self.query_tag = query_tag


class _FakeSnapshotService:
    def __init__(self, connector) -> None:  # noqa: ANN001
        self.connector = connector
        self.mode_seen = None
        self.execution_mode_seen = None

    def capture(self, config, *, mode="snapshot") -> ProjectSnapshot:  # noqa: ANN001
        self.mode_seen = mode
        return ProjectSnapshot(
            warehouse_kind="bigquery",
            generated_at=datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc),
            sources={
                "stripe.transactions": SourceSnapshot(
                    source_name="stripe.transactions",
                    table="demo.raw.stripe_transactions",
                    schema={"amount": "FLOAT64"},
                    row_count=40,
                    null_rates={"amount": 0.08},
                    freshness=None,
                    accepted_values={},
                    captured_at=datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc),
                )
            },
        )

    def capture_with_execution_mode(  # noqa: ANN001
        self,
        config,
        *,
        mode="snapshot",
        execution_mode="bounded",
    ) -> ProjectSnapshot:
        self.execution_mode_seen = execution_mode
        return self.capture(config, mode=mode)


def test_run_plan_reports_findings_from_baseline(tmp_path: Path) -> None:
    created_services: list[_FakeSnapshotService] = []

    def _service_factory(connector):  # noqa: ANN001
        service = _FakeSnapshotService(connector)
        created_services.append(service)
        return service

    config_path = tmp_path / "verixa.yaml"
    config_path.write_text(
        """
warehouse:
  kind: bigquery
  project: demo
baseline:
  path: {baseline_path}
sources:
  stripe.transactions:
    table: raw.stripe_transactions
    schema:
      amount: float
      currency: string
""".format(
            baseline_path=(tmp_path / ".verixa" / "{environment}" / "baseline.json").as_posix()
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    store = SnapshotStore(tmp_path / ".verixa" / "prod")
    store.write_baseline(
        ProjectSnapshot(
            warehouse_kind="bigquery",
            generated_at=datetime(2026, 4, 22, 11, 0, tzinfo=timezone.utc),
            sources={
                "stripe.transactions": SourceSnapshot(
                    source_name="stripe.transactions",
                    table="demo.raw.stripe_transactions",
                    schema={"amount": "FLOAT64", "currency": "STRING"},
                    row_count=100,
                    null_rates={"amount": 0.0, "currency": 0.0},
                    freshness=None,
                    accepted_values={},
                    captured_at=datetime(2026, 4, 22, 11, 0, tzinfo=timezone.utc),
                )
            },
        )
    )

    result = run_plan(
        config_path,
        environment="prod",
        connector_factory=_FakeConnector,
        snapshot_service_factory=_service_factory,
    )
    output = render_diff_result(result, title="Plan")

    assert result.error_count >= 1
    assert "column removed: currency" in output
    assert "row count changed: 100 -> 40" in output
    assert "Contract Violation" in output
    assert "Baseline Drift" in output
    assert created_services[0].mode_seen == "plan"
    assert created_services[0].execution_mode_seen == "bounded"


def test_run_plan_passes_diff_query_tag_to_connector(tmp_path: Path) -> None:
    created_connectors: list[_FakeConnector] = []

    def _connector_factory(warehouse, *, max_bytes_billed=None, query_tag=None):  # noqa: ANN001
        connector = _FakeConnector(
            warehouse,
            max_bytes_billed=max_bytes_billed,
            query_tag=query_tag,
        )
        created_connectors.append(connector)
        return connector

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
    store = SnapshotStore(tmp_path / ".verixa")
    store.write_baseline(
        ProjectSnapshot(
            warehouse_kind="bigquery",
            generated_at=datetime(2026, 4, 22, 11, 0, tzinfo=timezone.utc),
            sources={
                "stripe.transactions": SourceSnapshot(
                    source_name="stripe.transactions",
                    table="demo.raw.stripe_transactions",
                    schema={"amount": "FLOAT64"},
                    row_count=1,
                    null_rates={"amount": 0.0},
                    freshness=None,
                    accepted_values={},
                    captured_at=datetime(2026, 4, 22, 11, 0, tzinfo=timezone.utc),
                )
            },
        )
    )

    run_plan(
        config_path,
        connector_factory=_connector_factory,
        snapshot_service_factory=_FakeSnapshotService,
        snapshot_store_factory=lambda: store,
    )

    assert created_connectors[0].query_tag == "verixa:diff"


def test_run_plan_passes_execution_mode_to_service(tmp_path: Path) -> None:
    created_services: list[_FakeSnapshotService] = []

    def _service_factory(connector):  # noqa: ANN001
        service = _FakeSnapshotService(connector)
        created_services.append(service)
        return service

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
    store = SnapshotStore(tmp_path / ".verixa")
    store.write_baseline(
        ProjectSnapshot(
            warehouse_kind="bigquery",
            generated_at=datetime(2026, 4, 22, 11, 0, tzinfo=timezone.utc),
            sources={
                "stripe.transactions": SourceSnapshot(
                    source_name="stripe.transactions",
                    table="demo.raw.stripe_transactions",
                    schema={"amount": "FLOAT64"},
                    row_count=1,
                    null_rates={"amount": 0.0},
                    freshness=None,
                    accepted_values={},
                    captured_at=datetime(2026, 4, 22, 11, 0, tzinfo=timezone.utc),
                )
            },
        )
    )

    run_plan(
        config_path,
        execution_mode="full",
        connector_factory=_FakeConnector,
        snapshot_service_factory=_service_factory,
        snapshot_store_factory=lambda: store,
    )

    assert created_services[0].execution_mode_seen == "full"


def test_run_plan_attaches_dbt_downstream_models(tmp_path: Path) -> None:
    config_path = tmp_path / "verixa.yaml"
    config_path.write_text(
        """
warehouse:
  kind: bigquery
  project: demo
baseline:
  path: {baseline_path}
sources:
  stripe.transactions:
    table: raw.stripe_transactions
    schema:
      amount: float
      currency: string
""".format(
            baseline_path=(tmp_path / ".verixa" / "baseline.json").as_posix()
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    (tmp_path / "target").mkdir()
    (tmp_path / "target" / "manifest.json").write_text(
        """
{
  "nodes": {
    "model.demo.stg_orders": {
      "resource_type": "model",
      "name": "stg_orders",
      "original_file_path": "models/staging/orders.sql",
      "depends_on": {
        "nodes": ["source.demo.raw.stripe_transactions"],
        "macros": []
      }
    }
  },
  "sources": {
    "source.demo.raw.stripe_transactions": {
      "database": "demo",
      "schema": "raw",
      "identifier": "stripe_transactions",
      "name": "stripe_transactions",
      "meta": {
        "verixa": {
          "owners": ["finance"],
          "criticality": "high"
        }
      },
      "original_file_path": "models/sources/stripe.yml",
      "depends_on": {
        "nodes": []
      }
    }
  },
  "macros": {}
}
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (tmp_path / "verixa.targets.yaml").write_text(
        """
dbt:
  manifest_path: target/manifest.json
""".strip()
        + "\n",
        encoding="utf-8",
    )

    store = SnapshotStore(tmp_path / ".verixa")
    store.write_baseline(
        ProjectSnapshot(
            warehouse_kind="bigquery",
            generated_at=datetime(2026, 4, 22, 11, 0, tzinfo=timezone.utc),
            sources={
                "stripe.transactions": SourceSnapshot(
                    source_name="stripe.transactions",
                    table="demo.raw.stripe_transactions",
                    schema={"amount": "FLOAT64", "currency": "STRING"},
                    row_count=100,
                    null_rates={"amount": 0.0, "currency": 0.0},
                    freshness=None,
                    accepted_values={},
                    captured_at=datetime(2026, 4, 22, 11, 0, tzinfo=timezone.utc),
                )
            },
        )
    )

    result = run_plan(
        config_path,
        connector_factory=_FakeConnector,
        snapshot_service_factory=_FakeSnapshotService,
        snapshot_store_factory=lambda: store,
    )

    schema_finding = next(
        finding for finding in result.findings if finding.code == "schema_column_missing"
    )
    assert schema_finding.downstream_models == ("stg_orders",)
    assert schema_finding.owners == ("finance",)
    assert schema_finding.source_criticality == "high"
