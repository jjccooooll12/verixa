from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from verixa.cli.snapshot import run_snapshot
from verixa.output.console import render_snapshot_summary
from verixa.snapshot.models import ProjectSnapshot, SourceSnapshot
from verixa.storage.filesystem import SnapshotStore


class _FakeConnector:
    def __init__(self, warehouse, *, max_bytes_billed=None) -> None:  # noqa: ANN001
        self.warehouse = warehouse
        self.max_bytes_billed = max_bytes_billed


class _FakeSnapshotService:
    def __init__(self, connector) -> None:  # noqa: ANN001
        self.connector = connector
        self.mode_seen = None

    def capture(self, config, *, mode="snapshot") -> ProjectSnapshot:  # noqa: ANN001
        self.mode_seen = mode
        return ProjectSnapshot(
            warehouse_kind="bigquery",
            generated_at=datetime(2026, 4, 22, 11, 0, tzinfo=timezone.utc),
            sources={
                "stripe.transactions": SourceSnapshot(
                    source_name="stripe.transactions",
                    table="demo.raw.stripe_transactions",
                    schema={"amount": "FLOAT64"},
                    row_count=42,
                    null_rates={"amount": 0.0},
                    freshness=None,
                    accepted_values={},
                    captured_at=datetime(2026, 4, 22, 11, 0, tzinfo=timezone.utc),
                )
            },
        )


def test_run_snapshot_writes_baseline_file(tmp_path: Path) -> None:
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

    snapshot, baseline_path = run_snapshot(
        config_path,
        connector_factory=_FakeConnector,
        snapshot_service_factory=_service_factory,
        snapshot_store_factory=lambda: SnapshotStore(tmp_path / ".verixa"),
    )

    output = render_snapshot_summary(snapshot, baseline_path)

    assert "Captured baseline snapshot" in output
    assert baseline_path == tmp_path / ".verixa" / "baseline.json"
    assert baseline_path.exists()
    assert created_services[0].mode_seen == "snapshot"


def test_run_snapshot_passes_max_bytes_billed_to_connector(tmp_path: Path) -> None:
    created_connectors: list[_FakeConnector] = []

    def _connector_factory(warehouse, *, max_bytes_billed=None):  # noqa: ANN001
        connector = _FakeConnector(warehouse, max_bytes_billed=max_bytes_billed)
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

    run_snapshot(
        config_path,
        max_bytes_billed=1024,
        connector_factory=_connector_factory,
        snapshot_service_factory=_FakeSnapshotService,
        snapshot_store_factory=lambda: SnapshotStore(tmp_path / ".verixa"),
    )

    assert created_connectors[0].max_bytes_billed == 1024
