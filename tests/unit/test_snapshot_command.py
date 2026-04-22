from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from dataguard.cli.snapshot import run_snapshot
from dataguard.output.console import render_snapshot_summary
from dataguard.snapshot.models import ProjectSnapshot, SourceSnapshot


class _FakeConnector:
    def __init__(self, warehouse) -> None:  # noqa: ANN001
        self.warehouse = warehouse


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


def test_run_snapshot_writes_baseline_file(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("dataguard.cli.snapshot.BigQueryConnector", _FakeConnector)
    created_services: list[_FakeSnapshotService] = []

    def _service_factory(connector):  # noqa: ANN001
        service = _FakeSnapshotService(connector)
        created_services.append(service)
        return service

    monkeypatch.setattr("dataguard.cli.snapshot.SnapshotService", _service_factory)

    config_path = tmp_path / "dataguard.yaml"
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

    snapshot, baseline_path = run_snapshot(config_path)

    output = render_snapshot_summary(snapshot, baseline_path)

    assert "Captured baseline snapshot" in output
    assert baseline_path == Path(".dataguard") / "baseline.json"
    assert baseline_path.exists()
    assert created_services[0].mode_seen == "snapshot"
