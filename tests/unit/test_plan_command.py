from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from verixa.cli.plan import run_plan
from verixa.output.console import render_diff_result
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
    assert created_services[0].mode_seen == "plan"
