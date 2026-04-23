from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from verixa.baselines.manager import BaselineManager
from verixa.snapshot.models import ProjectSnapshot, SourceSnapshot
from verixa.storage.filesystem import SnapshotStore


def _snapshot(row_count: int = 10) -> ProjectSnapshot:
    return ProjectSnapshot(
        warehouse_kind="bigquery",
        generated_at=datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc),
        sources={
            "stripe.transactions": SourceSnapshot(
                source_name="stripe.transactions",
                table="demo.raw.stripe_transactions",
                schema={"amount": "FLOAT64"},
                row_count=row_count,
                null_rates={"amount": 0.0},
                freshness=None,
                accepted_values={},
                captured_at=datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc),
            )
        },
    )


def test_baseline_manager_creates_and_reads_proposal(tmp_path: Path) -> None:
    manager = BaselineManager(tmp_path / ".verixa" / "baselines")

    proposal = manager.create_proposal(
        _snapshot(),
        environment="prod",
        reason="accept expected drift",
        source_names=("stripe.transactions",),
        baseline_path=Path(".verixa/prod/baseline.json"),
    )
    loaded = manager.read_proposal(proposal.proposal_id)

    assert loaded.proposal_id == proposal.proposal_id
    assert loaded.environment == "prod"
    assert loaded.reason == "accept expected drift"
    assert loaded.snapshot.sources["stripe.transactions"].row_count == 10
    assert loaded.created_by is None or isinstance(loaded.created_by, str)


def test_baseline_manager_promotes_and_archives_existing_baseline(tmp_path: Path) -> None:
    manager = BaselineManager(tmp_path / ".verixa" / "baselines")
    baseline_path = tmp_path / ".verixa" / "prod" / "baseline.json"
    SnapshotStore(baseline_path=baseline_path).write_baseline(_snapshot(row_count=5))

    proposal = manager.create_proposal(
        _snapshot(row_count=20),
        environment="prod",
        reason="expected upstream rollout",
        source_names=("stripe.transactions",),
        baseline_path=baseline_path,
    )

    promoted_path = manager.promote_proposal(proposal.proposal_id, baseline_path=baseline_path)
    loaded = SnapshotStore(baseline_path=baseline_path).read_baseline()

    assert promoted_path == baseline_path
    assert loaded.sources["stripe.transactions"].row_count == 20
    assert manager.current_manifest_path("prod").exists()
    archived = manager.history_dir("prod") / f"{proposal.proposal_id}.baseline.json"
    assert archived.exists()


def test_baseline_manager_status_reports_pending_proposals(tmp_path: Path) -> None:
    manager = BaselineManager(tmp_path / ".verixa" / "baselines")
    baseline_path = tmp_path / ".verixa" / "staging" / "baseline.json"
    SnapshotStore(baseline_path=baseline_path).write_baseline(_snapshot())
    proposal = manager.create_proposal(
        _snapshot(),
        environment="staging",
        reason="refresh staging",
        source_names=("stripe.transactions",),
        baseline_path=baseline_path,
    )

    report = manager.status(environment="staging", baseline_path=baseline_path)

    assert report.environment == "staging"
    assert report.baseline_exists is True
    assert report.baseline_stale is False
    assert proposal.proposal_id in report.pending_proposals


def test_baseline_manager_status_reports_stale_with_remediation(tmp_path: Path) -> None:
    manager = BaselineManager(tmp_path / ".verixa" / "baselines")
    baseline_path = tmp_path / ".verixa" / "prod" / "baseline.json"
    stale_snapshot = ProjectSnapshot(
        warehouse_kind="bigquery",
        generated_at=datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
        sources=_snapshot().sources,
    )
    SnapshotStore(baseline_path=baseline_path).write_baseline(stale_snapshot)

    report = manager.status(
        environment="prod",
        baseline_path=baseline_path,
        warning_age_seconds=24 * 3600,
    )

    assert report.baseline_stale is True
    assert report.remediation is not None
    assert "verixa baseline propose --environment prod" in report.remediation
