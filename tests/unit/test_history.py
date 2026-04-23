from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from verixa.findings.schema import NormalizedFinding
from verixa.history.classifier import classify_finding_lifecycle
from verixa.history.store import RunHistoryStore, SnapshotHistoryStore
from verixa.snapshot.models import ProjectSnapshot
from tests.unit.test_support import make_source_snapshot


def _finding(
    *,
    fingerprint: str,
    code: str = "no_nulls_violation",
    source_name: str = "stripe.transactions",
) -> NormalizedFinding:
    return NormalizedFinding(
        schema_version="verixa.finding.v2",
        fingerprint=fingerprint,
        source_name=source_name,
        severity="error",
        code=code,
        stable_code=f"contract.{code}",
        message="message",
        category="contract",
        change_type="contract_violation",
        baseline_status="available",
        confidence="high",
        lifecycle_status="new",
        remediation="fix it",
    )


def test_classify_finding_lifecycle_marks_new_recurring_and_resolved() -> None:
    previous = (_finding(fingerprint="a"), _finding(fingerprint="b"))
    current = (_finding(fingerprint="b"), _finding(fingerprint="c"))

    report = classify_finding_lifecycle(current, previous)

    assert [item.lifecycle_status for item in report.active_findings] == ["recurring", "new"]
    assert [item.fingerprint for item in report.resolved_findings] == ["a"]
    assert report.resolved_findings[0].lifecycle_status == "resolved"


def test_run_history_store_round_trips_findings(tmp_path: Path) -> None:
    store = RunHistoryStore(tmp_path / ".verixa" / "history")
    findings = (_finding(fingerprint="abc"),)

    path = store.write_run("diff", findings, environment="prod")
    loaded = store.read_last_run("diff", environment="prod")

    assert path == tmp_path / ".verixa" / "history" / "prod" / "diff.json"
    assert loaded is not None
    assert loaded.command == "diff"
    assert loaded.environment == "prod"
    assert loaded.findings[0].fingerprint == "abc"


def test_snapshot_history_store_round_trips_snapshots(tmp_path: Path) -> None:
    store = SnapshotHistoryStore(tmp_path / ".verixa" / "history" / "snapshots")
    snapshot = ProjectSnapshot(
        warehouse_kind="bigquery",
        generated_at=datetime(2026, 4, 22, 11, 0, tzinfo=timezone.utc),
        sources={"stripe.transactions": make_source_snapshot()},
    )

    path = store.write_run("diff", snapshot, environment="prod", execution_mode="bounded")
    loaded = store.list_runs(environment="prod")

    assert path.parent == tmp_path / ".verixa" / "history" / "snapshots" / "prod"
    assert len(loaded) == 1
    assert loaded[0].command == "diff"
    assert loaded[0].environment == "prod"
    assert loaded[0].execution_mode == "bounded"
    assert loaded[0].snapshot == snapshot
