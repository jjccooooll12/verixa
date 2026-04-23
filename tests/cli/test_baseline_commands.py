from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from typer.testing import CliRunner

from tests.cli.support import build_app
from verixa.baselines.models import BaselineProposal, BaselineStatusReport
from verixa.snapshot.models import ProjectSnapshot


def test_baseline_status_command_renders_text() -> None:
    runner = CliRunner()
    app = build_app(
        run_baseline_status=lambda config, environment: BaselineStatusReport(
            environment=environment,
            baseline_path=Path(".verixa/prod/baseline.json"),
            baseline_exists=True,
            baseline_age_seconds=3600,
            baseline_stale=False,
            warning_age_seconds=7 * 24 * 3600,
            current_proposal_id="prop123",
            current_reason="expected rollout",
            current_created_at=datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc),
            current_created_by="tester",
            current_git_sha="abc123",
            current_promoted_at=datetime(2026, 4, 23, 13, 0, tzinfo=timezone.utc),
            current_promoted_by="reviewer",
            remediation=None,
            pending_proposals=("prop123",),
        )
    )

    result = runner.invoke(app, ["baseline", "status", "--environment", "prod"])

    assert result.exit_code == 0
    assert "Baseline prod" in result.stdout
    assert "Current proposal: prop123" in result.stdout
    assert "Captured by: tester" in result.stdout
    assert "Promoted by: reviewer" in result.stdout


def test_baseline_propose_command_renders_json() -> None:
    runner = CliRunner()
    app = build_app(
        run_baseline_propose=lambda config, environment, reason, source_names=(), max_bytes_billed=None: BaselineProposal(
            schema_version="verixa.baseline.proposal.v1",
            proposal_id="prop123",
            environment=environment,
            reason=reason,
            created_at=datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc),
            created_by="tester",
            git_sha="abc123",
            source_names=source_names,
            baseline_path=Path(".verixa/prod/baseline.json"),
            snapshot=ProjectSnapshot(warehouse_kind="bigquery", generated_at=datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc), sources={}),
        )
    )

    result = runner.invoke(
        app,
        [
            "baseline",
            "propose",
            "--environment",
            "prod",
            "--reason",
            "expected change",
            "--source",
            "stripe.transactions",
            "--format",
            "json",
        ],
    )

    assert result.exit_code == 0
    assert '"proposal_id": "prop123"' in result.stdout
    assert '"reason": "expected change"' in result.stdout


def test_baseline_promote_command_renders_text() -> None:
    runner = CliRunner()
    app = build_app(
        run_baseline_promote=lambda config, environment, proposal_id: Path(".verixa/prod/baseline.json")
    )

    result = runner.invoke(
        app,
        [
            "baseline",
            "promote",
            "--environment",
            "prod",
            "--proposal-id",
            "prop123",
        ],
    )

    assert result.exit_code == 0
    assert "Promoted baseline proposal prop123" in result.stdout


def test_baseline_accept_command_renders_text() -> None:
    runner = CliRunner()
    app = build_app(
        run_baseline_accept=lambda config, environment, reason, source_names=(), max_bytes_billed=None: BaselineProposal(
            schema_version="verixa.baseline.proposal.v1",
            proposal_id="prop999",
            environment=environment,
            reason=reason,
            created_at=datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc),
            created_by="tester",
            git_sha="abc123",
            source_names=source_names,
            baseline_path=Path(".verixa/prod/baseline.json"),
            snapshot=ProjectSnapshot(warehouse_kind="bigquery", generated_at=datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc), sources={}),
        )
    )

    result = runner.invoke(
        app,
        [
            "baseline",
            "accept",
            "--environment",
            "prod",
            "--reason",
            "expected rollout",
            "--source",
            "stripe.transactions",
        ],
    )

    assert result.exit_code == 0
    assert "Created baseline proposal prop999" in result.stdout
