from __future__ import annotations

from typer.testing import CliRunner

from verixa.cli.app import app
from verixa.cli.cost import CostReport
from verixa.cli.status import StatusReport
from verixa.diff.models import DiffResult, Finding



def test_diff_command_uses_diff_title(monkeypatch) -> None:
    runner = CliRunner()

    monkeypatch.setattr(
        "verixa.cli.app.run_diff",
        lambda config, risk_path=None, source_names=(): DiffResult(
            findings=(
                Finding(
                    source_name="stripe.transactions",
                    severity="warning",
                    code="row_count_changed",
                    message="diff output",
                ),
            ),
            sources_checked=1,
            used_baseline=True,
        ),
    )

    result = runner.invoke(app, ["diff"])

    assert result.exit_code == 0
    assert "diff output" in result.stdout



def test_validate_command_uses_validate_title(monkeypatch) -> None:
    runner = CliRunner()

    monkeypatch.setattr(
        "verixa.cli.app.run_validate",
        lambda config, risk_path=None, source_names=(): DiffResult(
            findings=(
                Finding(
                    source_name="stripe.transactions",
                    severity="error",
                    code="no_nulls_violation",
                    message="validate output",
                ),
            ),
            sources_checked=1,
            used_baseline=False,
        ),
    )

    result = runner.invoke(app, ["validate"])

    assert result.exit_code == 0
    assert "validate output" in result.stdout



def test_plan_alias_maps_to_diff(monkeypatch) -> None:
    runner = CliRunner()
    seen: dict[str, object] = {}

    def _run_diff(config, risk_path=None, source_names=()):  # noqa: ANN001
        seen["called"] = True
        return DiffResult(findings=(), sources_checked=1, used_baseline=True)

    monkeypatch.setattr("verixa.cli.app.run_diff", _run_diff)

    result = runner.invoke(app, ["plan"])

    assert result.exit_code == 0
    assert seen["called"] is True



def test_test_alias_maps_to_validate(monkeypatch) -> None:
    runner = CliRunner()
    seen: dict[str, object] = {}

    def _run_validate(config, risk_path=None, source_names=()):  # noqa: ANN001
        seen["called"] = True
        return DiffResult(findings=(), sources_checked=1, used_baseline=False)

    monkeypatch.setattr("verixa.cli.app.run_validate", _run_validate)

    result = runner.invoke(app, ["test"])

    assert result.exit_code == 0
    assert seen["called"] is True



def test_status_command_renders_text(monkeypatch) -> None:
    runner = CliRunner()

    monkeypatch.setattr(
        "verixa.cli.app.run_status",
        lambda config, source_names=(): StatusReport(
            config_path=config,
            config_exists=True,
            config_error=None,
            baseline_path=config.parent / ".verixa" / "baseline.json",
            baseline_exists=True,
            baseline_age_seconds=3600,
            baseline_error=None,
            auth_ok=True,
            auth_message="authenticated",
            warehouse_label="bigquery (demo)",
            sources=("stripe.transactions",),
        ),
    )

    result = runner.invoke(app, ["status"])

    assert result.exit_code == 0
    assert "Status" in result.stdout
    assert "bigquery (demo)" in result.stdout



def test_doctor_command_fails_on_errors(monkeypatch) -> None:
    runner = CliRunner()

    monkeypatch.setattr(
        "verixa.cli.app.run_doctor",
        lambda config, source_names=(): DiffResult(
            findings=(
                Finding(
                    source_name="auth",
                    severity="error",
                    code="auth_unusable",
                    message="doctor error",
                ),
            ),
            sources_checked=1,
            used_baseline=False,
        ),
    )

    result = runner.invoke(app, ["doctor"])

    assert result.exit_code == 1
    assert "doctor error" in result.stdout



def test_explain_command_renders_text(monkeypatch) -> None:
    runner = CliRunner()

    monkeypatch.setattr(
        "verixa.cli.app.run_explain",
        lambda config, source_name: {
            "source_name": source_name,
            "table": "raw.stripe_transactions",
            "schema": [{"name": "amount", "type": "FLOAT64"}],
            "freshness": {"column": "created_at", "max_age": "1h"},
            "scan": None,
            "check": {"fail_on_warning": False},
            "rules": {
                "null_rate_change": {"warning_delta": 0.01, "error_delta": 0.05},
                "row_count_change": {
                    "warning_drop_ratio": 0.2,
                    "error_drop_ratio": 0.5,
                    "warning_growth_ratio": 0.2,
                    "error_growth_ratio": 1.0,
                },
            },
            "tests": [{"kind": "no_nulls", "column": "amount"}],
        },
    )

    result = runner.invoke(app, ["explain", "stripe.transactions"])

    assert result.exit_code == 0
    assert "raw.stripe_transactions" in result.stdout
    assert "no_nulls: amount" in result.stdout



def test_cost_command_renders_json(monkeypatch) -> None:
    runner = CliRunner()

    monkeypatch.setattr(
        "verixa.cli.app.run_cost",
        lambda config, command, source_names=(): CostReport(
            command="diff",
            estimates={"stripe.transactions": 2048},
        ),
    )

    result = runner.invoke(app, ["cost", "diff", "--format", "json"])

    assert result.exit_code == 0
    assert '"command": "diff"' in result.stdout
    assert '"total_bytes_processed": 2048' in result.stdout
