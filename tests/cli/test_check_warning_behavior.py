from __future__ import annotations

from typer.testing import CliRunner

from verixa.cli.app import app
from verixa.diff.models import DiffResult, Finding


def test_check_command_does_not_fail_on_warning_only_results(monkeypatch) -> None:
    runner = CliRunner()

    monkeypatch.setattr(
        "verixa.cli.app.run_check",
        lambda config, risk_path=None, source_names=(): DiffResult(
            findings=(
                Finding(
                    source_name="stripe.transactions",
                    severity="warning",
                    code="row_count_changed",
                    message="warnings only",
                ),
            ),
            sources_checked=1,
            used_baseline=True,
        ),
    )

    result = runner.invoke(app, ["check", "--fail-on-error"])

    assert result.exit_code == 0
    assert "warnings only" in result.stdout


def test_check_command_can_fail_on_warning_when_requested(monkeypatch) -> None:
    runner = CliRunner()

    monkeypatch.setattr(
        "verixa.cli.app.run_check",
        lambda config, risk_path=None, source_names=(): DiffResult(
            findings=(
                Finding(
                    source_name="stripe.transactions",
                    severity="warning",
                    code="row_count_changed",
                    message="warnings only",
                ),
            ),
            sources_checked=1,
            used_baseline=True,
        ),
    )

    result = runner.invoke(app, ["check", "--fail-on-warning"])

    assert result.exit_code == 1


def test_check_command_fails_when_warning_policy_sources_exist(monkeypatch) -> None:
    runner = CliRunner()

    monkeypatch.setattr(
        "verixa.cli.app.run_check",
        lambda config, risk_path=None, source_names=(): DiffResult(
            findings=(
                Finding(
                    source_name="stripe.transactions",
                    severity="warning",
                    code="row_count_changed",
                    message="warnings only",
                ),
            ),
            sources_checked=1,
            used_baseline=True,
            warning_policy_sources=("stripe.transactions",),
        ),
    )

    result = runner.invoke(app, ["check"])

    assert result.exit_code == 1


def test_check_command_can_emit_json_output(monkeypatch) -> None:
    runner = CliRunner()

    monkeypatch.setattr(
        "verixa.cli.app.run_check",
        lambda config, risk_path=None, source_names=(): DiffResult(
            findings=(
                Finding(
                    source_name="stripe.transactions",
                    severity="warning",
                    code="row_count_changed",
                    message="warnings only",
                ),
            ),
            sources_checked=1,
            used_baseline=True,
        ),
    )

    result = runner.invoke(app, ["check", "--format", "json"])

    assert result.exit_code == 0
    assert '"title": "Check"' in result.stdout
    assert '"severity": "warning"' in result.stdout
    assert '"warning_policy_failures": 0' in result.stdout
