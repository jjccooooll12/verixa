from __future__ import annotations

from typer.testing import CliRunner

from verixa.cli.app import app
from verixa.diff.models import DiffResult, Finding


def test_check_command_fails_when_requested_and_errors_exist(monkeypatch) -> None:
    runner = CliRunner()

    monkeypatch.setattr(
        "verixa.cli.app.run_check",
        lambda config, risk_path=None, source_names=(): DiffResult(
            findings=(
                Finding(
                    source_name="stripe.transactions",
                    severity="error",
                    code="schema_column_missing",
                    message="findings",
                ),
            ),
            sources_checked=1,
            used_baseline=True,
        ),
    )

    result = runner.invoke(app, ["check", "--fail-on-error"])

    assert result.exit_code == 1
    assert "findings" in result.stdout


def test_check_command_passes_source_selection(monkeypatch) -> None:
    runner = CliRunner()
    seen: dict[str, object] = {}

    def _run_check(config, risk_path=None, source_names=()):  # noqa: ANN001
        seen["source_names"] = source_names
        return DiffResult(findings=(), sources_checked=1, used_baseline=True)

    monkeypatch.setattr("verixa.cli.app.run_check", _run_check)

    result = runner.invoke(app, ["check", "--source", "stripe.transactions"])

    assert result.exit_code == 0
    assert seen["source_names"] == ("stripe.transactions",)


def test_check_command_can_include_estimated_bytes(monkeypatch) -> None:
    runner = CliRunner()

    monkeypatch.setattr(
        "verixa.cli.app._estimate_bytes",
        lambda config, source_names, command: {"stripe.transactions": 2048},
    )
    monkeypatch.setattr(
        "verixa.cli.app.run_check",
        lambda config, risk_path=None, source_names=(): DiffResult(
            findings=(),
            sources_checked=1,
            used_baseline=True,
        ),
    )

    result = runner.invoke(app, ["check", "--estimate-bytes"])

    assert result.exit_code == 0
    assert "Estimated scan: 2.0 KB total" in result.stdout
