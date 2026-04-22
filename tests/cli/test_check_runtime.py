from __future__ import annotations

from typer.testing import CliRunner

from verixa.cli.app import app
from verixa.storage.filesystem import StorageError


def test_check_command_returns_runtime_error_when_baseline_is_missing(monkeypatch) -> None:
    runner = CliRunner()

    def _raise_missing_baseline(config, risk_path=None, source_names=()):  # noqa: ANN001
        raise StorageError("baseline missing")

    monkeypatch.setattr("verixa.cli.app.run_check", _raise_missing_baseline)

    result = runner.invoke(app, ["check", "--fail-on-error"])

    assert result.exit_code == 2
    assert "Error: baseline missing" in result.stderr


def test_check_command_can_emit_json_runtime_errors(monkeypatch) -> None:
    runner = CliRunner()

    def _raise_missing_baseline(config, risk_path=None, source_names=()):  # noqa: ANN001
        raise StorageError("baseline missing")

    monkeypatch.setattr("verixa.cli.app.run_check", _raise_missing_baseline)

    result = runner.invoke(app, ["check", "--fail-on-error", "--format", "json"])

    assert result.exit_code == 2
    assert '"message": "baseline missing"' in result.stderr
    assert '"exit_code": 2' in result.stderr
