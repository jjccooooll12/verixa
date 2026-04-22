from __future__ import annotations

from typer.testing import CliRunner

from tests.cli.support import build_app
from verixa.storage.filesystem import StorageError


def test_check_command_returns_runtime_error_when_baseline_is_missing() -> None:
    runner = CliRunner()

    def _raise_missing_baseline(config, risk_path=None, source_names=(), environment=None, max_bytes_billed=None):  # noqa: ANN001
        raise StorageError("baseline missing")

    app = build_app(run_check=_raise_missing_baseline)
    result = runner.invoke(app, ["check", "--fail-on-error"])

    assert result.exit_code == 2
    assert "Error: baseline missing" in result.stderr


def test_check_command_can_emit_json_runtime_errors() -> None:
    runner = CliRunner()

    def _raise_missing_baseline(config, risk_path=None, source_names=(), environment=None, max_bytes_billed=None):  # noqa: ANN001
        raise StorageError("baseline missing")

    app = build_app(run_check=_raise_missing_baseline)
    result = runner.invoke(app, ["check", "--fail-on-error", "--format", "json"])

    assert result.exit_code == 2
    assert '"message": "baseline missing"' in result.stderr
    assert '"exit_code": 2' in result.stderr
