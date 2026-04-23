from __future__ import annotations

from typer.testing import CliRunner

from tests.cli.support import build_app
from verixa.diff.models import DiffResult, Finding


def test_check_command_fails_when_requested_and_errors_exist() -> None:
    runner = CliRunner()
    app = build_app(
        run_check=lambda config, risk_path=None, source_names=(), environment=None, max_bytes_billed=None: DiffResult(
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
        )
    )

    result = runner.invoke(app, ["check", "--fail-on-error"])

    assert result.exit_code == 1
    assert "findings" in result.stdout


def test_check_command_passes_source_selection() -> None:
    runner = CliRunner()
    seen: dict[str, object] = {}

    def _run_check(  # noqa: ANN001
        config,
        risk_path=None,
        source_names=(),
        environment=None,
        max_bytes_billed=None,
        execution_mode="bounded",
    ):
        seen["source_names"] = source_names
        seen["max_bytes_billed"] = max_bytes_billed
        seen["execution_mode"] = execution_mode
        return DiffResult(findings=(), sources_checked=1, used_baseline=True)

    app = build_app(run_check=_run_check)
    result = runner.invoke(
        app,
        ["check", "--source", "stripe.transactions", "--execution-mode", "cheap"],
    )

    assert result.exit_code == 0
    assert seen["source_names"] == ("stripe.transactions",)
    assert seen["max_bytes_billed"] is None
    assert seen["execution_mode"] == "cheap"


def test_check_command_can_include_estimated_bytes() -> None:
    runner = CliRunner()
    app = build_app(
        estimate_bytes=lambda config, source_names, command: {"stripe.transactions": 2048},
        run_check=lambda config, risk_path=None, source_names=(), environment=None, max_bytes_billed=None: DiffResult(
            findings=(),
            sources_checked=1,
            used_baseline=True,
        ),
    )

    result = runner.invoke(app, ["check", "--estimate-bytes"])

    assert result.exit_code == 0
    assert "Estimated scan: 2.0 KB total" in result.stdout


def test_check_command_passes_max_bytes_billed_override() -> None:
    runner = CliRunner()
    seen: dict[str, object] = {}

    def _run_check(  # noqa: ANN001
        config,
        risk_path=None,
        source_names=(),
        environment=None,
        max_bytes_billed=None,
        execution_mode="bounded",
    ):
        seen["max_bytes_billed"] = max_bytes_billed
        return DiffResult(findings=(), sources_checked=1, used_baseline=True)

    app = build_app(run_check=_run_check)
    result = runner.invoke(app, ["check", "--max-bytes-billed", "500MB"])

    assert result.exit_code == 0
    assert seen["max_bytes_billed"] == 500 * 1024 * 1024


def test_check_command_does_not_fail_in_advisory_mode() -> None:
    runner = CliRunner()
    app = build_app(
        run_check=lambda config, risk_path=None, source_names=(), environment=None, max_bytes_billed=None: DiffResult(
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
        )
    )

    result = runner.invoke(app, ["check", "--fail-on-error", "--advisory", "--format", "json"])

    assert result.exit_code == 0
    assert '"advisory_mode": true' in result.stdout


def test_check_command_respects_project_level_advisory_mode() -> None:
    runner = CliRunner()
    app = build_app(
        run_check=lambda config, risk_path=None, source_names=(), environment=None, max_bytes_billed=None: DiffResult(
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
            advisory_mode_enabled=True,
        )
    )

    result = runner.invoke(app, ["check", "--fail-on-error"])

    assert result.exit_code == 0
