"""Typer application for the DataGuard CLI."""

from __future__ import annotations

from enum import Enum
from pathlib import Path

import typer

from dataguard.cli.check import run_check
from dataguard.cli.init import init_project
from dataguard.cli.plan import run_plan
from dataguard.cli.snapshot import run_snapshot
from dataguard.cli.test import run_test
from dataguard.config.loader import load_config
from dataguard.config.errors import ConfigError
from dataguard.connectors.bigquery.connector import BigQueryConnector
from dataguard.connectors.base import ConnectorError
from dataguard.diff.models import DiffResult
from dataguard.output.console import render_diff_result, render_snapshot_summary
from dataguard.output.exit_codes import FINDINGS_ERROR, RUNTIME_ERROR, SUCCESS
from dataguard.output.json import (
    render_diff_result_json,
    render_error_json,
    render_snapshot_summary_json,
)
from dataguard.snapshot.service import SnapshotService
from dataguard.storage.filesystem import StorageError

app = typer.Typer(add_completion=False, help="Developer-first Data CI for source tables.")


class OutputFormat(str, Enum):
    """Output formats supported by the CLI."""

    TEXT = "text"
    JSON = "json"


@app.command("init")
def init_command(
    force: bool = typer.Option(False, help="Overwrite starter files if they already exist."),
) -> None:
    """Initialize a DataGuard project."""

    try:
        created = init_project(force=force)
    except FileExistsError as exc:
        _exit_with_error(str(exc))
    typer.echo("Created:")
    for path in created:
        typer.echo(f"- {path}")


@app.command("snapshot")
def snapshot_command(
    config: Path = typer.Option(Path("dataguard.yaml"), help="Path to dataguard.yaml."),
    source: list[str] | None = typer.Option(
        None,
        "--source",
        help="Limit the command to one or more source names. Repeat to select multiple.",
    ),
    output: OutputFormat = typer.Option(
        OutputFormat.TEXT,
        "--format",
        help="Output format.",
    ),
    estimate_bytes: bool = typer.Option(
        False,
        "--estimate-bytes",
        help="Estimate BigQuery bytes processed for live stats queries and include it in output.",
    ),
) -> None:
    """Capture the current source baseline and store it locally."""

    try:
        estimates = _estimate_bytes(config, tuple(source or ()), mode="snapshot") if estimate_bytes else None
        snapshot, baseline_path = run_snapshot(
            config,
            source_names=tuple(source or ()),
        )
        typer.echo(_render_snapshot_output(snapshot, baseline_path, output, estimates))
    except (ConfigError, ConnectorError, StorageError) as exc:
        _exit_with_error(str(exc), output)


@app.command("plan")
def plan_command(
    config: Path = typer.Option(Path("dataguard.yaml"), help="Path to dataguard.yaml."),
    risk_config: Path | None = typer.Option(
        Path("dataguard.risk.yaml"),
        help="Optional path to downstream risk mapping YAML.",
    ),
    source: list[str] | None = typer.Option(
        None,
        "--source",
        help="Limit the command to one or more source names. Repeat to select multiple.",
    ),
    output: OutputFormat = typer.Option(
        OutputFormat.TEXT,
        "--format",
        help="Output format.",
    ),
    estimate_bytes: bool = typer.Option(
        False,
        "--estimate-bytes",
        help="Estimate BigQuery bytes processed for live stats queries and include it in output.",
    ),
) -> None:
    """Show likely breakages before deploy."""

    try:
        estimates = _estimate_bytes(config, tuple(source or ()), mode="plan") if estimate_bytes else None
        result = run_plan(
            config,
            risk_path=risk_config,
            source_names=tuple(source or ()),
        )
        typer.echo(_render_diff_output(result, "Plan", output, estimates))
    except (ConfigError, ConnectorError, StorageError) as exc:
        _exit_with_error(str(exc), output)


@app.command("test")
def test_command(
    config: Path = typer.Option(Path("dataguard.yaml"), help="Path to dataguard.yaml."),
    risk_config: Path | None = typer.Option(
        Path("dataguard.risk.yaml"),
        help="Optional path to downstream risk mapping YAML.",
    ),
    source: list[str] | None = typer.Option(
        None,
        "--source",
        help="Limit the command to one or more source names. Repeat to select multiple.",
    ),
    output: OutputFormat = typer.Option(
        OutputFormat.TEXT,
        "--format",
        help="Output format.",
    ),
    estimate_bytes: bool = typer.Option(
        False,
        "--estimate-bytes",
        help="Estimate BigQuery bytes processed for live stats queries and include it in output.",
    ),
) -> None:
    """Run contract checks against current live data."""

    try:
        estimates = _estimate_bytes(config, tuple(source or ()), mode="test") if estimate_bytes else None
        result = run_test(
            config,
            risk_path=risk_config,
            source_names=tuple(source or ()),
        )
        typer.echo(_render_diff_output(result, "Test", output, estimates))
    except (ConfigError, ConnectorError, StorageError) as exc:
        _exit_with_error(str(exc), output)


@app.command("check")
def check_command(
    config: Path = typer.Option(Path("dataguard.yaml"), help="Path to dataguard.yaml."),
    risk_config: Path | None = typer.Option(
        Path("dataguard.risk.yaml"),
        help="Optional path to downstream risk mapping YAML.",
    ),
    source: list[str] | None = typer.Option(
        None,
        "--source",
        help="Limit the command to one or more source names. Repeat to select multiple.",
    ),
    fail_on_error: bool = typer.Option(
        False,
        "--fail-on-error",
        help="Exit with status 1 when error-severity findings exist.",
    ),
    fail_on_warning: bool = typer.Option(
        False,
        "--fail-on-warning",
        help="Exit with status 1 when any warning-severity findings exist.",
    ),
    output: OutputFormat = typer.Option(
        OutputFormat.TEXT,
        "--format",
        help="Output format.",
    ),
    estimate_bytes: bool = typer.Option(
        False,
        "--estimate-bytes",
        help="Estimate BigQuery bytes processed for live stats queries and include it in output.",
    ),
) -> None:
    """Run CI-friendly validation."""

    try:
        estimates = _estimate_bytes(config, tuple(source or ()), mode="plan") if estimate_bytes else None
        result = run_check(
            config,
            risk_path=risk_config,
            source_names=tuple(source or ()),
        )
        typer.echo(_render_diff_output(result, "Check", output, estimates))
    except (ConfigError, ConnectorError, StorageError) as exc:
        _exit_with_error(str(exc), output)

    should_fail = False
    if fail_on_error and result.error_count > 0:
        should_fail = True
    if fail_on_warning and result.warning_count > 0:
        should_fail = True
    if result.warning_policy_failure_count > 0:
        should_fail = True
    if should_fail:
        raise typer.Exit(code=FINDINGS_ERROR)
    raise typer.Exit(code=SUCCESS)


def _render_snapshot_output(
    snapshot,
    baseline_path: Path,
    output: OutputFormat,
    estimated_bytes_by_source: dict[str, int] | None = None,
) -> str:
    if output == OutputFormat.JSON:
        return render_snapshot_summary_json(snapshot, baseline_path, estimated_bytes_by_source)
    return render_snapshot_summary(snapshot, baseline_path, estimated_bytes_by_source)


def _render_diff_output(
    result: DiffResult,
    title: str,
    output: OutputFormat,
    estimated_bytes_by_source: dict[str, int] | None = None,
) -> str:
    if output == OutputFormat.JSON:
        return render_diff_result_json(result, title, estimated_bytes_by_source)
    return render_diff_result(result, title=title, estimated_bytes_by_source=estimated_bytes_by_source)


def _exit_with_error(message: str, output: OutputFormat) -> None:
    if output == OutputFormat.JSON:
        typer.echo(render_error_json(message, RUNTIME_ERROR), err=True)
    else:
        typer.secho(f"Error: {message}", err=True, fg=typer.colors.RED)
    raise typer.Exit(code=RUNTIME_ERROR)


def main() -> None:
    """CLI entrypoint used by console scripts."""

    app()


def _estimate_bytes(
    config_path: Path,
    source_names: tuple[str, ...],
    *,
    mode: str,
) -> dict[str, int]:
    config = load_config(config_path, source_names=source_names)
    connector = BigQueryConnector(config.warehouse)
    service = SnapshotService(connector)
    return service.estimate_bytes(config, mode=mode)
