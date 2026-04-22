"""Typer application for the Verixa CLI."""

from __future__ import annotations

import json
from enum import Enum
from pathlib import Path
from typing import Any

import typer

from verixa.cli.check import run_check
from verixa.cli.cost import CostReport, run_cost
from verixa.cli.diff import run_diff
from verixa.cli.doctor import run_doctor
from verixa.cli.explain import run_explain
from verixa.cli.init import init_project
from verixa.cli.snapshot import run_snapshot
from verixa.cli.status import StatusReport, run_status
from verixa.cli.validate import run_validate
from verixa.config.errors import ConfigError
from verixa.connectors.base import ConnectorError
from verixa.diff.models import DiffResult
from verixa.output.console import render_diff_result, render_snapshot_summary
from verixa.output.exit_codes import FINDINGS_ERROR, RUNTIME_ERROR, SUCCESS
from verixa.output.json import (
    render_diff_result_json,
    render_error_json,
    render_snapshot_summary_json,
)
from verixa.storage.filesystem import StorageError

app = typer.Typer(add_completion=False, help="Verixa: developer-first Data CI for source contracts.")


class OutputFormat(str, Enum):
    """Output formats supported by the CLI."""

    TEXT = "text"
    JSON = "json"


@app.command("init")
def init_command(
    force: bool = typer.Option(False, help="Overwrite starter files if they already exist."),
) -> None:
    """Initialize a Verixa project."""

    try:
        created = init_project(force=force)
    except FileExistsError as exc:
        _exit_with_error(str(exc))
    typer.echo("Created:")
    for path in created:
        typer.echo(f"- {path}")


@app.command("snapshot")
def snapshot_command(
    config: Path = typer.Option(Path("verixa.yaml"), help="Path to verixa.yaml."),
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
        help="Estimate BigQuery bytes processed for the snapshot query shape and include it in output.",
    ),
) -> None:
    """Capture the current source baseline and store it locally."""

    try:
        selected_sources = tuple(source or ())
        estimates = _estimate_bytes(config, selected_sources, command="snapshot") if estimate_bytes else None
        snapshot, baseline_path = run_snapshot(config, source_names=selected_sources)
        typer.echo(_render_snapshot_output(snapshot, baseline_path, output, estimates))
    except (ConfigError, ConnectorError, StorageError, ValueError) as exc:
        _exit_with_error(str(exc), output)


@app.command("diff")
def diff_command(
    config: Path = typer.Option(Path("verixa.yaml"), help="Path to verixa.yaml."),
    risk_config: Path | None = typer.Option(
        Path("verixa.risk.yaml"),
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
        help="Estimate BigQuery bytes processed for the diff query shape and include it in output.",
    ),
) -> None:
    """Show contract and baseline drift for current data."""

    _run_diff_like_command(
        command_name="diff",
        config=config,
        risk_config=risk_config,
        source=source,
        output=output,
        estimate_bytes=estimate_bytes,
    )


@app.command("plan", hidden=True)
def plan_alias_command(
    config: Path = typer.Option(Path("verixa.yaml"), help="Path to verixa.yaml."),
    risk_config: Path | None = typer.Option(Path("verixa.risk.yaml")),
    source: list[str] | None = typer.Option(None, "--source"),
    output: OutputFormat = typer.Option(OutputFormat.TEXT, "--format"),
    estimate_bytes: bool = typer.Option(False, "--estimate-bytes"),
) -> None:
    """Legacy alias for ``verixa diff``."""

    _run_diff_like_command(
        command_name="diff",
        config=config,
        risk_config=risk_config,
        source=source,
        output=output,
        estimate_bytes=estimate_bytes,
    )


@app.command("validate")
def validate_command(
    config: Path = typer.Option(Path("verixa.yaml"), help="Path to verixa.yaml."),
    risk_config: Path | None = typer.Option(
        Path("verixa.risk.yaml"),
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
        help="Estimate BigQuery bytes processed for the validate query shape and include it in output.",
    ),
) -> None:
    """Run contract validation against current live data."""

    _run_validate_like_command(
        command_name="validate",
        config=config,
        risk_config=risk_config,
        source=source,
        output=output,
        estimate_bytes=estimate_bytes,
    )


@app.command("test", hidden=True)
def test_alias_command(
    config: Path = typer.Option(Path("verixa.yaml"), help="Path to verixa.yaml."),
    risk_config: Path | None = typer.Option(Path("verixa.risk.yaml")),
    source: list[str] | None = typer.Option(None, "--source"),
    output: OutputFormat = typer.Option(OutputFormat.TEXT, "--format"),
    estimate_bytes: bool = typer.Option(False, "--estimate-bytes"),
) -> None:
    """Legacy alias for ``verixa validate``."""

    _run_validate_like_command(
        command_name="validate",
        config=config,
        risk_config=risk_config,
        source=source,
        output=output,
        estimate_bytes=estimate_bytes,
    )


@app.command("check")
def check_command(
    config: Path = typer.Option(Path("verixa.yaml"), help="Path to verixa.yaml."),
    risk_config: Path | None = typer.Option(
        Path("verixa.risk.yaml"),
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
        help="Estimate BigQuery bytes processed for the check query shape and include it in output.",
    ),
) -> None:
    """Run the CI-friendly validation path."""

    try:
        selected_sources = tuple(source or ())
        estimates = _estimate_bytes(config, selected_sources, command="check") if estimate_bytes else None
        result = run_check(
            config,
            risk_path=risk_config,
            source_names=selected_sources,
        )
        typer.echo(_render_diff_output(result, "Check", output, estimates))
    except (ConfigError, ConnectorError, StorageError, ValueError) as exc:
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


@app.command("status")
def status_command(
    config: Path = typer.Option(Path("verixa.yaml"), help="Path to verixa.yaml."),
    source: list[str] | None = typer.Option(
        None,
        "--source",
        help="Optionally limit status to one or more source names.",
    ),
    output: OutputFormat = typer.Option(OutputFormat.TEXT, "--format", help="Output format."),
) -> None:
    """Show config, baseline, auth, and source status."""

    try:
        report = run_status(config, source_names=tuple(source or ()))
        typer.echo(_render_status_output(report, output))
    except (ConfigError, ConnectorError, StorageError) as exc:
        _exit_with_error(str(exc), output)


@app.command("doctor")
def doctor_command(
    config: Path = typer.Option(Path("verixa.yaml"), help="Path to verixa.yaml."),
    source: list[str] | None = typer.Option(
        None,
        "--source",
        help="Optionally limit diagnostics to one or more source names.",
    ),
    output: OutputFormat = typer.Option(OutputFormat.TEXT, "--format", help="Output format."),
) -> None:
    """Run diagnostics for config, auth, baseline, and source access."""

    try:
        result = run_doctor(config, source_names=tuple(source or ()))
        typer.echo(_render_diff_output(result, "Doctor", output))
    except (ConfigError, ConnectorError, StorageError) as exc:
        _exit_with_error(str(exc), output)

    if result.error_count > 0:
        raise typer.Exit(code=FINDINGS_ERROR)
    raise typer.Exit(code=SUCCESS)


@app.command("explain")
def explain_command(
    source_name: str = typer.Argument(..., help="Logical source name to explain."),
    config: Path = typer.Option(Path("verixa.yaml"), help="Path to verixa.yaml."),
    output: OutputFormat = typer.Option(OutputFormat.TEXT, "--format", help="Output format."),
) -> None:
    """Show one source contract in a human-readable form."""

    try:
        payload = run_explain(config, source_name)
        typer.echo(_render_explain_output(payload, output))
    except (ConfigError, ConnectorError, StorageError, ValueError) as exc:
        _exit_with_error(str(exc), output)


@app.command("cost")
def cost_command(
    command: str = typer.Argument(
        "diff",
        help="Workflow step to estimate: snapshot, diff, validate, or check. Legacy aliases plan and test are also accepted.",
    ),
    config: Path = typer.Option(Path("verixa.yaml"), help="Path to verixa.yaml."),
    source: list[str] | None = typer.Option(
        None,
        "--source",
        help="Optionally limit estimates to one or more source names.",
    ),
    output: OutputFormat = typer.Option(OutputFormat.TEXT, "--format", help="Output format."),
) -> None:
    """Estimate BigQuery bytes processed for one Verixa workflow step."""

    try:
        report = run_cost(config, command=command, source_names=tuple(source or ()))
        typer.echo(_render_cost_output(report, output))
    except (ConfigError, ConnectorError, StorageError, ValueError) as exc:
        _exit_with_error(str(exc), output)


def _run_diff_like_command(
    *,
    command_name: str,
    config: Path,
    risk_config: Path | None,
    source: list[str] | None,
    output: OutputFormat,
    estimate_bytes: bool,
) -> None:
    try:
        selected_sources = tuple(source or ())
        estimates = _estimate_bytes(config, selected_sources, command=command_name) if estimate_bytes else None
        result = run_diff(config, risk_path=risk_config, source_names=selected_sources)
        typer.echo(_render_diff_output(result, "Diff", output, estimates))
    except (ConfigError, ConnectorError, StorageError, ValueError) as exc:
        _exit_with_error(str(exc), output)



def _run_validate_like_command(
    *,
    command_name: str,
    config: Path,
    risk_config: Path | None,
    source: list[str] | None,
    output: OutputFormat,
    estimate_bytes: bool,
) -> None:
    try:
        selected_sources = tuple(source or ())
        estimates = _estimate_bytes(config, selected_sources, command=command_name) if estimate_bytes else None
        result = run_validate(config, risk_path=risk_config, source_names=selected_sources)
        typer.echo(_render_diff_output(result, "Validate", output, estimates))
    except (ConfigError, ConnectorError, StorageError, ValueError) as exc:
        _exit_with_error(str(exc), output)



def _render_snapshot_output(snapshot, baseline_path: Path, output: OutputFormat, estimated_bytes_by_source: dict[str, int] | None = None) -> str:
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



def _render_status_output(report: StatusReport, output: OutputFormat) -> str:
    payload: dict[str, Any] = {
        "config": {
            "path": str(report.config_path),
            "exists": report.config_exists,
            "error": report.config_error,
        },
        "baseline": {
            "path": str(report.baseline_path),
            "exists": report.baseline_exists,
            "age_seconds": report.baseline_age_seconds,
            "error": report.baseline_error,
        },
        "auth": {
            "ok": report.auth_ok,
            "message": report.auth_message,
        },
        "warehouse": report.warehouse_label,
        "sources": list(report.sources),
    }
    if output == OutputFormat.JSON:
        return json.dumps(payload, indent=2, sort_keys=True) + "\n"

    lines = ["Status"]
    config_line = f"- Config: {'OK' if report.config_exists and report.config_error is None else 'MISSING'} {report.config_path}"
    if report.config_error is not None and report.config_exists:
        config_line = f"- Config: ERROR {report.config_error}"
    lines.append(config_line)

    baseline_state = "OK" if report.baseline_exists and report.baseline_error is None else "MISSING"
    if report.baseline_error is not None:
        baseline_state = "ERROR"
    baseline_line = f"- Baseline: {baseline_state} {report.baseline_path}"
    if report.baseline_age_seconds is not None:
        baseline_line += f" ({_format_age(report.baseline_age_seconds)} old)"
    lines.append(baseline_line)

    if report.warehouse_label is not None:
        lines.append(f"- Warehouse: {report.warehouse_label}")
    if report.auth_ok is None:
        lines.append("- Auth: not checked")
    else:
        auth_state = "OK" if report.auth_ok else "ERROR"
        lines.append(f"- Auth: {auth_state} {report.auth_message}")
    if report.sources:
        lines.append(f"- Sources: {', '.join(report.sources)}")
    else:
        lines.append("- Sources: none")
    return "\n".join(lines)



def _render_explain_output(payload: dict[str, Any], output: OutputFormat) -> str:
    if output == OutputFormat.JSON:
        return json.dumps(payload, indent=2, sort_keys=True) + "\n"

    lines = [payload["source_name"]]
    lines.append(f"- Table: {payload['table']}")
    if payload["freshness"] is None:
        lines.append("- Freshness: none")
    else:
        lines.append(
            f"- Freshness: {payload['freshness']['column']} <= {payload['freshness']['max_age']}"
        )
    if payload["scan"] is None:
        lines.append("- Scan: full table")
    else:
        lines.append(
            f"- Scan: {payload['scan']['column']} ({payload['scan']['column_type']}) over {payload['scan']['lookback']}"
        )
    lines.append(f"- Warning policy: fail_on_warning={str(payload['check']['fail_on_warning']).lower()}")
    lines.append("Schema:")
    for column in payload["schema"]:
        lines.append(f"- {column['name']}: {column['type']}")
    lines.append("Tests:")
    if payload["tests"]:
        for test in payload["tests"]:
            if test["kind"] == "no_nulls":
                lines.append(f"- no_nulls: {test['column']}")
            else:
                lines.append(
                    f"- accepted_values: {test['column']} in [{', '.join(test['values'])}]"
                )
    else:
        lines.append("- none")
    lines.append("Thresholds:")
    lines.append(
        "- null_rate_change: "
        f"warning={payload['rules']['null_rate_change']['warning_delta']}, "
        f"error={payload['rules']['null_rate_change']['error_delta']}"
    )
    lines.append(
        "- row_count_change: "
        f"drop_warning={payload['rules']['row_count_change']['warning_drop_ratio']}, "
        f"drop_error={payload['rules']['row_count_change']['error_drop_ratio']}, "
        f"growth_warning={payload['rules']['row_count_change']['warning_growth_ratio']}, "
        f"growth_error={payload['rules']['row_count_change']['error_growth_ratio']}"
    )
    return "\n".join(lines)



def _render_cost_output(report: CostReport, output: OutputFormat) -> str:
    payload = {
        "command": report.command,
        "summary": {
            "sources_estimated": len(report.estimates),
            "total_bytes_processed": report.total_bytes,
        },
        "sources": [
            {"source_name": source_name, "estimated_bytes_processed": bytes_processed}
            for source_name, bytes_processed in sorted(report.estimates.items())
        ],
    }
    if output == OutputFormat.JSON:
        return json.dumps(payload, indent=2, sort_keys=True) + "\n"

    lines = [f"Cost {report.command}"]
    for source_name, bytes_processed in sorted(report.estimates.items()):
        lines.append(f"- {source_name}: {_format_bytes(bytes_processed)}")
    lines.append(f"Total: {_format_bytes(report.total_bytes)}")
    return "\n".join(lines)



def _exit_with_error(message: str, output: OutputFormat = OutputFormat.TEXT) -> None:
    if output == OutputFormat.JSON:
        typer.echo(render_error_json(message, RUNTIME_ERROR), err=True)
    else:
        typer.secho(f"Error: {message}", err=True, fg=typer.colors.RED)
    raise typer.Exit(code=RUNTIME_ERROR)



def _estimate_bytes(
    config_path: Path,
    source_names: tuple[str, ...],
    *,
    command: str,
) -> dict[str, int]:
    return run_cost(config_path, command=command, source_names=source_names).estimates



def _format_age(seconds: int) -> str:
    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, _ = divmod(remainder, 60)
    parts: list[str] = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes and len(parts) < 2:
        parts.append(f"{minutes}m")
    if not parts:
        parts.append("0m")
    return " ".join(parts[:2])



def _format_bytes(value: int) -> str:
    units = ("B", "KB", "MB", "GB", "TB")
    size = float(value)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{int(value)} B"



def main() -> None:
    """CLI entrypoint used by console scripts."""

    app()
