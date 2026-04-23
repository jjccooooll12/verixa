"""Typer application for the Verixa CLI."""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Callable

import typer

from verixa.cli.check import run_check as _run_check_impl
from verixa.cli.cost import CostReport, run_cost as _run_cost_impl
from verixa.cli.diff import run_diff as _run_diff_impl
from verixa.cli.doctor import run_doctor as _run_doctor_impl
from verixa.cli.explain import run_explain as _run_explain_impl
from verixa.cli.init import init_project as _init_project_impl
from verixa.cli.snapshot import run_snapshot as _run_snapshot_impl
from verixa.cli.status import StatusReport, run_status as _run_status_impl
from verixa.cli.validate import run_validate as _run_validate_impl
from verixa.config.errors import ConfigError
from verixa.connectors.base import ConnectorError
from verixa.contracts.normalize import parse_byte_size, parse_duration_to_seconds
from verixa.diff.models import DiffResult
from verixa.output.console import render_diff_result, render_snapshot_summary
from verixa.output.exit_codes import FINDINGS_ERROR, RUNTIME_ERROR, SUCCESS
from verixa.output.json import (
    render_diff_result_json,
    render_error_json,
    render_snapshot_summary_json,
)
from verixa.snapshot.models import ProjectSnapshot
from verixa.storage.filesystem import StorageError
from verixa.targeting import resolve_source_names as _resolve_source_names_impl


class OutputFormat(str, Enum):
    """Output formats supported by the CLI."""

    TEXT = "text"
    JSON = "json"


class InitWarehouse(str, Enum):
    """Starter warehouse templates supported by ``verixa init``."""

    BIGQUERY = "bigquery"
    SNOWFLAKE = "snowflake"


@dataclass(frozen=True, slots=True)
class AppDeps:
    """Explicit command dependencies used to build a CLI instance."""

    init_project: Callable[..., list[Path]]
    run_snapshot: Callable[..., tuple[ProjectSnapshot, Path]]
    run_diff: Callable[..., DiffResult]
    run_validate: Callable[..., DiffResult]
    run_check: Callable[..., DiffResult]
    run_status: Callable[..., StatusReport]
    run_doctor: Callable[..., DiffResult]
    run_explain: Callable[..., dict[str, Any]]
    run_cost: Callable[..., CostReport]
    estimate_bytes: Callable[..., dict[str, int]]
    resolve_source_names: Callable[..., tuple[str, ...]]


def _estimate_bytes_impl(
    config_path: Path,
    source_names: tuple[str, ...],
    *,
    command: str,
) -> dict[str, int]:
    return _run_cost_impl(
        config_path,
        command=command,
        source_names=source_names,
        mode="estimate",
    ).estimates


DEFAULT_APP_DEPS = AppDeps(
    init_project=_init_project_impl,
    run_snapshot=_run_snapshot_impl,
    run_diff=_run_diff_impl,
    run_validate=_run_validate_impl,
    run_check=_run_check_impl,
    run_status=_run_status_impl,
    run_doctor=_run_doctor_impl,
    run_explain=_run_explain_impl,
    run_cost=_run_cost_impl,
    estimate_bytes=_estimate_bytes_impl,
    resolve_source_names=_resolve_source_names_impl,
)


def create_app(deps: AppDeps | None = None) -> typer.Typer:
    """Build a Typer app with explicit command dependencies."""

    active_deps = deps or DEFAULT_APP_DEPS
    app = typer.Typer(add_completion=False, help="Verixa: developer-first Data CI for source contracts.")

    @app.command("init")
    def init_command(
        force: bool = typer.Option(False, help="Overwrite starter files if they already exist."),
        warehouse: InitWarehouse = typer.Option(
            InitWarehouse.BIGQUERY,
            "--warehouse",
            help="Starter warehouse template to write into verixa.yaml.",
        ),
    ) -> None:
        """Initialize a Verixa project."""

        try:
            created = active_deps.init_project(force=force, warehouse_kind=warehouse.value)
        except ValueError as exc:
            _exit_with_error(str(exc))
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
        environment: str | None = typer.Option(
            None,
            "--environment",
            help="Select a baseline environment when baseline.path uses {environment} or {env}. Falls back to VERIXA_ENV.",
        ),
        output: OutputFormat = typer.Option(
            OutputFormat.TEXT,
            "--format",
            help="Output format.",
        ),
        changed_file: list[str] | None = typer.Option(
            None,
            "--changed-file",
            help="Changed repo path used to auto-select sources. Repeat to select multiple.",
        ),
        changed_against: str | None = typer.Option(
            None,
            "--changed-against",
            help="Use git diff against this ref to auto-select sources from verixa.targets.yaml.",
        ),
        targets_config: Path | None = typer.Option(
            Path("verixa.targets.yaml"),
            "--targets-config",
            help="Optional path to changed-file targeting YAML.",
        ),
        estimate_bytes: bool = typer.Option(
            False,
            "--estimate-bytes",
            help="Estimate BigQuery bytes processed for the snapshot query shape and include it in output.",
        ),
        max_bytes_billed: str | None = typer.Option(
            None,
            "--max-bytes-billed",
            help="Cap live BigQuery query bytes for this run. Accepts values like 500MB or 1GB.",
        ),
    ) -> None:
        """Capture the current source baseline and store it locally."""

        try:
            selected_sources = _resolve_cli_source_names(
                deps=active_deps,
                config_path=config,
                explicit_sources=tuple(source or ()),
                changed_files=tuple(changed_file or ()),
                changed_against=changed_against,
                targets_path=targets_config,
                output=output,
            )
            parsed_max_bytes_billed = _parse_max_bytes_billed(max_bytes_billed)
            estimates = (
                active_deps.estimate_bytes(config, selected_sources, command="snapshot")
                if estimate_bytes
                else None
            )
            snapshot, baseline_path = active_deps.run_snapshot(
                config,
                source_names=selected_sources,
                environment=environment,
                max_bytes_billed=parsed_max_bytes_billed,
            )
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
        environment: str | None = typer.Option(
            None,
            "--environment",
            help="Select a baseline environment when baseline.path uses {environment} or {env}. Falls back to VERIXA_ENV.",
        ),
        output: OutputFormat = typer.Option(
            OutputFormat.TEXT,
            "--format",
            help="Output format.",
        ),
        changed_file: list[str] | None = typer.Option(
            None,
            "--changed-file",
            help="Changed repo path used to auto-select sources. Repeat to select multiple.",
        ),
        changed_against: str | None = typer.Option(
            None,
            "--changed-against",
            help="Use git diff against this ref to auto-select sources from verixa.targets.yaml.",
        ),
        targets_config: Path | None = typer.Option(
            Path("verixa.targets.yaml"),
            "--targets-config",
            help="Optional path to changed-file targeting YAML.",
        ),
        estimate_bytes: bool = typer.Option(
            False,
            "--estimate-bytes",
            help="Estimate BigQuery bytes processed for the diff query shape and include it in output.",
        ),
        max_bytes_billed: str | None = typer.Option(
            None,
            "--max-bytes-billed",
            help="Cap live BigQuery query bytes for this run. Accepts values like 500MB or 1GB.",
        ),
    ) -> None:
        """Show contract and baseline drift for current data."""

        _run_diff_like_command(
            deps=active_deps,
            command_name="diff",
            config=config,
            risk_config=risk_config,
            source=source,
            environment=environment,
            changed_file=changed_file,
            changed_against=changed_against,
            targets_config=targets_config,
            output=output,
            estimate_bytes=estimate_bytes,
            max_bytes_billed=max_bytes_billed,
        )

    @app.command("plan", hidden=True)
    def plan_alias_command(
        config: Path = typer.Option(Path("verixa.yaml"), help="Path to verixa.yaml."),
        risk_config: Path | None = typer.Option(Path("verixa.risk.yaml")),
        source: list[str] | None = typer.Option(None, "--source"),
        environment: str | None = typer.Option(None, "--environment"),
        output: OutputFormat = typer.Option(OutputFormat.TEXT, "--format"),
        changed_file: list[str] | None = typer.Option(None, "--changed-file"),
        changed_against: str | None = typer.Option(None, "--changed-against"),
        targets_config: Path | None = typer.Option(Path("verixa.targets.yaml"), "--targets-config"),
        estimate_bytes: bool = typer.Option(False, "--estimate-bytes"),
        max_bytes_billed: str | None = typer.Option(None, "--max-bytes-billed"),
    ) -> None:
        """Legacy alias for ``verixa diff``."""

        _run_diff_like_command(
            deps=active_deps,
            command_name="diff",
            config=config,
            risk_config=risk_config,
            source=source,
            environment=environment,
            changed_file=changed_file,
            changed_against=changed_against,
            targets_config=targets_config,
            output=output,
            estimate_bytes=estimate_bytes,
            max_bytes_billed=max_bytes_billed,
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
        changed_file: list[str] | None = typer.Option(
            None,
            "--changed-file",
            help="Changed repo path used to auto-select sources. Repeat to select multiple.",
        ),
        changed_against: str | None = typer.Option(
            None,
            "--changed-against",
            help="Use git diff against this ref to auto-select sources from verixa.targets.yaml.",
        ),
        targets_config: Path | None = typer.Option(
            Path("verixa.targets.yaml"),
            "--targets-config",
            help="Optional path to changed-file targeting YAML.",
        ),
        estimate_bytes: bool = typer.Option(
            False,
            "--estimate-bytes",
            help="Estimate BigQuery bytes processed for the validate query shape and include it in output.",
        ),
        max_bytes_billed: str | None = typer.Option(
            None,
            "--max-bytes-billed",
            help="Cap live BigQuery query bytes for this run. Accepts values like 500MB or 1GB.",
        ),
    ) -> None:
        """Run contract validation against current live data."""

        _run_validate_like_command(
            deps=active_deps,
            command_name="validate",
            config=config,
            risk_config=risk_config,
            source=source,
            changed_file=changed_file,
            changed_against=changed_against,
            targets_config=targets_config,
            output=output,
            estimate_bytes=estimate_bytes,
            max_bytes_billed=max_bytes_billed,
        )

    @app.command("test", hidden=True)
    def test_alias_command(
        config: Path = typer.Option(Path("verixa.yaml"), help="Path to verixa.yaml."),
        risk_config: Path | None = typer.Option(Path("verixa.risk.yaml")),
        source: list[str] | None = typer.Option(None, "--source"),
        output: OutputFormat = typer.Option(OutputFormat.TEXT, "--format"),
        changed_file: list[str] | None = typer.Option(None, "--changed-file"),
        changed_against: str | None = typer.Option(None, "--changed-against"),
        targets_config: Path | None = typer.Option(Path("verixa.targets.yaml"), "--targets-config"),
        estimate_bytes: bool = typer.Option(False, "--estimate-bytes"),
        max_bytes_billed: str | None = typer.Option(None, "--max-bytes-billed"),
    ) -> None:
        """Legacy alias for ``verixa validate``."""

        _run_validate_like_command(
            deps=active_deps,
            command_name="validate",
            config=config,
            risk_config=risk_config,
            source=source,
            changed_file=changed_file,
            changed_against=changed_against,
            targets_config=targets_config,
            output=output,
            estimate_bytes=estimate_bytes,
            max_bytes_billed=max_bytes_billed,
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
        environment: str | None = typer.Option(
            None,
            "--environment",
            help="Select a baseline environment when baseline.path uses {environment} or {env}. Falls back to VERIXA_ENV.",
        ),
        output: OutputFormat = typer.Option(
            OutputFormat.TEXT,
            "--format",
            help="Output format.",
        ),
        changed_file: list[str] | None = typer.Option(
            None,
            "--changed-file",
            help="Changed repo path used to auto-select sources. Repeat to select multiple.",
        ),
        changed_against: str | None = typer.Option(
            None,
            "--changed-against",
            help="Use git diff against this ref to auto-select sources from verixa.targets.yaml.",
        ),
        targets_config: Path | None = typer.Option(
            Path("verixa.targets.yaml"),
            "--targets-config",
            help="Optional path to changed-file targeting YAML.",
        ),
        estimate_bytes: bool = typer.Option(
            False,
            "--estimate-bytes",
            help="Estimate BigQuery bytes processed for the check query shape and include it in output.",
        ),
        max_bytes_billed: str | None = typer.Option(
            None,
            "--max-bytes-billed",
            help="Cap live BigQuery query bytes for this run. Accepts values like 500MB or 1GB.",
        ),
    ) -> None:
        """Run the CI-friendly validation path."""

        try:
            selected_sources = _resolve_cli_source_names(
                deps=active_deps,
                config_path=config,
                explicit_sources=tuple(source or ()),
                changed_files=tuple(changed_file or ()),
                changed_against=changed_against,
                targets_path=targets_config,
                output=output,
            )
            parsed_max_bytes_billed = _parse_max_bytes_billed(max_bytes_billed)
            estimates = (
                active_deps.estimate_bytes(config, selected_sources, command="check")
                if estimate_bytes
                else None
            )
            result = active_deps.run_check(
                config,
                risk_path=risk_config,
                source_names=selected_sources,
                environment=environment,
                max_bytes_billed=parsed_max_bytes_billed,
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
        environment: str | None = typer.Option(
            None,
            "--environment",
            help="Select a baseline environment when baseline.path uses {environment} or {env}. Falls back to VERIXA_ENV.",
        ),
        output: OutputFormat = typer.Option(OutputFormat.TEXT, "--format", help="Output format."),
    ) -> None:
        """Show config, baseline, auth, and source status."""

        try:
            report = active_deps.run_status(
                config,
                source_names=tuple(source or ()),
                environment=environment,
            )
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
        environment: str | None = typer.Option(
            None,
            "--environment",
            help="Select a baseline environment when baseline.path uses {environment} or {env}. Falls back to VERIXA_ENV.",
        ),
        output: OutputFormat = typer.Option(OutputFormat.TEXT, "--format", help="Output format."),
    ) -> None:
        """Run diagnostics for config, auth, baseline, and source access."""

        try:
            result = active_deps.run_doctor(
                config,
                source_names=tuple(source or ()),
                environment=environment,
            )
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
            payload = active_deps.run_explain(config, source_name)
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
        changed_file: list[str] | None = typer.Option(
            None,
            "--changed-file",
            help="Changed repo path used to auto-select sources. Repeat to select multiple.",
        ),
        changed_against: str | None = typer.Option(
            None,
            "--changed-against",
            help="Use git diff against this ref to auto-select sources from verixa.targets.yaml.",
        ),
        targets_config: Path | None = typer.Option(
            Path("verixa.targets.yaml"),
            "--targets-config",
            help="Optional path to changed-file targeting YAML.",
        ),
        max_bytes_billed: str | None = typer.Option(
            None,
            "--max-bytes-billed",
            help="Compare estimates against a byte ceiling. Accepts values like 500MB or 1GB.",
        ),
        history_window: str | None = typer.Option(
            None,
            "--history-window",
            help="For Snowflake, report recent warehouse usage within this lookback window, for example 30m or 1h.",
        ),
        output: OutputFormat = typer.Option(OutputFormat.TEXT, "--format", help="Output format."),
    ) -> None:
        """Estimate BigQuery bytes or report recent Snowflake usage for one workflow step."""

        try:
            selected_sources = _resolve_cli_source_names(
                deps=active_deps,
                config_path=config,
                explicit_sources=tuple(source or ()),
                changed_files=tuple(changed_file or ()),
                changed_against=changed_against,
                targets_path=targets_config,
                output=output,
            )
            report = active_deps.run_cost(
                config,
                command=command,
                source_names=selected_sources,
                max_bytes_billed=_parse_max_bytes_billed(max_bytes_billed),
                history_window_seconds=_parse_history_window(history_window),
            )
            typer.echo(_render_cost_output(report, output))
        except (ConfigError, ConnectorError, StorageError, ValueError) as exc:
            _exit_with_error(str(exc), output)

    return app


def _run_diff_like_command(
    *,
    deps: AppDeps,
    command_name: str,
    config: Path,
    risk_config: Path | None,
    source: list[str] | None,
    environment: str | None,
    changed_file: list[str] | None,
    changed_against: str | None,
    targets_config: Path | None,
    output: OutputFormat,
    estimate_bytes: bool,
    max_bytes_billed: str | None,
) -> None:
    try:
        selected_sources = _resolve_cli_source_names(
            deps=deps,
            config_path=config,
            explicit_sources=tuple(source or ()),
            changed_files=tuple(changed_file or ()),
            changed_against=changed_against,
            targets_path=targets_config,
            output=output,
        )
        parsed_max_bytes_billed = _parse_max_bytes_billed(max_bytes_billed)
        estimates = (
            deps.estimate_bytes(config, selected_sources, command=command_name)
            if estimate_bytes
            else None
        )
        result = deps.run_diff(
            config,
            risk_path=risk_config,
            source_names=selected_sources,
            environment=environment,
            max_bytes_billed=parsed_max_bytes_billed,
        )
        typer.echo(_render_diff_output(result, "Diff", output, estimates))
    except (ConfigError, ConnectorError, StorageError, ValueError) as exc:
        _exit_with_error(str(exc), output)


def _run_validate_like_command(
    *,
    deps: AppDeps,
    command_name: str,
    config: Path,
    risk_config: Path | None,
    source: list[str] | None,
    changed_file: list[str] | None,
    changed_against: str | None,
    targets_config: Path | None,
    output: OutputFormat,
    estimate_bytes: bool,
    max_bytes_billed: str | None,
) -> None:
    try:
        selected_sources = _resolve_cli_source_names(
            deps=deps,
            config_path=config,
            explicit_sources=tuple(source or ()),
            changed_files=tuple(changed_file or ()),
            changed_against=changed_against,
            targets_path=targets_config,
            output=output,
        )
        parsed_max_bytes_billed = _parse_max_bytes_billed(max_bytes_billed)
        estimates = (
            deps.estimate_bytes(config, selected_sources, command=command_name)
            if estimate_bytes
            else None
        )
        result = deps.run_validate(
            config,
            risk_path=risk_config,
            source_names=selected_sources,
            max_bytes_billed=parsed_max_bytes_billed,
        )
        typer.echo(_render_diff_output(result, "Validate", output, estimates))
    except (ConfigError, ConnectorError, StorageError, ValueError) as exc:
        _exit_with_error(str(exc), output)



def _resolve_cli_source_names(
    *,
    deps: AppDeps,
    config_path: Path,
    explicit_sources: tuple[str, ...],
    changed_files: tuple[str, ...],
    changed_against: str | None,
    targets_path: Path | None,
    output: OutputFormat,
) -> tuple[str, ...]:
    try:
        return deps.resolve_source_names(
            config_path,
            explicit_source_names=explicit_sources,
            changed_files=changed_files,
            changed_against=changed_against,
            targets_path=targets_path,
        )
    except (ConfigError, ConnectorError, StorageError, ValueError) as exc:
        _exit_with_error(str(exc), output)


def _render_snapshot_output(
    snapshot: ProjectSnapshot,
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


def _render_status_output(report: StatusReport, output: OutputFormat) -> str:
    payload: dict[str, Any] = {
        "config": {
            "path": str(report.config_path),
            "exists": report.config_exists,
            "error": report.config_error,
        },
        "environment": report.environment,
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
        "warehouse_max_bytes_billed": report.warehouse_max_bytes_billed,
        "sources": list(report.sources),
    }
    if output == OutputFormat.JSON:
        return json.dumps(payload, indent=2, sort_keys=True) + "\n"

    lines = ["Status"]
    config_line = f"- Config: {'OK' if report.config_exists and report.config_error is None else 'MISSING'} {report.config_path}"
    if report.config_error is not None and report.config_exists:
        config_line = f"- Config: ERROR {report.config_error}"
    lines.append(config_line)

    if report.environment is None:
        lines.append("- Environment: default")
    else:
        lines.append(f"- Environment: {report.environment}")

    baseline_state = "OK" if report.baseline_exists and report.baseline_error is None else "MISSING"
    if report.baseline_error is not None:
        baseline_state = "ERROR"
    baseline_line = f"- Baseline: {baseline_state} {report.baseline_path}"
    if report.baseline_age_seconds is not None:
        baseline_line += f" ({_format_age(report.baseline_age_seconds)} old)"
    lines.append(baseline_line)

    if report.warehouse_label is not None:
        lines.append(f"- Warehouse: {report.warehouse_label}")
    if report.warehouse_max_bytes_billed is None:
        lines.append("- Max bytes billed: none")
    else:
        lines.append(f"- Max bytes billed: {_format_bytes(report.warehouse_max_bytes_billed)}")
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
    lines.append(f"- Warehouse: {_format_explain_warehouse(payload['warehouse'])}")
    if payload["warehouse"]["max_bytes_billed"] is None:
        lines.append("- Max bytes billed: none")
    else:
        lines.append(
            f"- Max bytes billed: {_format_bytes(payload['warehouse']['max_bytes_billed'])}"
        )
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
    lines.append(
        "- numeric_distribution_change: "
        f"warning={payload['rules']['numeric_distribution_change']['warning_relative_delta']}, "
        f"error={payload['rules']['numeric_distribution_change']['error_relative_delta']}, "
        f"baseline_min={payload['rules']['numeric_distribution_change']['minimum_baseline_value']}"
    )
    return "\n".join(lines)


def _format_explain_warehouse(warehouse: dict[str, Any]) -> str:
    kind = warehouse["kind"]
    if kind == "bigquery":
        parts = []
        if warehouse.get("project") is not None:
            parts.append(f"project={warehouse['project']}")
        if warehouse.get("location") is not None:
            parts.append(f"location={warehouse['location']}")
        return f"bigquery ({', '.join(parts)})" if parts else "bigquery"

    if kind == "snowflake":
        parts = []
        for key in (
            "connection_name",
            "account",
            "user",
            "warehouse_name",
            "database",
            "schema",
            "role",
            "authenticator",
        ):
            value = warehouse.get(key)
            if value is not None:
                parts.append(f"{key}={value}")
        return f"snowflake ({', '.join(parts)})" if parts else "snowflake"

    return str(kind)


def _render_cost_output(report: CostReport, output: OutputFormat) -> str:
    if report.mode == "history":
        payload = {
            "command": report.command,
            "mode": report.mode,
            "query_tag": report.query_tag,
            "history_window_seconds": report.history_window_seconds,
            "summary": {
                "queries_found": len(report.usage_records),
                "total_bytes_scanned": report.total_bytes,
                "total_bytes_written": report.total_bytes_written,
                "total_elapsed_ms": report.total_elapsed_ms,
            },
            "queries": [
                {
                    "query_id": record.query_id,
                    "query_tag": record.query_tag,
                    "warehouse_name": record.warehouse_name,
                    "start_time": (
                        record.start_time.isoformat().replace("+00:00", "Z")
                        if record.start_time is not None
                        else None
                    ),
                    "total_elapsed_ms": record.total_elapsed_ms,
                    "bytes_scanned": record.bytes_scanned,
                    "bytes_written": record.bytes_written,
                    "rows_produced": record.rows_produced,
                }
                for record in report.usage_records
            ],
        }
        if output == OutputFormat.JSON:
            return json.dumps(payload, indent=2, sort_keys=True) + "\n"

        lines = [f"Cost {report.command}", "Mode: recent Snowflake usage"]
        if report.query_tag is not None:
            lines.append(f"Query tag: {report.query_tag}")
        if report.history_window_seconds is not None:
            lines.append(f"Window: {_format_age(report.history_window_seconds)}")
        if not report.usage_records:
            lines.append("Queries: none found")
            return "\n".join(lines)
        for record in report.usage_records:
            details = [
                f"query_id={record.query_id}",
                f"scanned={_format_bytes(record.bytes_scanned or 0)}",
            ]
            if record.warehouse_name is not None:
                details.append(f"warehouse={record.warehouse_name}")
            if record.total_elapsed_ms is not None:
                details.append(f"elapsed={record.total_elapsed_ms}ms")
            if record.start_time is not None:
                details.append(
                    f"started={record.start_time.isoformat().replace('+00:00', 'Z')}"
                )
            lines.append(f"- {' '.join(details)}")
        lines.append(f"Total scanned: {_format_bytes(report.total_bytes)}")
        lines.append(f"Total written: {_format_bytes(report.total_bytes_written)}")
        lines.append(f"Total elapsed: {report.total_elapsed_ms}ms")
        return "\n".join(lines)

    payload = {
        "command": report.command,
        "mode": report.mode,
        "summary": {
            "sources_estimated": len(report.estimates),
            "total_bytes_processed": report.total_bytes,
            "max_bytes_billed": report.max_bytes_billed,
            "has_over_limit_sources": bool(report.over_limit_sources),
            "over_limit_sources": list(report.over_limit_sources),
        },
        "sources": [
            {
                "source_name": source_name,
                "estimated_bytes_processed": bytes_processed,
                "over_max_bytes_billed": (
                    report.max_bytes_billed is not None and bytes_processed > report.max_bytes_billed
                ),
            }
            for source_name, bytes_processed in sorted(report.estimates.items())
        ],
    }
    if output == OutputFormat.JSON:
        return json.dumps(payload, indent=2, sort_keys=True) + "\n"

    lines = [f"Cost {report.command}"]
    for source_name, bytes_processed in sorted(report.estimates.items()):
        suffix = ""
        if report.max_bytes_billed is not None and bytes_processed > report.max_bytes_billed:
            suffix = " (over limit)"
        lines.append(f"- {source_name}: {_format_bytes(bytes_processed)}{suffix}")
    lines.append(f"Total: {_format_bytes(report.total_bytes)}")
    if report.max_bytes_billed is None:
        lines.append("Max bytes billed: none")
    else:
        lines.append(f"Max bytes billed: {_format_bytes(report.max_bytes_billed)}")
    if report.over_limit_sources:
        lines.append(f"Would exceed limit: {', '.join(report.over_limit_sources)}")
    return "\n".join(lines)


def _exit_with_error(message: str, output: OutputFormat = OutputFormat.TEXT) -> None:
    if output == OutputFormat.JSON:
        typer.echo(render_error_json(message, RUNTIME_ERROR), err=True)
    else:
        typer.secho(f"Error: {message}", err=True, fg=typer.colors.RED)
    raise typer.Exit(code=RUNTIME_ERROR)


def _parse_max_bytes_billed(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return parse_byte_size(value)
    except ValueError as exc:
        raise ValueError(f"Invalid --max-bytes-billed value '{value}': {exc}") from exc


def _parse_history_window(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return parse_duration_to_seconds(value)
    except ValueError as exc:
        raise ValueError(f"Invalid --history-window value '{value}': {exc}") from exc


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


app = create_app()


def main() -> None:
    """CLI entrypoint used by console scripts."""

    app()
