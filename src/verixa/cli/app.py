"""Typer application for the Verixa CLI."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable

import typer

from verixa.baselines.models import BaselineProposal, BaselineStatusReport
from verixa.cli.baseline import (
    run_baseline_accept as _run_baseline_accept_impl,
    run_baseline_promote as _run_baseline_promote_impl,
    run_baseline_propose as _run_baseline_propose_impl,
    run_baseline_status as _run_baseline_status_impl,
)
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
from verixa.diff.models import DiffResult, Finding
from verixa.history.classifier import LifecycleReport, classify_finding_lifecycle
from verixa.history.store import HistoryStoreError, RunHistoryStore
from verixa.findings.schema import normalize_diff_result
from verixa.output.github_annotations import render_diff_result_github_annotations
from verixa.output.console import render_diff_result, render_snapshot_summary
from verixa.output.exit_codes import FINDINGS_ERROR, RUNTIME_ERROR, SUCCESS
from verixa.output.github_markdown import render_diff_result_github_markdown
from verixa.output.json import (
    render_diff_result_json,
    render_error_json,
    render_snapshot_summary_json,
)
from verixa.policy.export import render_diff_result_policy_v1
from verixa.snapshot.models import ProjectSnapshot
from verixa.storage.filesystem import StorageError
from verixa.suppressions import SuppressionError, apply_suppressions, load_suppressions
from verixa.suppressions.loader import split_active_and_expired
from verixa.targeting import resolve_source_names as _resolve_source_names_impl


class BasicOutputFormat(str, Enum):
    """Output formats supported by non-diff CLI commands."""

    TEXT = "text"
    JSON = "json"


class FindingOutputFormat(str, Enum):
    """Output formats supported by diff-like CLI commands."""

    TEXT = "text"
    JSON = "json"
    GITHUB_MARKDOWN = "github-markdown"
    GITHUB_ANNOTATIONS = "github-annotations"
    POLICY_V1 = "policy-v1"


class InitWarehouse(str, Enum):
    """Starter warehouse templates supported by ``verixa init``."""

    BIGQUERY = "bigquery"
    SNOWFLAKE = "snowflake"


@dataclass(frozen=True, slots=True)
class AppDeps:
    """Explicit command dependencies used to build a CLI instance."""

    init_project: Callable[..., list[Path]]
    run_snapshot: Callable[..., tuple[ProjectSnapshot, Path]]
    run_baseline_status: Callable[..., BaselineStatusReport]
    run_baseline_propose: Callable[..., BaselineProposal]
    run_baseline_promote: Callable[..., Path]
    run_baseline_accept: Callable[..., BaselineProposal]
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
    run_baseline_status=_run_baseline_status_impl,
    run_baseline_propose=_run_baseline_propose_impl,
    run_baseline_promote=_run_baseline_promote_impl,
    run_baseline_accept=_run_baseline_accept_impl,
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
    baseline_app = typer.Typer(help="Baseline lifecycle commands.")
    app.add_typer(baseline_app, name="baseline")

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
        output: BasicOutputFormat = typer.Option(
            BasicOutputFormat.TEXT,
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

    @baseline_app.command("status")
    def baseline_status_command(
        config: Path = typer.Option(Path("verixa.yaml"), help="Path to verixa.yaml."),
        environment: str = typer.Option(..., "--environment", help="Baseline environment to inspect."),
        output: BasicOutputFormat = typer.Option(BasicOutputFormat.TEXT, "--format", help="Output format."),
    ) -> None:
        """Show the promoted baseline state and pending proposals for one environment."""

        try:
            report = active_deps.run_baseline_status(
                config,
                environment=environment,
            )
            typer.echo(_render_baseline_status_output(report, output))
        except (ConfigError, ConnectorError, StorageError, ValueError) as exc:
            _exit_with_error(str(exc), output)

    @baseline_app.command("propose")
    def baseline_propose_command(
        config: Path = typer.Option(Path("verixa.yaml"), help="Path to verixa.yaml."),
        environment: str = typer.Option(..., "--environment", help="Target baseline environment."),
        reason: str = typer.Option(..., "--reason", help="Why this baseline change is expected."),
        source: list[str] | None = typer.Option(
            None,
            "--source",
            help="Limit the proposal to one or more source names. Repeat to select multiple.",
        ),
        max_bytes_billed: str | None = typer.Option(
            None,
            "--max-bytes-billed",
            help="Cap live BigQuery query bytes for this run. Accepts values like 500MB or 1GB.",
        ),
        output: BasicOutputFormat = typer.Option(BasicOutputFormat.TEXT, "--format", help="Output format."),
    ) -> None:
        """Capture a proposed baseline for later promotion."""

        try:
            proposal = active_deps.run_baseline_propose(
                config,
                environment=environment,
                reason=reason,
                source_names=tuple(source or ()),
                max_bytes_billed=_parse_max_bytes_billed(max_bytes_billed),
            )
            typer.echo(_render_baseline_proposal_output(proposal, output))
        except (ConfigError, ConnectorError, StorageError, ValueError) as exc:
            _exit_with_error(str(exc), output)

    @baseline_app.command("promote")
    def baseline_promote_command(
        config: Path = typer.Option(Path("verixa.yaml"), help="Path to verixa.yaml."),
        environment: str = typer.Option(..., "--environment", help="Target baseline environment."),
        proposal_id: str = typer.Option(..., "--proposal-id", help="Proposal id to promote."),
        output: BasicOutputFormat = typer.Option(BasicOutputFormat.TEXT, "--format", help="Output format."),
    ) -> None:
        """Promote a proposed baseline into the active environment baseline."""

        try:
            baseline_path = active_deps.run_baseline_promote(
                config,
                environment=environment,
                proposal_id=proposal_id,
            )
            typer.echo(_render_baseline_promote_output(proposal_id, baseline_path, output))
        except (ConfigError, ConnectorError, StorageError, ValueError) as exc:
            _exit_with_error(str(exc), output)

    @baseline_app.command("accept")
    def baseline_accept_command(
        config: Path = typer.Option(Path("verixa.yaml"), help="Path to verixa.yaml."),
        environment: str = typer.Option(..., "--environment", help="Target baseline environment."),
        reason: str = typer.Option(..., "--reason", help="Why this baseline change should be accepted."),
        source: list[str] | None = typer.Option(
            None,
            "--source",
            help="Limit the acceptance proposal to one or more source names. Repeat to select multiple.",
        ),
        max_bytes_billed: str | None = typer.Option(
            None,
            "--max-bytes-billed",
            help="Cap live BigQuery query bytes for this run. Accepts values like 500MB or 1GB.",
        ),
        output: BasicOutputFormat = typer.Option(BasicOutputFormat.TEXT, "--format", help="Output format."),
    ) -> None:
        """Capture an acceptance proposal for an expected baseline change."""

        try:
            proposal = active_deps.run_baseline_accept(
                config,
                environment=environment,
                reason=reason,
                source_names=tuple(source or ()),
                max_bytes_billed=_parse_max_bytes_billed(max_bytes_billed),
            )
            typer.echo(_render_baseline_proposal_output(proposal, output))
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
        output: FindingOutputFormat = typer.Option(
            FindingOutputFormat.TEXT,
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
        output: FindingOutputFormat = typer.Option(FindingOutputFormat.TEXT, "--format"),
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
        output: FindingOutputFormat = typer.Option(
            FindingOutputFormat.TEXT,
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
        output: FindingOutputFormat = typer.Option(FindingOutputFormat.TEXT, "--format"),
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
        output: FindingOutputFormat = typer.Option(
            FindingOutputFormat.TEXT,
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
            lifecycle_report = _build_lifecycle_report(
                result,
                command_name="check",
                environment=environment,
                estimated_bytes_by_source=estimates,
            )
            filtered_result, filtered_lifecycle = _apply_cli_suppressions(
                result,
                environment=environment,
                lifecycle_report=lifecycle_report,
                estimated_bytes_by_source=estimates,
            )
            typer.echo(
                _render_diff_output(
                    filtered_result,
                    "Check",
                    output,
                    estimates,
                    filtered_lifecycle,
                    environment=environment,
                )
            )
        except (ConfigError, ConnectorError, StorageError, SuppressionError, ValueError) as exc:
            _exit_with_error(str(exc), output)

        should_fail = False
        if fail_on_error and filtered_result.error_count > 0:
            should_fail = True
        if fail_on_warning and filtered_result.warning_count > 0:
            should_fail = True
        if filtered_result.warning_policy_failure_count > 0:
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
        output: BasicOutputFormat = typer.Option(BasicOutputFormat.TEXT, "--format", help="Output format."),
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
        output: FindingOutputFormat = typer.Option(FindingOutputFormat.TEXT, "--format", help="Output format."),
    ) -> None:
        """Run diagnostics for config, auth, baseline, and source access."""

        try:
            result = active_deps.run_doctor(
                config,
                source_names=tuple(source or ()),
                environment=environment,
            )
            if expired_findings := _expired_suppression_findings():
                result = DiffResult(
                    findings=tuple(
                        sorted(
                            result.findings + expired_findings,
                            key=lambda item: (item.source_name, item.code, item.column or ""),
                        )
                    ),
                    sources_checked=result.sources_checked,
                    used_baseline=result.used_baseline,
                    warning_policy_sources=result.warning_policy_sources,
                )
            lifecycle_report = _build_lifecycle_report(
                result,
                command_name="doctor",
                environment=environment,
            )
            typer.echo(
                _render_diff_output(
                    result,
                    "Doctor",
                    output,
                    lifecycle_report=lifecycle_report,
                    environment=environment,
                )
            )
        except (ConfigError, ConnectorError, StorageError, SuppressionError) as exc:
            _exit_with_error(str(exc), output)

        if result.error_count > 0:
            raise typer.Exit(code=FINDINGS_ERROR)
        raise typer.Exit(code=SUCCESS)

    @app.command("explain")
    def explain_command(
        source_name: str = typer.Argument(..., help="Logical source name to explain."),
        config: Path = typer.Option(Path("verixa.yaml"), help="Path to verixa.yaml."),
        output: BasicOutputFormat = typer.Option(BasicOutputFormat.TEXT, "--format", help="Output format."),
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
        output: BasicOutputFormat = typer.Option(BasicOutputFormat.TEXT, "--format", help="Output format."),
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
    output: FindingOutputFormat,
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
        lifecycle_report = _build_lifecycle_report(
            result,
            command_name=command_name,
            environment=environment,
            estimated_bytes_by_source=estimates,
        )
        filtered_result, filtered_lifecycle = _apply_cli_suppressions(
            result,
            environment=environment,
            lifecycle_report=lifecycle_report,
            estimated_bytes_by_source=estimates,
        )
        typer.echo(
            _render_diff_output(
                filtered_result,
                "Diff",
                output,
                estimates,
                filtered_lifecycle,
                environment=environment,
            )
        )
    except (ConfigError, ConnectorError, StorageError, SuppressionError, ValueError) as exc:
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
    output: FindingOutputFormat,
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
        lifecycle_report = _build_lifecycle_report(
            result,
            command_name=command_name,
            environment=None,
            estimated_bytes_by_source=estimates,
        )
        filtered_result, filtered_lifecycle = _apply_cli_suppressions(
            result,
            environment=None,
            lifecycle_report=lifecycle_report,
            estimated_bytes_by_source=estimates,
        )
        typer.echo(
            _render_diff_output(
                filtered_result,
                "Validate",
                output,
                estimates,
                filtered_lifecycle,
                environment=None,
            )
        )
    except (ConfigError, ConnectorError, StorageError, SuppressionError, ValueError) as exc:
        _exit_with_error(str(exc), output)



def _resolve_cli_source_names(
    *,
    deps: AppDeps,
    config_path: Path,
    explicit_sources: tuple[str, ...],
    changed_files: tuple[str, ...],
    changed_against: str | None,
    targets_path: Path | None,
    output: BasicOutputFormat | FindingOutputFormat,
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
    output: BasicOutputFormat,
    estimated_bytes_by_source: dict[str, int] | None = None,
) -> str:
    if output == BasicOutputFormat.JSON:
        return render_snapshot_summary_json(snapshot, baseline_path, estimated_bytes_by_source)
    return render_snapshot_summary(snapshot, baseline_path, estimated_bytes_by_source)


def _render_diff_output(
    result: DiffResult,
    title: str,
    output: FindingOutputFormat,
    estimated_bytes_by_source: dict[str, int] | None = None,
    lifecycle_report: LifecycleReport | None = None,
    environment: str | None = None,
) -> str:
    if output == FindingOutputFormat.JSON:
        return render_diff_result_json(
            result,
            title,
            estimated_bytes_by_source,
            lifecycle_report=lifecycle_report,
            environment=environment,
        )
    if output == FindingOutputFormat.POLICY_V1:
        return render_diff_result_policy_v1(
            result,
            title,
            estimated_bytes_by_source,
            lifecycle_report=lifecycle_report,
            environment=environment,
        )
    if output == FindingOutputFormat.GITHUB_MARKDOWN:
        return render_diff_result_github_markdown(
            result,
            title,
            estimated_bytes_by_source,
            lifecycle_report=lifecycle_report,
        )
    if output == FindingOutputFormat.GITHUB_ANNOTATIONS:
        return render_diff_result_github_annotations(
            result,
            title,
            estimated_bytes_by_source,
            lifecycle_report=lifecycle_report,
        )
    return render_diff_result(
        result,
        title=title,
        estimated_bytes_by_source=estimated_bytes_by_source,
        lifecycle_report=lifecycle_report,
    )


def _build_lifecycle_report(
    result: DiffResult,
    *,
    command_name: str,
    environment: str | None,
    estimated_bytes_by_source: dict[str, int] | None = None,
) -> LifecycleReport | None:
    try:
        normalized = normalize_diff_result(
            result,
            estimated_bytes_by_source=estimated_bytes_by_source,
        )
        store = RunHistoryStore()
        previous = store.read_last_run(command_name, environment=environment)
        lifecycle = classify_finding_lifecycle(
            normalized,
            () if previous is None else previous.findings,
        )
        store.write_run(
            command_name,
            lifecycle.active_findings,
            environment=environment,
        )
        return lifecycle
    except HistoryStoreError:
        return None


def _apply_cli_suppressions(
    result: DiffResult,
    *,
    environment: str | None,
    lifecycle_report: LifecycleReport | None,
    estimated_bytes_by_source: dict[str, int] | None = None,
) -> tuple[DiffResult, LifecycleReport | None]:
    rules = load_suppressions()
    if not rules:
        return result, lifecycle_report
    outcome = apply_suppressions(
        result,
        environment=environment,
        rules=rules,
        lifecycle_report=lifecycle_report,
        estimated_bytes_by_source=estimated_bytes_by_source,
    )
    return outcome.result, outcome.lifecycle_report


def _expired_suppression_findings() -> tuple[Finding, ...]:
    rules = load_suppressions()
    if not rules:
        return ()
    _, expired_rules = split_active_and_expired(rules, now=datetime.now(timezone.utc))
    findings = []
    for rule in expired_rules:
        findings.append(
            Finding(
                source_name="suppressions",
                severity="warning",
                code="suppression_expired",
                message=(
                    f"expired suppression for fingerprint {rule.fingerprint} owned by {rule.owner}; "
                    f"remove it or renew it with a new expiry. Reason: {rule.reason}"
                ),
            )
        )
    return tuple(findings)


def _render_status_output(report: StatusReport, output: BasicOutputFormat) -> str:
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
            "state": report.baseline_state,
            "stale": report.baseline_stale,
            "warning_age_seconds": report.baseline_warning_age_seconds,
            "remediation": report.baseline_remediation,
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
    if output == BasicOutputFormat.JSON:
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

    baseline_state = report.baseline_state.upper()
    baseline_line = f"- Baseline: {baseline_state} {report.baseline_path}"
    if report.baseline_age_seconds is not None:
        baseline_line += f" ({_format_age(report.baseline_age_seconds)} old)"
    lines.append(baseline_line)
    if report.baseline_remediation is not None:
        lines.append(f"- Baseline remediation: {report.baseline_remediation}")

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


def _render_baseline_status_output(
    report: BaselineStatusReport,
    output: BasicOutputFormat,
) -> str:
    payload = {
        "environment": report.environment,
        "baseline": {
            "path": str(report.baseline_path),
            "exists": report.baseline_exists,
            "age_seconds": report.baseline_age_seconds,
            "stale": report.baseline_stale,
            "warning_age_seconds": report.warning_age_seconds,
            "current_proposal_id": report.current_proposal_id,
            "current_reason": report.current_reason,
            "current_created_at": None
            if report.current_created_at is None
            else report.current_created_at.isoformat().replace("+00:00", "Z"),
            "current_created_by": report.current_created_by,
            "current_git_sha": report.current_git_sha,
            "current_promoted_at": None
            if report.current_promoted_at is None
            else report.current_promoted_at.isoformat().replace("+00:00", "Z"),
            "current_promoted_by": report.current_promoted_by,
            "remediation": report.remediation,
        },
        "pending_proposals": list(report.pending_proposals),
    }
    if output == BasicOutputFormat.JSON:
        return json.dumps(payload, indent=2, sort_keys=True) + "\n"

    lines = [f"Baseline {report.environment}"]
    state = "STALE" if report.baseline_stale else ("OK" if report.baseline_exists else "MISSING")
    line = f"- Current: {state} {report.baseline_path}"
    if report.baseline_age_seconds is not None:
        line += f" ({_format_age(report.baseline_age_seconds)} old)"
    lines.append(line)
    if report.current_proposal_id is not None:
        lines.append(f"- Current proposal: {report.current_proposal_id}")
    if report.current_reason is not None:
        lines.append(f"- Current reason: {report.current_reason}")
    if report.current_created_at is not None:
        lines.append(
            f"- Captured at: {report.current_created_at.isoformat().replace('+00:00', 'Z')}"
        )
    if report.current_created_by is not None:
        lines.append(f"- Captured by: {report.current_created_by}")
    if report.current_git_sha is not None:
        lines.append(f"- Git SHA: {report.current_git_sha}")
    if report.current_promoted_at is not None:
        lines.append(
            f"- Promoted at: {report.current_promoted_at.isoformat().replace('+00:00', 'Z')}"
        )
    if report.current_promoted_by is not None:
        lines.append(f"- Promoted by: {report.current_promoted_by}")
    if report.pending_proposals:
        lines.append(f"- Pending proposals: {', '.join(report.pending_proposals)}")
    else:
        lines.append("- Pending proposals: none")
    if report.remediation is not None:
        lines.append(f"- Remediation: {report.remediation}")
    return "\n".join(lines)


def _render_baseline_proposal_output(
    proposal: BaselineProposal,
    output: BasicOutputFormat,
) -> str:
    payload = {
        "proposal_id": proposal.proposal_id,
        "environment": proposal.environment,
        "reason": proposal.reason,
        "created_by": proposal.created_by,
        "git_sha": proposal.git_sha,
        "source_names": list(proposal.source_names),
        "baseline_path": str(proposal.baseline_path),
        "created_at": proposal.created_at.isoformat().replace("+00:00", "Z"),
    }
    if output == BasicOutputFormat.JSON:
        return json.dumps(payload, indent=2, sort_keys=True) + "\n"

    lines = [f"Created baseline proposal {proposal.proposal_id}"]
    lines.append(f"- Environment: {proposal.environment}")
    lines.append(f"- Reason: {proposal.reason}")
    if proposal.created_by is not None:
        lines.append(f"- Created by: {proposal.created_by}")
    if proposal.git_sha is not None:
        lines.append(f"- Git SHA: {proposal.git_sha}")
    lines.append(f"- Baseline path: {proposal.baseline_path}")
    if proposal.source_names:
        lines.append(f"- Sources: {', '.join(proposal.source_names)}")
    else:
        lines.append("- Sources: all")
    return "\n".join(lines)


def _render_baseline_promote_output(
    proposal_id: str,
    baseline_path: Path,
    output: BasicOutputFormat,
) -> str:
    payload = {
        "proposal_id": proposal_id,
        "baseline_path": str(baseline_path),
    }
    if output == BasicOutputFormat.JSON:
        return json.dumps(payload, indent=2, sort_keys=True) + "\n"

    return "\n".join(
        (
            f"Promoted baseline proposal {proposal_id}",
            f"- Baseline path: {baseline_path}",
        )
    )


def _render_explain_output(payload: dict[str, Any], output: BasicOutputFormat) -> str:
    if output == BasicOutputFormat.JSON:
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


def _render_cost_output(report: CostReport, output: BasicOutputFormat) -> str:
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
        if output == BasicOutputFormat.JSON:
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
    if output == BasicOutputFormat.JSON:
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


def _exit_with_error(
    message: str,
    output: BasicOutputFormat | FindingOutputFormat = BasicOutputFormat.TEXT,
) -> None:
    if output in {
        BasicOutputFormat.JSON,
        FindingOutputFormat.JSON,
        FindingOutputFormat.POLICY_V1,
        FindingOutputFormat.GITHUB_ANNOTATIONS,
    }:
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
