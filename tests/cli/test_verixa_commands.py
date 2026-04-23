from __future__ import annotations

from typer.testing import CliRunner

from tests.cli.support import build_app
from verixa.cli.cost import CostReport
from verixa.cli.status import StatusReport
from verixa.diff.models import DiffResult, Finding


def test_diff_command_uses_diff_title() -> None:
    runner = CliRunner()
    app = build_app(
        run_diff=lambda config, risk_path=None, source_names=(), environment=None, max_bytes_billed=None: DiffResult(
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
        )
    )

    result = runner.invoke(app, ["diff"])

    assert result.exit_code == 0
    assert "diff output" in result.stdout


def test_validate_command_uses_validate_title() -> None:
    runner = CliRunner()
    app = build_app(
        run_validate=lambda config, risk_path=None, source_names=(), max_bytes_billed=None: DiffResult(
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
        )
    )

    result = runner.invoke(app, ["validate"])

    assert result.exit_code == 0
    assert "validate output" in result.stdout


def test_plan_alias_maps_to_diff() -> None:
    runner = CliRunner()
    seen: dict[str, object] = {}

    def _run_diff(config, risk_path=None, source_names=(), environment=None, max_bytes_billed=None):  # noqa: ANN001
        seen["called"] = True
        seen["environment"] = environment
        return DiffResult(findings=(), sources_checked=1, used_baseline=True)

    app = build_app(run_diff=_run_diff)
    result = runner.invoke(app, ["plan"])

    assert result.exit_code == 0
    assert seen["called"] is True
    assert seen["environment"] is None


def test_test_alias_maps_to_validate() -> None:
    runner = CliRunner()
    seen: dict[str, object] = {}

    def _run_validate(config, risk_path=None, source_names=(), max_bytes_billed=None):  # noqa: ANN001
        seen["called"] = True
        return DiffResult(findings=(), sources_checked=1, used_baseline=False)

    app = build_app(run_validate=_run_validate)
    result = runner.invoke(app, ["test"])

    assert result.exit_code == 0
    assert seen["called"] is True


def test_status_command_renders_text() -> None:
    runner = CliRunner()
    app = build_app(
        run_status=lambda config, source_names=(), environment=None: StatusReport(
            config_path=config,
            config_exists=True,
            config_error=None,
            environment=environment,
            baseline_path=config.parent / ".verixa" / "baseline.json",
            baseline_exists=True,
            baseline_age_seconds=3600,
            baseline_stale=False,
            baseline_warning_age_seconds=7 * 24 * 3600,
            baseline_state="ok",
            baseline_remediation=None,
            baseline_error=None,
            auth_ok=True,
            auth_message="authenticated",
            warehouse_label="bigquery (demo)",
            warehouse_max_bytes_billed=500 * 1024 * 1024,
            sources=("stripe.transactions",),
        )
    )

    result = runner.invoke(app, ["status"])

    assert result.exit_code == 0
    assert "Status" in result.stdout
    assert "Environment: default" in result.stdout
    assert "bigquery (demo)" in result.stdout


def test_doctor_command_fails_on_errors() -> None:
    runner = CliRunner()
    app = build_app(
        run_doctor=lambda config, source_names=(), environment=None: DiffResult(
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
        )
    )

    result = runner.invoke(app, ["doctor"])

    assert result.exit_code == 1
    assert "doctor error" in result.stdout


def test_explain_command_renders_text() -> None:
    runner = CliRunner()
    app = build_app(
        run_explain=lambda config, source_name: {
            "source_name": source_name,
            "table": "raw.stripe_transactions",
            "warehouse": {
                "kind": "bigquery",
                "project": "demo",
                "location": "US",
                "max_bytes_billed": 500 * 1024 * 1024,
            },
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
                "numeric_distribution_change": {
                    "warning_relative_delta": 0.25,
                    "error_relative_delta": 0.5,
                    "minimum_baseline_value": 1.0,
                },
            },
            "tests": [{"kind": "no_nulls", "column": "amount"}],
        }
    )

    result = runner.invoke(app, ["explain", "stripe.transactions"])

    assert result.exit_code == 0
    assert "raw.stripe_transactions" in result.stdout
    assert "no_nulls: amount" in result.stdout
    assert "numeric_distribution_change" in result.stdout


def test_explain_command_renders_snowflake_warehouse_details() -> None:
    runner = CliRunner()
    app = build_app(
        run_explain=lambda config, source_name: {
            "source_name": source_name,
            "table": "RAW.INGEST.ORDERS",
            "warehouse": {
                "kind": "snowflake",
                "project": None,
                "location": None,
                "max_bytes_billed": None,
                "account": "xy12345.us-east-1",
                "user": "analyst",
                "warehouse_name": "ANALYTICS",
                "database": "RAW",
                "schema": "INGEST",
                "role": "TRANSFORMER",
                "authenticator": "externalbrowser",
                "connection_name": None,
            },
            "schema": [{"name": "amount", "type": "FLOAT64"}],
            "freshness": None,
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
                "numeric_distribution_change": {
                    "warning_relative_delta": 0.25,
                    "error_relative_delta": 0.5,
                    "minimum_baseline_value": 1.0,
                },
            },
            "tests": [],
        }
    )

    result = runner.invoke(app, ["explain", "stripe.transactions"])

    assert result.exit_code == 0
    assert "Warehouse: snowflake" in result.stdout
    assert "account=xy12345.us-east-1" in result.stdout
    assert "warehouse_name=ANALYTICS" in result.stdout


def test_cost_command_renders_json() -> None:
    runner = CliRunner()
    app = build_app(
        run_cost=lambda config, command, source_names=(), max_bytes_billed=None, history_window_seconds=None: CostReport(
            command="diff",
            mode="estimate",
            estimates={"stripe.transactions": 2048},
            max_bytes_billed=max_bytes_billed,
        )
    )

    result = runner.invoke(app, ["cost", "diff", "--format", "json", "--max-bytes-billed", "1KB"])

    assert result.exit_code == 0
    assert '"command": "diff"' in result.stdout
    assert '"total_bytes_processed": 2048' in result.stdout
    assert '"max_bytes_billed": 1024' in result.stdout
    assert '"has_over_limit_sources": true' in result.stdout


def test_diff_command_renders_policy_v1() -> None:
    runner = CliRunner()
    app = build_app(
        run_diff=lambda config, risk_path=None, source_names=(), environment=None, max_bytes_billed=None: DiffResult(
            findings=(
                Finding(
                    source_name="stripe.transactions",
                    severity="error",
                    code="no_nulls_violation",
                    message="no_nulls violated on amount",
                    column="amount",
                ),
            ),
            sources_checked=1,
            used_baseline=True,
        )
    )

    result = runner.invoke(app, ["diff", "--format", "policy-v1"])

    assert result.exit_code == 0
    assert '"schema_version": "verixa.policy.v1"' in result.stdout
    assert '"schema_version": "verixa.finding.v2"' in result.stdout


def test_diff_command_renders_github_markdown() -> None:
    runner = CliRunner()
    app = build_app(
        run_diff=lambda config, risk_path=None, source_names=(), environment=None, max_bytes_billed=None: DiffResult(
            findings=(
                Finding(
                    source_name="stripe.transactions",
                    severity="error",
                    code="accepted_values_violation",
                    message="accepted values violated on currency",
                    column="currency",
                ),
            ),
            sources_checked=1,
            used_baseline=True,
        )
    )

    result = runner.invoke(app, ["diff", "--format", "github-markdown"])

    assert result.exit_code == 0
    assert "# Verixa Diff" in result.stdout
    assert "`contract.accepted_values_violation` `currency`" in result.stdout


def test_diff_command_renders_github_annotations() -> None:
    runner = CliRunner()
    app = build_app(
        run_diff=lambda config, risk_path=None, source_names=(), environment=None, max_bytes_billed=None: DiffResult(
            findings=(
                Finding(
                    source_name="stripe.transactions",
                    severity="error",
                    code="accepted_values_violation",
                    message="accepted values violated on currency",
                    column="currency",
                ),
            ),
            sources_checked=1,
            used_baseline=True,
        )
    )

    result = runner.invoke(app, ["diff", "--format", "github-annotations"])

    assert result.exit_code == 0
    assert '"annotation_level": "failure"' in result.stdout
    assert '"title": "Verixa Diff: contract.accepted_values_violation"' in result.stdout


def test_cost_command_passes_history_window() -> None:
    runner = CliRunner()
    seen: dict[str, object] = {}

    def _run_cost(  # noqa: ANN001
        config,
        command,
        source_names=(),
        max_bytes_billed=None,
        history_window_seconds=None,
    ):
        seen["command"] = command
        seen["history_window_seconds"] = history_window_seconds
        return CostReport(
            command="diff",
            mode="history",
            usage_records=(),
            query_tag="verixa:diff",
            history_window_seconds=history_window_seconds,
        )

    app = build_app(run_cost=_run_cost)
    result = runner.invoke(app, ["cost", "diff", "--history-window", "30m"])

    assert result.exit_code == 0
    assert seen["command"] == "diff"
    assert seen["history_window_seconds"] == 1800
    assert "recent Snowflake usage" in result.stdout


def test_diff_command_can_target_sources_from_changed_files() -> None:
    runner = CliRunner()
    seen: dict[str, object] = {}

    def _resolve_source_names(
        config_path,
        *,
        explicit_source_names=(),
        changed_files=(),
        changed_against=None,
        targets_path=None,
    ):
        seen["explicit_source_names"] = explicit_source_names
        seen["changed_files"] = changed_files
        seen["changed_against"] = changed_against
        seen["targets_path"] = targets_path
        return ("stripe.transactions",)

    def _run_diff(config, risk_path=None, source_names=(), environment=None, max_bytes_billed=None):  # noqa: ANN001
        seen["resolved_source_names"] = source_names
        seen["environment"] = environment
        return DiffResult(findings=(), sources_checked=1, used_baseline=True)

    app = build_app(resolve_source_names=_resolve_source_names, run_diff=_run_diff)
    result = runner.invoke(
        app,
        ["diff", "--changed-file", "models/staging/stripe/orders.sql"],
    )

    assert result.exit_code == 0
    assert seen["explicit_source_names"] == ()
    assert seen["changed_files"] == ("models/staging/stripe/orders.sql",)
    assert seen["changed_against"] is None
    assert str(seen["targets_path"]) == "verixa.targets.yaml"
    assert seen["resolved_source_names"] == ("stripe.transactions",)
    assert seen["environment"] is None


def test_diff_command_passes_environment() -> None:
    runner = CliRunner()
    seen: dict[str, object] = {}

    def _run_diff(config, risk_path=None, source_names=(), environment=None, max_bytes_billed=None):  # noqa: ANN001
        seen["environment"] = environment
        return DiffResult(findings=(), sources_checked=1, used_baseline=True)

    app = build_app(run_diff=_run_diff)
    result = runner.invoke(app, ["diff", "--environment", "prod"])

    assert result.exit_code == 0
    assert seen["environment"] == "prod"
