"""Implementation of ``verixa doctor``."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from verixa.config.errors import ConfigError
from verixa.config.loader import load_config, resolve_config_path
from verixa.connectors.base import ConnectorError, WarehouseConnector
from verixa.connectors.factory import create_connector
from verixa.contracts.models import ProjectConfig
from verixa.diff.models import DiffResult, Finding
from verixa.storage.filesystem import (
    SnapshotStore,
    StorageError,
    baseline_path_requires_environment,
    create_snapshot_store,
    format_missing_baseline_message,
    resolve_environment_name,
)

ConfigLoader = Callable[..., ProjectConfig]
ConnectorFactory = Callable[..., WarehouseConnector]


def run_doctor(
    config_path: Path | None = None,
    *,
    source_names: tuple[str, ...] = (),
    environment: str | None = None,
    config_loader: ConfigLoader = load_config,
    connector_factory: ConnectorFactory = create_connector,
) -> DiffResult:
    findings: list[Finding] = []
    resolved_config_path = resolve_config_path(config_path)
    store = SnapshotStore()
    active_environment = resolve_environment_name(environment)

    if not resolved_config_path.exists():
        findings.append(
            Finding(
                source_name="config",
                severity="error",
                code="config_missing",
                message=f"config missing: {resolved_config_path}",
            )
        )
        return DiffResult(findings=tuple(findings), sources_checked=0, used_baseline=False)

    try:
        config = config_loader(resolved_config_path, source_names=source_names)
    except ConfigError as exc:
        findings.append(
            Finding(
                source_name="config",
                severity="error",
                code="config_invalid",
                message=str(exc),
            )
        )
        return DiffResult(findings=tuple(findings), sources_checked=0, used_baseline=False)

    try:
        store = create_snapshot_store(
            config.baseline.path,
            environment=active_environment,
        )
    except StorageError as exc:
        findings.append(
            Finding(
                source_name="baseline",
                severity="error",
                code="baseline_path_invalid",
                message=str(exc),
            )
        )
        return DiffResult(findings=tuple(findings), sources_checked=0, used_baseline=False)

    if not store.baseline_exists():
        missing_for_environment = (
            active_environment is not None
            and baseline_path_requires_environment(config.baseline.path)
        )
        findings.append(
            Finding(
                source_name="baseline",
                severity="error" if missing_for_environment else "warning",
                code="baseline_missing_for_environment" if missing_for_environment else "baseline_missing",
                message=format_missing_baseline_message(
                    store.baseline_path,
                    environment=active_environment,
                    path_template=config.baseline.path,
                ),
            )
        )
    else:
        try:
            store.read_baseline()
        except StorageError as exc:
            findings.append(
                Finding(
                    source_name="baseline",
                    severity="error",
                    code="baseline_unreadable",
                    message=str(exc),
                )
            )

    connector = connector_factory(config.warehouse)
    try:
        auth_ok, auth_message = connector.check_auth()
    except ConnectorError as exc:
        findings.append(
            Finding(
                source_name="auth",
                severity="error",
                code="auth_check_failed",
                message=str(exc),
            )
        )
        return DiffResult(findings=tuple(findings), sources_checked=len(config.sources), used_baseline=store.baseline_exists())

    if not auth_ok:
        findings.append(
            Finding(
                source_name="auth",
                severity="error",
                code="auth_unusable",
                message=f"warehouse auth check failed: {auth_message}",
            )
        )
        return DiffResult(findings=tuple(findings), sources_checked=len(config.sources), used_baseline=store.baseline_exists())

    if hasattr(connector, "describe_runtime_environment"):
        findings.extend(_snowflake_runtime_findings(connector, config))

    for source_name in sorted(config.sources):
        source = config.sources[source_name]
        try:
            ok, message = connector.check_source_access(source)
        except Exception as exc:  # pragma: no cover - defensive path for diagnostics
            ok = False
            message = str(exc)
        if not ok:
            findings.append(
                Finding(
                    source_name=source_name,
                    severity="error",
                    code="source_unreachable",
                    message=f"cannot access source metadata: {message}",
                )
            )

    return DiffResult(
        findings=tuple(sorted(findings, key=lambda item: (item.source_name, item.code))),
        sources_checked=len(config.sources),
        used_baseline=store.baseline_exists(),
    )


def _snowflake_runtime_findings(
    connector: WarehouseConnector,
    config: ProjectConfig,
) -> list[Finding]:
    try:
        context = connector.describe_runtime_environment()
    except Exception as exc:  # pragma: no cover - exercised in tests through mocks
        return [
            Finding(
                source_name="warehouse",
                severity="error",
                code="snowflake_runtime_unavailable",
                message=f"could not describe Snowflake runtime environment: {exc}",
            )
        ]

    findings: list[Finding] = []
    if context.current_warehouse is None:
        findings.append(
            Finding(
                source_name="warehouse",
                severity="error",
                code="snowflake_warehouse_missing",
                message="Snowflake session has no active warehouse.",
            )
        )
    elif _normalized_match(config.warehouse.warehouse_name, context.current_warehouse) is False:
        findings.append(
            Finding(
                source_name="warehouse",
                severity="warning",
                code="snowflake_warehouse_mismatch",
                message=(
                    f"configured warehouse '{config.warehouse.warehouse_name}' does not match "
                    f"current warehouse '{context.current_warehouse}'"
                ),
            )
        )

    if not context.compute_ok:
        findings.append(
            Finding(
                source_name="warehouse",
                severity="error",
                code="snowflake_warehouse_unusable",
                message=(
                    "Snowflake compute check failed"
                    if context.compute_message is None
                    else f"Snowflake compute check failed: {context.compute_message}"
                ),
            )
        )

    if _normalized_match(config.warehouse.role, context.current_role) is False:
        findings.append(
            Finding(
                source_name="warehouse",
                severity="warning",
                code="snowflake_role_mismatch",
                message=(
                    f"configured role '{config.warehouse.role}' does not match "
                    f"current role '{context.current_role}'"
                ),
            )
        )

    if _normalized_match(config.warehouse.database, context.current_database) is False:
        findings.append(
            Finding(
                source_name="warehouse",
                severity="warning",
                code="snowflake_database_mismatch",
                message=(
                    f"configured database '{config.warehouse.database}' does not match "
                    f"current database '{context.current_database}'"
                ),
            )
        )

    if _normalized_match(config.warehouse.schema, context.current_schema) is False:
        findings.append(
            Finding(
                source_name="warehouse",
                severity="warning",
                code="snowflake_schema_mismatch",
                message=(
                    f"configured schema '{config.warehouse.schema}' does not match "
                    f"current schema '{context.current_schema}'"
                ),
            )
        )

    return findings


def _normalized_match(expected: str | None, actual: str | None) -> bool | None:
    if expected is None:
        return None
    if actual is None:
        return False
    return expected.upper() == actual.upper()
