"""Implementation of ``verixa doctor``."""

from __future__ import annotations

from pathlib import Path

from verixa.config.errors import ConfigError
from verixa.config.loader import load_config, resolve_config_path
from verixa.connectors.base import ConnectorError
from verixa.connectors.bigquery.connector import BigQueryConnector
from verixa.diff.models import DiffResult, Finding
from verixa.storage.filesystem import SnapshotStore, StorageError


def run_doctor(
    config_path: Path | None = None,
    *,
    source_names: tuple[str, ...] = (),
) -> DiffResult:
    findings: list[Finding] = []
    resolved_config_path = resolve_config_path(config_path)
    store = SnapshotStore()

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
        config = load_config(resolved_config_path, source_names=source_names)
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

    if not store.baseline_exists():
        findings.append(
            Finding(
                source_name="baseline",
                severity="warning",
                code="baseline_missing",
                message="baseline missing: run 'verixa snapshot' before 'verixa diff' or 'verixa check'",
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

    connector = BigQueryConnector(config.warehouse)
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

    for source_name in sorted(config.sources):
        source = config.sources[source_name]
        ok, message = connector.check_source_access(source)
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
