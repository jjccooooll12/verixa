"""Implementation of ``verixa status``."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from verixa.config.errors import ConfigError
from verixa.config.loader import load_config, resolve_config_path
from verixa.connectors.bigquery.connector import BigQueryConnector
from verixa.storage.filesystem import SnapshotStore, StorageError


@dataclass(frozen=True, slots=True)
class StatusReport:
    config_path: Path
    config_exists: bool
    config_error: str | None
    baseline_path: Path
    baseline_exists: bool
    baseline_age_seconds: int | None
    baseline_error: str | None
    auth_ok: bool | None
    auth_message: str | None
    warehouse_label: str | None
    sources: tuple[str, ...]


def run_status(
    config_path: Path | None = None,
    *,
    source_names: tuple[str, ...] = (),
) -> StatusReport:
    resolved_config_path = resolve_config_path(config_path)
    store = SnapshotStore()
    existing_baseline_path = store.existing_baseline_path()

    config_exists = resolved_config_path.exists()
    config_error: str | None = None
    auth_ok: bool | None = None
    auth_message: str | None = None
    warehouse_label: str | None = None
    sources: tuple[str, ...] = ()

    if config_exists:
        try:
            config = load_config(resolved_config_path, source_names=source_names)
        except ConfigError as exc:
            config_error = str(exc)
        else:
            warehouse_label = config.warehouse.kind
            if config.warehouse.project:
                warehouse_label += f" ({config.warehouse.project})"
            sources = tuple(sorted(config.sources))
            connector = BigQueryConnector(config.warehouse)
            auth_ok, auth_message = connector.check_auth()
    else:
        config_error = f"Config file '{resolved_config_path}' was not found."

    baseline_exists = store.baseline_exists()
    baseline_age_seconds: int | None = None
    baseline_error: str | None = None
    if baseline_exists:
        try:
            snapshot = store.read_baseline()
        except StorageError as exc:
            baseline_error = str(exc)
        else:
            baseline_age_seconds = int(
                (datetime.now(timezone.utc) - snapshot.generated_at).total_seconds()
            )

    return StatusReport(
        config_path=resolved_config_path,
        config_exists=config_exists,
        config_error=config_error,
        baseline_path=existing_baseline_path,
        baseline_exists=baseline_exists,
        baseline_age_seconds=baseline_age_seconds,
        baseline_error=baseline_error,
        auth_ok=auth_ok,
        auth_message=auth_message,
        warehouse_label=warehouse_label,
        sources=sources,
    )
